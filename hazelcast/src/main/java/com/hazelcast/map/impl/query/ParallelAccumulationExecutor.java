/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.map.impl.query;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static com.hazelcast.internal.util.FutureUtil.RETHROW_EVERYTHING;
import static com.hazelcast.internal.util.FutureUtil.returnWithDeadline;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implementation of the {@link AccumulationExecutor} that runs the accumulation in a multi-threaded way.
 * Entries are split into chunks and each chunk is sent as a task to the underlying executor.
 * At the end the result is merged to a single AggregationResult.
 */
public class ParallelAccumulationExecutor implements AccumulationExecutor {

    private static final int THREAD_SPLIT_COUNT = 8;

    private final ManagedExecutorService executor;
    private final SerializationService serializationService;
    private final int callTimeoutInMillis;

    public ParallelAccumulationExecutor(ManagedExecutorService executor, SerializationService serializationService,
                                        int callTimeoutInMillis) {
        this.executor = executor;
        this.serializationService = serializationService;
        this.callTimeoutInMillis = callTimeoutInMillis;
    }

    @Override
    @SuppressWarnings("unchecked")
    public AggregationResult execute(Aggregator aggregator,
                                     Iterable<QueryableEntry> entries,
                                     PartitionIdSet partitionIds) {
        Collection<Aggregator> chunkAggregators = accumulateParallel(aggregator, entries);

        Aggregator resultAggregator = clone(aggregator);
        try {
            for (Aggregator chunkAggregator : chunkAggregators) {
                resultAggregator.combine(chunkAggregator);
            }
        } finally {
            resultAggregator.onCombinationFinished();
        }

        AggregationResult result = new AggregationResult(resultAggregator, serializationService);
        result.setPartitionIds(partitionIds);
        return result;
    }

    protected Collection<Aggregator> accumulateParallel(Aggregator aggregator, Iterable<QueryableEntry> entries) {
        Collection<Future<Aggregator>> futures = new ArrayList<>();

        List<QueryableEntry> buffer = new ArrayList<>();
        for (QueryableEntry entry : entries) {
            buffer.add(entry);
            if (buffer.size() == THREAD_SPLIT_COUNT) {
                futures.add(executor.submit(new AccumulatePartitionCallable(clone(aggregator), buffer)));
                buffer = new ArrayList<>();
            }
        }

        if (!buffer.isEmpty()) {
            futures.add(executor.submit(new AccumulatePartitionCallable(clone(aggregator), buffer)));
        }

        return returnWithDeadline(futures, callTimeoutInMillis, MILLISECONDS, RETHROW_EVERYTHING);
    }

    private Aggregator clone(Aggregator aggregator) {
        return serializationService.toObject(serializationService.toData(aggregator));
    }

    private static final class AccumulatePartitionCallable implements Callable<Aggregator> {
        private final Aggregator aggregator;
        private final Iterable<QueryableEntry> entries;

        private AccumulatePartitionCallable(Aggregator aggregator, Iterable<QueryableEntry> entries) {
            this.aggregator = aggregator;
            this.entries = entries;
        }

        @Override
        public Aggregator call() {
            try {
                for (QueryableEntry entry : entries) {
                    aggregator.accumulate(entry);
                }
            } finally {
                aggregator.onAccumulationFinished();
            }
            return aggregator;
        }
    }
}
