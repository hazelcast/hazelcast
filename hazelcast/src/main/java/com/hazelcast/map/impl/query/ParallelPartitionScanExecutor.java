/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntriesSegment;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static com.hazelcast.query.PagingPredicateAccessor.getNearestAnchorEntry;
import static com.hazelcast.util.FutureUtil.RETHROW_EVERYTHING;
import static com.hazelcast.util.FutureUtil.returnWithDeadline;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implementation of the {@link PartitionScanExecutor} which executes the partition scan in a parallel-fashion
 * delegating to the underlying executor.
 */
public class ParallelPartitionScanExecutor implements PartitionScanExecutor {

    private final PartitionScanRunner partitionScanRunner;
    private final ManagedExecutorService executor;
    private final int timeoutInMillis;

    public ParallelPartitionScanExecutor(PartitionScanRunner partitionScanRunner, ManagedExecutorService executor,
                                         int timeoutInMillis) {
        this.partitionScanRunner = partitionScanRunner;
        this.executor = executor;
        this.timeoutInMillis = timeoutInMillis;
    }

    @Override
    public void execute(String mapName, Predicate predicate, Collection<Integer> partitions, Result result) {
        runUsingPartitionScanWithoutPaging(mapName, predicate, partitions, result);
        if (predicate instanceof PagingPredicate) {
            Map.Entry<Integer, Map.Entry> nearestAnchorEntry = getNearestAnchorEntry((PagingPredicate) predicate);
            result.orderAndLimit((PagingPredicate) predicate, nearestAnchorEntry);
        }
    }

    /**
     * {@inheritDoc}
     * Parallel execution for a partition chunk query is not supported.
     */
    @Override
    public QueryableEntriesSegment execute(String mapName, Predicate predicate, int partitionId, int tableIndex, int fetchSize) {
        return partitionScanRunner.run(mapName, predicate, partitionId, tableIndex, fetchSize);
    }

    protected void runUsingPartitionScanWithoutPaging(String name, Predicate predicate, Collection<Integer> partitions,
                                                      Result result) {
        List<Future<Result>> futures = new ArrayList<Future<Result>>(partitions.size());

        for (Integer partitionId : partitions) {
            Future<Result> future = runPartitionScanForPartition(name, predicate, partitionId, result.createSubResult());
            futures.add(future);
        }

        Collection<Result> subResults = waitForResult(futures, timeoutInMillis);
        for (Result subResult : subResults) {
            result.combine(subResult);
        }
    }

    protected Future<Result> runPartitionScanForPartition(String name, Predicate predicate, int partitionId, Result result) {
        QueryPartitionCallable task = new QueryPartitionCallable(name, predicate, partitionId, result);
        return executor.submit(task);
    }

    private static Collection<Result> waitForResult(List<Future<Result>> lsFutures, int timeoutInMillis) {
        return returnWithDeadline(lsFutures, timeoutInMillis, MILLISECONDS, RETHROW_EVERYTHING);
    }

    private final class QueryPartitionCallable implements Callable<Result> {
        protected final int partition;
        protected final String name;
        protected final Predicate predicate;
        protected final Result result;

        private QueryPartitionCallable(String name, Predicate predicate, int partitionId, Result result) {
            this.name = name;
            this.predicate = predicate;
            this.partition = partitionId;
            this.result = result;
        }

        @Override
        public Result call() {
            partitionScanRunner.run(name, predicate, partition, result);
            result.setPartitionIds(Collections.singletonList(partition));
            return result;
        }
    }
}
