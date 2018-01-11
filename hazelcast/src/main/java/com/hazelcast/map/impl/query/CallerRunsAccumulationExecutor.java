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

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Collection;

/**
 * Implementation of the {@link AccumulationExecutor} that runs the accumulation in the calling thread in a sequential fashion.
 */
public class CallerRunsAccumulationExecutor implements AccumulationExecutor {

    private SerializationService serializationService;

    public CallerRunsAccumulationExecutor(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    @SuppressWarnings("unchecked")
    public AggregationResult execute(
            Aggregator aggregator, Collection<QueryableEntry> entries, Collection<Integer> partitionIds) {
        Aggregator resultAggregator = serializationService.toObject(serializationService.toData(aggregator));
        try {
            for (QueryableEntry entry : entries) {
                resultAggregator.accumulate(entry);
            }
        } finally {
            resultAggregator.onAccumulationFinished();
        }

        AggregationResult result = new AggregationResult(resultAggregator);
        result.setPartitionIds(partitionIds);
        return result;
    }
}
