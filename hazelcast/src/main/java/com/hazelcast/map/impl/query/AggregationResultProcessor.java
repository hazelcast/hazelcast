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
import com.hazelcast.query.impl.QueryableEntry;

/**
 * Responsible for populating {@link AggregationResult}s
 */
public class AggregationResultProcessor implements ResultProcessor<AggregationResult> {

    private final AccumulationExecutor accumulationExecutor;
    private final SerializationService serializationService;

    public AggregationResultProcessor(AccumulationExecutor accumulationExecutor, SerializationService serializationService) {
        this.accumulationExecutor = accumulationExecutor;
        this.serializationService = serializationService;
    }

    @Override
    public AggregationResult populateResult(Query query, long resultLimit, Iterable<QueryableEntry> entries,
                                            PartitionIdSet partitionIds) {
        return accumulationExecutor.execute(query.getAggregator(), entries, partitionIds);
    }

    @Override
    public AggregationResult populateResult(Query query, long resultLimit) {
        Aggregator resultAggregator = serializationService.toObject(serializationService.toData(query.getAggregator()));
        return new AggregationResult(resultAggregator, serializationService);
    }
}
