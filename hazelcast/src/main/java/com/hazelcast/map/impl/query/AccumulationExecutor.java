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
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.query.impl.QueryableEntry;

/**
 * Executes the accumulation phase of the Aggregator.
 * <p>
 * All entries are accumulates by the result aggregator returned in the AggregationResult.
 */
public interface AccumulationExecutor {

    /**
     * @param aggregator   Instance of aggregator using which the entries should be accumulated (it will not be modified)
     * @param entries      Entries to be accumulated
     * @param partitionIds IDs of the partitions where the entries reside
     * @return AggregationResult encompassing the result aggregator
     */
    AggregationResult execute(Aggregator aggregator, Iterable<QueryableEntry> entries,
                              PartitionIdSet partitionIds);
}
