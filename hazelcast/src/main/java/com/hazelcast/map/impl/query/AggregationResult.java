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
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullablePartitionIdSet;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullablePartitionIdSet;

/**
 * Contains the result of the evaluation of an aggregation on a specific partition or node.
 * <p>
 * At the end of the aggregation execution path all AggregationResults are merged into one AggregationResult.
 */
public class AggregationResult implements Result<AggregationResult> {

    private Aggregator aggregator;
    private PartitionIdSet partitionIds;

    private final transient SerializationService serializationService;

    public AggregationResult() {
        this.serializationService = null;
    }

    public AggregationResult(Aggregator aggregator, SerializationService serializationService) {
        this.aggregator = aggregator;
        this.serializationService = serializationService;
    }

    @SuppressWarnings("unchecked")
    public <R> Aggregator<?, R> getAggregator() {
        return aggregator;
    }

    @Override
    public PartitionIdSet getPartitionIds() {
        return partitionIds;
    }

    @Override
    public void combine(AggregationResult result) {
        PartitionIdSet otherPartitionIds = result.getPartitionIds();
        if (otherPartitionIds == null) {
            return;
        }
        if (partitionIds == null) {
            partitionIds = new PartitionIdSet(otherPartitionIds);
        } else {
            partitionIds.addAll(otherPartitionIds);
        }
        aggregator.combine(result.aggregator);
    }

    @Override
    public void onCombineFinished() {
        if (aggregator != null) {
            aggregator.onCombinationFinished();
        }
    }

    @Override
    public void add(QueryableEntry entry) {
        aggregator.accumulate(entry);
    }

    @Override
    public AggregationResult createSubResult() {
        Aggregator aggregatorClone = serializationService.toObject(serializationService.toData(aggregator));
        return new AggregationResult(aggregatorClone, serializationService);
    }

    @Override
    public void orderAndLimit(PagingPredicate pagingPredicate, Map.Entry<Integer, Map.Entry> nearestAnchorEntry) {
        // Do nothing, since there is no support of paging predicates for
        // aggregations.
    }

    @Override
    public void completeConstruction(PartitionIdSet partitionIds) {
        setPartitionIds(partitionIds);
    }

    @Override
    public void setPartitionIds(PartitionIdSet partitionIds) {
        this.partitionIds = new PartitionIdSet(partitionIds);
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.AGGREGATION_RESULT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        writeNullablePartitionIdSet(partitionIds, out);
        out.writeObject(aggregator);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.partitionIds = readNullablePartitionIdSet(in);
        this.aggregator = in.readObject();
    }
}
