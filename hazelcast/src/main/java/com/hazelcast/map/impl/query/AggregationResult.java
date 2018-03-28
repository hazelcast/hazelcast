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
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Contains the result of the evaluation of an aggregation on a specific Partition or Node.
 * <p>
 * At the end of the aggregation execution path all AggregationResults are merged into one AggregationResult.
 */
public class AggregationResult implements Result<AggregationResult>, IdentifiedDataSerializable {

    private Aggregator aggregator;
    private Collection<Integer> partitionIds;

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
    public Collection<Integer> getPartitionIds() {
        return partitionIds;
    }

    @Override
    public void combine(AggregationResult result) {
        Collection<Integer> otherPartitionIds = result.getPartitionIds();
        if (otherPartitionIds == null) {
            return;
        }
        if (partitionIds == null) {
            partitionIds = new ArrayList<Integer>(otherPartitionIds.size());
        }
        partitionIds.addAll(otherPartitionIds);
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
    public void completeConstruction(Collection<Integer> partitionIds) {
        setPartitionIds(partitionIds);
    }

    @Override
    public void setPartitionIds(Collection<Integer> partitionIds) {
        this.partitionIds = new ArrayList<Integer>(partitionIds);
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.AGGREGATION_RESULT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        int partitionSize = (partitionIds == null) ? 0 : partitionIds.size();
        out.writeInt(partitionSize);
        if (partitionSize > 0) {
            for (Integer partitionId : partitionIds) {
                out.writeInt(partitionId);
            }
        }
        out.writeObject(aggregator);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int partitionSize = in.readInt();
        if (partitionSize > 0) {
            this.partitionIds = new ArrayList<Integer>(partitionSize);
            for (int i = 0; i < partitionSize; i++) {
                this.partitionIds.add(in.readInt());
            }
        }
        this.aggregator = in.readObject();
    }
}
