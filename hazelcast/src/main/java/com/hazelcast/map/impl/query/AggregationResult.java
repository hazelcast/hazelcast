/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Contains the result of the evaluation of an aggregation on a specific Partition or Node.
 *
 * At the end of the aggregation execution path all AggregationResults are merged into one AggregationResult.
 */
public class AggregationResult implements Result<AggregationResult>, IdentifiedDataSerializable {

    private Aggregator aggregator;
    private Collection<Integer> partitionIds;

    public AggregationResult() {
    }

    public AggregationResult(Aggregator aggregator) {
        this.aggregator = aggregator;
    }

    public <R> Aggregator<?, ?, R> getAggregator() {
        return aggregator;
    }

    @Override
    public Collection<Integer> getPartitionIds() {
        return partitionIds;
    }

    @Override
    public void combine(AggregationResult result) {
        if (partitionIds == null) {
            partitionIds = new ArrayList<Integer>(result.getPartitionIds().size());
        }
        partitionIds.addAll(result.getPartitionIds());
        aggregator.combine((result.aggregator));
    }

    @Override
    public void onCombineFinished() {
        if (aggregator != null) {
            aggregator.onCombinationFinished();
        }
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
        out.writeObject(aggregator);
        out.writeInt(partitionIds.size());
        for (Integer partitionId : partitionIds) {
            out.writeInt(partitionId);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.aggregator = in.readObject();
        int partitionIdsSize = in.readInt();
        this.partitionIds = new ArrayList<Integer>(partitionIdsSize);
        for (int i = 0; i < partitionIdsSize; i++) {
            this.partitionIds.add(in.readInt());
        }
    }
}
