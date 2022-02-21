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

package com.hazelcast.replicatedmap.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.ReplicatedMapMergeTypes;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Inserts the merging entries for all partitions of a member via locally invoked {@link MergeOperation}.
 *
 * @since 3.10
 */
public class MergeOperationFactory extends PartitionAwareOperationFactory {

    private String name;
    private List<ReplicatedMapMergeTypes<Object, Object>>[] mergingEntries;
    private SplitBrainMergePolicy<Object, ReplicatedMapMergeTypes<Object, Object>, Object> mergePolicy;

    public MergeOperationFactory() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public MergeOperationFactory(String name, int[] partitions, List<ReplicatedMapMergeTypes<Object, Object>>[] mergingEntries,
                                 SplitBrainMergePolicy<Object, ReplicatedMapMergeTypes<Object, Object>, Object> mergePolicy) {
        this.name = name;
        this.partitions = partitions;
        this.mergingEntries = mergingEntries;
        this.mergePolicy = mergePolicy;
    }

    @Override
    public Operation createPartitionOperation(int partitionId) {
        for (int i = 0; i < partitions.length; i++) {
            if (partitions[i] == partitionId) {
                return new MergeOperation(name, mergingEntries[i], mergePolicy);
            }
        }
        throw new IllegalArgumentException("Unknown partitionId " + partitionId + " (" + Arrays.toString(partitions) + ")");
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeIntArray(partitions);
        for (List<ReplicatedMapMergeTypes<Object, Object>> list : mergingEntries) {
            out.writeInt(list.size());
            for (ReplicatedMapMergeTypes<Object, Object> mergingEntry : list) {
                out.writeObject(mergingEntry);
            }
        }
        out.writeObject(mergePolicy);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        partitions = in.readIntArray();
        //noinspection unchecked
        mergingEntries = new List[partitions.length];
        for (int partitionIndex = 0; partitionIndex < partitions.length; partitionIndex++) {
            int size = in.readInt();
            List<ReplicatedMapMergeTypes<Object, Object>> list = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                ReplicatedMapMergeTypes<Object, Object> mergingEntry = in.readObject();
                list.add(mergingEntry);
            }
            mergingEntries[partitionIndex] = list;
        }
        mergePolicy = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapDataSerializerHook.MERGE_FACTORY;
    }
}
