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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import com.hazelcast.spi.merge.KeyMergeDataHolder;
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
@SuppressWarnings("WeakerAccess")
public class MergeOperationFactory extends PartitionAwareOperationFactory {

    protected String name;
    protected List<KeyMergeDataHolder<Data, Data>>[] mergeData;
    protected SplitBrainMergePolicy policy;

    public MergeOperationFactory() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public MergeOperationFactory(String name, int[] partitions, List<KeyMergeDataHolder<Data, Data>>[] mergeData,
                                 SplitBrainMergePolicy policy) {
        this.name = name;
        this.partitions = partitions;
        this.mergeData = mergeData;
        this.policy = policy;
    }

    @Override
    public Operation createPartitionOperation(int partitionId) {
        for (int i = 0; i < partitions.length; i++) {
            if (partitions[i] == partitionId) {
                return new MergeOperation(name, mergeData[i], policy, false);
            }
        }
        throw new IllegalArgumentException("Unknown partitionId " + partitionId + " (" + Arrays.toString(partitions) + ")");
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeIntArray(partitions);
        for (List<KeyMergeDataHolder<Data, Data>> entry : mergeData) {
            out.writeInt(entry.size());
            for (KeyMergeDataHolder<Data, Data> mergeEntry : entry) {
                out.writeObject(mergeEntry);
            }
        }
        out.writeObject(policy);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        partitions = in.readIntArray();
        mergeData = new List[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            int size = in.readInt();
            List<KeyMergeDataHolder<Data, Data>> list = new ArrayList<KeyMergeDataHolder<Data, Data>>(size);
            for (int j = 0; j < size; j++) {
                KeyMergeDataHolder<Data, Data> mergeEntry = in.readObject();
                list.add(mergeEntry);
            }
            mergeData[i] = list;
        }
        policy = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.MERGE_FACTORY;
    }
}
