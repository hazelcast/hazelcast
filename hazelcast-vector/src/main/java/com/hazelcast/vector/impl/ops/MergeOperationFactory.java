/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.impl.ops;

import com.hazelcast.internal.namespace.NamespaceUtil;
import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.VectorCollectionMergeTypes;
import com.hazelcast.vector.VectorDocument;
import com.hazelcast.vector.impl.VectorCollectionSerializerHook;
import com.hazelcast.vector.impl.VectorCollectionService;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class MergeOperationFactory extends PartitionAwareOperationFactory {

    private String name;
    private List<VectorCollectionMergeTypes<Object, VectorDocument<?>>>[] mergingEntries;
    private SplitBrainMergePolicy<VectorDocument<?>,
            VectorCollectionMergeTypes<Object, VectorDocument<?>>, Object> mergePolicy;

    public MergeOperationFactory() {
    }

    public MergeOperationFactory(String name, int[] partitions,
                                 List<VectorCollectionMergeTypes<Object, VectorDocument<?>>>[] mergingEntries,
                                 SplitBrainMergePolicy<VectorDocument<?>,
                                         VectorCollectionMergeTypes<Object, VectorDocument<?>>, Object> mergePolicy) {
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
    public int getFactoryId() {
        return VectorCollectionSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.MERGE_OPERATION_FACTORY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeIntArray(partitions);
        for (var list : mergingEntries) {
            SerializationUtil.writeList(list, out);
        }
        out.writeObject(mergePolicy);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        partitions = in.readIntArray();
        mergingEntries = new List[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            mergingEntries[i] = SerializationUtil.readList(in);
        }
        mergePolicy = NamespaceUtil.callWithNamespace(in::readObject, name, VectorCollectionService::lookupNamespace);
    }
}
