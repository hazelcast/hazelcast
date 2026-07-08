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

package com.hazelcast.vector.internal.impl.ops;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import com.hazelcast.vector.internal.impl.VectorCollectionSerializerHook;

import java.io.IOException;
import java.util.Arrays;

public class PutAllOperationFactory extends PartitionAwareOperationFactory {
    protected String name;
    protected VectorEntries[] vectorEntries;

    public PutAllOperationFactory() {
    }

    public PutAllOperationFactory(String name, int[] partitions, VectorEntries[] vectorEntries) {
        this.name = name;
        this.partitions = partitions;
        this.vectorEntries = vectorEntries;
        //NB: general structure copied from c.hz.map.impl.operation.PutAllPartitionAwareOperationFactory
        //May benefit from refactoring or default interfaces
    }

    @Override
    public Operation createPartitionOperation(int partitionId) {
        for (int i = 0; i < partitions.length; i++) {
            if (partitions[i] == partitionId) {
                return new PutAllOperation(name, vectorEntries[i]);
            }
        }
        throw new IllegalArgumentException("Unknown partitionId " + partitionId + " (" + Arrays.toString(partitions) + ")");
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeIntArray(partitions);
        for (VectorEntries entry : vectorEntries) {
            out.writeObject(entry);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        partitions = in.readIntArray();
        vectorEntries = new VectorEntries[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            vectorEntries[i] = in.readObject();
        }
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.PUT_ALL_FACTORY;
    }
}
