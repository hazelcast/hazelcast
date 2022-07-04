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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;

public class MultiMapPutAllOperationFactory extends PartitionAwareOperationFactory {
    protected String name;
    protected MapEntries[] mapEntries;

    public MultiMapPutAllOperationFactory() {
    }

    @SuppressFBWarnings({"EI_EXPOSE_REP2"})
    public MultiMapPutAllOperationFactory(String name, int[] partitions, MapEntries[] mapEntries) {
        this.name = name;
        this.partitions = partitions;
        this.mapEntries = mapEntries;
        //NB: general structure copied from c.hz.map.impl.operation.PutAllOperationPartitionAwareFactort
        //May benefit from refactoring or default interfaces
    }

    @Override
    public Operation createPartitionOperation(int partitionId) {
        for (int i = 0; i < partitions.length; i++) {
            if (partitions[i] == partitionId) {
                return new PutAllOperation(name, mapEntries[i]);
            }
        }
        throw new IllegalArgumentException("Unknown partitionId " + partitionId + " (" + Arrays.toString(partitions) + ")");
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeIntArray(partitions);
        for (MapEntries entry : mapEntries) {
            entry.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        partitions = in.readIntArray();
        mapEntries = new MapEntries[partitions.length];
        for (int i = 0; i < partitions.length; i++) {
            MapEntries entry = new MapEntries();
            entry.readData(in);
            mapEntries[i] = entry;
        }
    }

    @Override
    public int getFactoryId() {
        return MultiMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.PUT_ALL_PARTITION_AWARE_FACTORY;
    }
}
