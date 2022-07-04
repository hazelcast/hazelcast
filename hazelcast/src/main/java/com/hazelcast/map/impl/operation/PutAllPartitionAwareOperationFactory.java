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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

/**
 * Inserts the {@link MapEntries} for all partitions of a member via locally invoked {@link PutAllOperation}.
 * <p>
 * Used to reduce the number of remote invocations of an {@link IMap#putAll(Map)} or {@link IMap#setAll(Map)} call.
 */
public class PutAllPartitionAwareOperationFactory extends PartitionAwareOperationFactory implements Versioned {

    protected String name;
    protected MapEntries[] mapEntries;
    protected boolean triggerMapLoader;

    public PutAllPartitionAwareOperationFactory() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public PutAllPartitionAwareOperationFactory(String name, int[] partitions,
                                                MapEntries[] mapEntries, boolean triggerMapLoader) {
        this.name = name;
        this.partitions = partitions;
        this.mapEntries = mapEntries;
        this.triggerMapLoader = triggerMapLoader;
    }

    @Override
    public Operation createPartitionOperation(int partitionId) {
        for (int i = 0; i < partitions.length; i++) {
            if (partitions[i] == partitionId) {
                return new PutAllOperation(name, mapEntries[i], triggerMapLoader);
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
        if (out.getVersion().isGreaterOrEqual(Versions.V4_1)) {
            out.writeBoolean(triggerMapLoader);
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
        if (in.getVersion().isGreaterOrEqual(Versions.V4_1)) {
            triggerMapLoader = in.readBoolean();
        } else {
            triggerMapLoader = true;
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PUT_ALL_PARTITION_AWARE_FACTORY;
    }
}
