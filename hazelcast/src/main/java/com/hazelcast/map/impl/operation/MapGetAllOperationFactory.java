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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapGetAllOperationFactory extends PartitionAwareOperationFactory implements Versioned {

    protected String name;
    // can be null if the operation is deserialized from <5.8 version
    private Map<Integer, List<Data>> keysForPartitions;
    private List<Data> keys = new ArrayList<>();

    public MapGetAllOperationFactory() {
    }

    public MapGetAllOperationFactory(String name, Map<Integer, List<Data>> keysForPartitions) {
        this.name = name;
        this.keysForPartitions = keysForPartitions;
    }

    @Override
    public Operation createPartitionOperation(int partition) {
        if (keysForPartitions != null) {
            return new GetAllOperation(name, keysForPartitions.get(partition), true);
        } else {
            // RU_COMPAT_5_7
            // We did not get keysForPartitions, use slower path and repartition the data in GetAllOperation
            return new GetAllOperation(name, keys, false);
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        if (out.getVersion().isGreaterOrEqual(Versions.V6_0)) {
            out.writeInt(keysForPartitions.size());
            for (Map.Entry<Integer, List<Data>> entry : keysForPartitions.entrySet()) {
                Integer partitionId = entry.getKey();
                List<Data> keys = entry.getValue();
                out.writeInt(partitionId);
                out.writeInt(keys.size());
                for (Data key : keys) {
                    IOUtil.writeData(out, key);
                }
            }
        } else {
            // RU_COMPAT_5_7
            // flatten `keysForPartitions` if provided, otherwise `keys` must be set
            // receiving end may not be able to handle keysForPartitions
            if (keysForPartitions != null) {
                var keysCount = keysForPartitions.values().stream().mapToInt(List::size).sum();
                out.writeInt(keysCount);
                for (var keys : keysForPartitions.values()) {
                    for (Data key : keys) {
                        IOUtil.writeData(out, key);
                    }
                }
            } else {
                out.writeInt(keys.size());
                for (Data key : keys) {
                    IOUtil.writeData(out, key);
                }
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        if (in.getVersion().isGreaterOrEqual(Versions.V6_0)) {
            int partitionCount = in.readInt();
            keysForPartitions = new HashMap<>(partitionCount);
            for (int i = 0; i < partitionCount; ++i) {
                int partitionId = in.readInt();
                int size = in.readInt();
                var keysForPartition = new ArrayList<Data>(size);
                for (int j = 0; j < size; j++) {
                    keysForPartition.add(IOUtil.readData(in));
                }
                keysForPartitions.put(partitionId, keysForPartition);
            }
        } else {
            // RU_COMPAT_5_7
            // In <= 5.7 we send only flat list of keys
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                Data data = IOUtil.readData(in);
                keys.add(data);
            }
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MAP_GET_ALL_FACTORY;
    }
}
