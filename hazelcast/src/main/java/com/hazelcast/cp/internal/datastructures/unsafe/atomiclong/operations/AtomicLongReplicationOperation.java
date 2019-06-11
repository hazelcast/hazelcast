/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.unsafe.atomiclong.operations;

import com.hazelcast.cp.internal.datastructures.unsafe.atomiclong.AtomicLongContainer;
import com.hazelcast.cp.internal.datastructures.unsafe.atomiclong.AtomicLongDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.unsafe.atomiclong.AtomicLongService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.cp.internal.datastructures.unsafe.atomiclong.AtomicLongDataSerializerHook.REPLICATION;
import static com.hazelcast.util.MapUtil.createHashMap;

public class AtomicLongReplicationOperation extends Operation
        implements IdentifiedDataSerializable {

    private Map<String, Long> migrationData;

    public AtomicLongReplicationOperation() {
    }

    public AtomicLongReplicationOperation(Map<String, Long> migrationData) {
        this.migrationData = migrationData;
    }

    @Override
    public void run() throws Exception {
        AtomicLongService atomicLongService = getService();
        for (Map.Entry<String, Long> longEntry : migrationData.entrySet()) {
            String name = longEntry.getKey();
            AtomicLongContainer container = atomicLongService.getLongContainer(name);
            Long value = longEntry.getValue();
            container.set(value);
        }
    }

    @Override
    public String getServiceName() {
        return AtomicLongService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return AtomicLongDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return REPLICATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, Long> entry : migrationData.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeLong(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        migrationData = createHashMap(mapSize);
        for (int i = 0; i < mapSize; i++) {
            String name = in.readUTF();
            Long longContainer = in.readLong();
            migrationData.put(name, longContainer);
        }
    }
}
