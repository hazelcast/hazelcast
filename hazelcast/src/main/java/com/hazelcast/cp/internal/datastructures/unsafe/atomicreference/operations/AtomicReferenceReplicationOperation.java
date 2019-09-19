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

package com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.operations;

import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.AtomicReferenceContainer;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.AtomicReferenceDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.AtomicReferenceService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

public class AtomicReferenceReplicationOperation extends Operation
        implements IdentifiedDataSerializable {

    private Map<String, Data> migrationData;

    public AtomicReferenceReplicationOperation() {
    }

    public AtomicReferenceReplicationOperation(Map<String, Data> migrationData) {
        this.migrationData = migrationData;
    }

    @Override
    public void run() throws Exception {
        AtomicReferenceService service = getService();
        for (Map.Entry<String, Data> entry : migrationData.entrySet()) {
            String name = entry.getKey();
            AtomicReferenceContainer container = service.getReferenceContainer(name);
            Data value = entry.getValue();
            container.set(value);
        }
    }

    @Override
    public String getServiceName() {
        return AtomicReferenceService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return AtomicReferenceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return AtomicReferenceDataSerializerHook.REPLICATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, Data> entry : migrationData.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeData(entry.getValue());
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        migrationData = createHashMap(mapSize);
        for (int i = 0; i < mapSize; i++) {
            String name = in.readUTF();
            Data data = in.readData();
            migrationData.put(name, data);
        }
    }
}

