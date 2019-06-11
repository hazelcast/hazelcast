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

package com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations;

import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.SemaphoreContainer;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.SemaphoreDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.SemaphoreService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.util.MapUtil.createHashMap;

public class SemaphoreReplicationOperation extends Operation implements IdentifiedDataSerializable {

    private Map<String, SemaphoreContainer> migrationData;

    public SemaphoreReplicationOperation() {
    }

    public SemaphoreReplicationOperation(Map<String, SemaphoreContainer> migrationData) {
        this.migrationData = migrationData;
    }

    @Override
    public void run() throws Exception {
        SemaphoreService service = getService();
        for (SemaphoreContainer semaphoreContainer : migrationData.values()) {
            semaphoreContainer.setInitialized();
        }
        service.insertMigrationData(migrationData);
    }

    @Override
    public String getServiceName() {
        return SemaphoreService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return SemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SemaphoreDataSerializerHook.SEMAPHORE_REPLICATION_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, SemaphoreContainer> entry : migrationData.entrySet()) {
            String key = entry.getKey();
            SemaphoreContainer value = entry.getValue();
            out.writeUTF(key);
            value.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        migrationData = createHashMap(size);
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            SemaphoreContainer semaphoreContainer = new SemaphoreContainer();
            semaphoreContainer.readData(in);
            migrationData.put(name, semaphoreContainer);
        }
    }
}
