/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.semaphore.operations;

import com.hazelcast.concurrent.semaphore.Permit;
import com.hazelcast.concurrent.semaphore.SemaphoreDataSerializerHook;
import com.hazelcast.concurrent.semaphore.SemaphoreService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SemaphoreReplicationOperation extends AbstractOperation implements IdentifiedDataSerializable {

    Map<String, Permit> migrationData;

    public SemaphoreReplicationOperation() {
    }

    public SemaphoreReplicationOperation(Map<String, Permit> migrationData) {
        this.migrationData = migrationData;
    }

    @Override
    public void run() throws Exception {
        SemaphoreService service = getService();
        for (Permit permit : migrationData.values()) {
            permit.setInitialized();
        }
        service.insertMigrationData(migrationData);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, Permit> entry : migrationData.entrySet()) {
            String key = entry.getKey();
            Permit value = entry.getValue();
            out.writeUTF(key);
            value.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        migrationData = new HashMap<String, Permit>(size);
        for (int i = 0; i < size; i++) {
            String name = in.readUTF();
            Permit permit = new Permit();
            permit.readData(in);
            migrationData.put(name, permit);
        }
    }

    @Override
    public int getFactoryId() {
        return SemaphoreDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return SemaphoreDataSerializerHook.SEMAPHORE_REPLICATION_OPERATION;
    }
}
