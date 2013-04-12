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

package com.hazelcast.concurrent.semaphore;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @ali 1/22/13
 */
public class SemaphoreReplicationOperation extends AbstractOperation {

    Map<String, Permit> migrationData;

    public SemaphoreReplicationOperation() {
    }

    public SemaphoreReplicationOperation(Map<String, Permit> migrationData) {
        this.migrationData = migrationData;
    }

    public void run() throws Exception {
        SemaphoreService service = getService();
        service.insertMigrationData(migrationData);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, Permit> entry : migrationData.entrySet()) {
            out.writeUTF(entry.getKey());
            entry.getValue().writeData(out);
        }
    }

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
}
