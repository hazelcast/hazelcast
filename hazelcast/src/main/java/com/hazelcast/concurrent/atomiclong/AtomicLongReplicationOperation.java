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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * User: sancar
 * Date: 12/28/12
 * Time: 2:26 PM
 */
public class AtomicLongReplicationOperation extends AbstractOperation {

    private Map<String, Long> migrationData;

    public AtomicLongReplicationOperation() {
        super();
    }

    public AtomicLongReplicationOperation(Map<String, Long> migrationData) {
        this.migrationData = migrationData;
    }

    public void run() throws Exception {
        AtomicLongService atomicLongService = getService();
        for (Map.Entry<String, Long> longEntry : migrationData.entrySet()) {
            atomicLongService.getNumber(longEntry.getKey()).set(longEntry.getValue());
        }

    }

    public String getServiceName() {
        return AtomicLongService.SERVICE_NAME;
    }


    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(migrationData.size());
        for (Map.Entry<String, Long> entry : migrationData.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeLong(entry.getValue());
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        int mapSize = in.readInt();
        migrationData = new HashMap<String, Long>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            String name = in.readUTF();
            Long number = in.readLong();
            migrationData.put(name, number);
        }
    }
}
