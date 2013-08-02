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

package com.hazelcast.map.client;

import com.hazelcast.client.InitializingObjectRequest;
import com.hazelcast.client.KeyBasedClientRequest;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.map.operation.PutOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.*;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public class MapPutRequest extends KeyBasedClientRequest implements Portable, InitializingObjectRequest {

    protected Data key;
    protected Data value;
    protected String name;
    protected int threadId;
    protected long ttl;

    public MapPutRequest() {
    }

    public MapPutRequest(String name, Data key, Data value, int threadId, long ttl) {
        this.name = name;
        this.key = key;
        this.value = value;
        this.threadId = threadId;
        this.ttl = ttl;
    }

    public MapPutRequest(String name, Data key, Data value, int threadId) {
        this.name = name;
        this.key = key;
        this.value = value;
        this.threadId = threadId;
        this.ttl = -1;
    }

    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.PUT;
    }

    protected Object getKey() {
        return key;
    }

    @Override
    protected Operation prepareOperation() {
        PutOperation op = new PutOperation(name, key, value, ttl);
        op.setThreadId(threadId);
        return op;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public Object getObjectId() {
        return name;
    }

    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeInt("t", threadId);
        writer.writeLong("ttl", ttl);
        final ObjectDataOutput out = writer.getRawDataOutput();
        key.writeData(out);
        value.writeData(out);
    }

    public void readPortable(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        threadId = reader.readInt("t");
        ttl = reader.readLong("ttl");
        final ObjectDataInput in = reader.getRawDataInput();
        key = new Data();
        key.readData(in);
        value = new Data();
        value.readData(in);
    }

}
