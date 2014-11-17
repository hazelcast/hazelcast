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

package com.hazelcast.map.impl.client;

import com.hazelcast.client.impl.client.KeyBasedClientRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.TryRemoveOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.Operation;
import java.io.IOException;
import java.security.Permission;
import java.util.concurrent.TimeUnit;

public class MapTryRemoveRequest extends KeyBasedClientRequest implements Portable, SecureRequest {

    protected String name;
    protected Data key;
    protected long threadId;
    protected long timeout;

    public MapTryRemoveRequest() {
    }

    public MapTryRemoveRequest(String name, Data key, long threadId, long timeout) {
        this.name = name;
        this.key = key;
        this.threadId = threadId;
        this.timeout = timeout;
    }

    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.TRY_REMOVE;
    }

    @Override
    protected Object getKey() {
        return key;
    }

    @Override
    protected Operation prepareOperation() {
        TryRemoveOperation operation = new TryRemoveOperation(name, key, timeout);
        operation.setThreadId(threadId);
        return operation;
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeLong("t", threadId);
        writer.writeLong("timeout", timeout);
        final ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(key);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        threadId = reader.readLong("t");
        timeout = reader.readLong("timeout");
        final ObjectDataInput in = reader.getRawDataInput();
        key = in.readData();
    }

    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        return "tryRemove";
    }

    @Override
    public Object[] getParameters() {
        return new Object[] {key, timeout, TimeUnit.MILLISECONDS};
    }
}
