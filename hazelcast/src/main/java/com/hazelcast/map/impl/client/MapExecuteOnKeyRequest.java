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
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.EntryOperation;
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

public class MapExecuteOnKeyRequest extends KeyBasedClientRequest implements Portable, SecureRequest {

    private String name;
    private Data key;
    private EntryProcessor processor;
    private boolean submitToKey;

    public MapExecuteOnKeyRequest() {
    }

    public MapExecuteOnKeyRequest(String name, EntryProcessor processor, Data key) {
        this.name = name;
        this.processor = processor;
        this.key = key;
    }

    @Override
    protected Object getKey() {
        return key;
    }

    @Override
    protected Operation prepareOperation() {
        return new EntryOperation(name, key, processor);
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public int getClassId() {
        return MapPortableHook.EXECUTE_ON_KEY;
    }

    public void setAsSubmitToKey() {
        this.submitToKey = true;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeBoolean("s", submitToKey);
        final ObjectDataOutput out = writer.getRawDataOutput();
        key.writeData(out);
        out.writeObject(processor);
    }

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        submitToKey = reader.readBoolean("s");
        final ObjectDataInput in = reader.getRawDataInput();
        key = new Data();
        key.readData(in);
        processor = in.readObject();
    }

    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_PUT, ActionConstants.ACTION_REMOVE);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public String getMethodName() {
        if (submitToKey) {
            return "submitToKey";
        }
        return "executeOnKey";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{key, processor};
    }
}
