/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.lock.client;

import com.hazelcast.client.impl.client.KeyBasedClientRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.concurrent.lock.InternalLockNamespace;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.operations.BeforeAwaitOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.LockPermission;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.security.Permission;

public class BeforeAwaitRequest extends KeyBasedClientRequest implements Portable, SecureRequest {

    private ObjectNamespace namespace;
    private Data key;
    private long threadId;
    private String conditionId;

    public BeforeAwaitRequest() {
    }

    public BeforeAwaitRequest(ObjectNamespace namespace, long threadId, String conditionId, Data key) {
        this.namespace = namespace;
        this.threadId = threadId;
        this.conditionId = conditionId;
        this.key = key;
    }

    @Override
    protected Object getKey() {
        return key;
    }

    @Override
    protected Operation prepareOperation() {
        return new BeforeAwaitOperation(namespace, key, threadId, conditionId);
    }

    @Override
    public String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return LockPortableHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return LockPortableHook.CONDITION_BEFORE_AWAIT;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeLong("tid", threadId);
        writer.writeUTF("cid", conditionId);
        ObjectDataOutput out = writer.getRawDataOutput();
        namespace.writeData(out);
        out.writeData(key);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        threadId = reader.readLong("tid");
        conditionId = reader.readUTF("cid");
        ObjectDataInput in = reader.getRawDataInput();
        namespace = new InternalLockNamespace();
        namespace.readData(in);
        key = in.readData();
    }

    @Override
    public Permission getRequiredPermission() {
        return new LockPermission(namespace.getObjectName(), ActionConstants.ACTION_LOCK);
    }
}
