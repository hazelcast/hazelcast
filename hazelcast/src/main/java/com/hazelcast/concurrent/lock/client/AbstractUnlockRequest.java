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

package com.hazelcast.concurrent.lock.client;

import com.hazelcast.client.impl.client.KeyBasedClientRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.operations.UnlockOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;

import java.io.IOException;

public abstract class AbstractUnlockRequest extends KeyBasedClientRequest
        implements Portable, SecureRequest {

    protected Data key;
    private long threadId;
    private boolean force;

    public AbstractUnlockRequest() {
    }

    public AbstractUnlockRequest(Data key, long threadId) {
        this.key = key;
        this.threadId = threadId;
    }

    protected AbstractUnlockRequest(Data key, long threadId, boolean force) {
        this.key = key;
        this.threadId = threadId;
        this.force = force;
    }

    protected String getName() {
        return serializationService.toObject(key);
    }

    @Override
    protected final Object getKey() {
        return key;
    }

    @Override
    protected final Operation prepareOperation() {
        return new UnlockOperation(getNamespace(), key, threadId, force);
    }

    protected abstract ObjectNamespace getNamespace();

    @Override
    public final String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeLong("tid", threadId);
        writer.writeBoolean("force", force);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeData(key);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        threadId = reader.readLong("tid");
        force = reader.readBoolean("force");
        ObjectDataInput in = reader.getRawDataInput();
        key = in.readData();
    }

    @Override
    public String getDistributedObjectName() {
        return serializationService.toObject(key);
    }

    @Override
    public String getMethodName() {
        if (force) {
            return "forceUnlock";
        }
        return "unlock";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{key};
    }
}
