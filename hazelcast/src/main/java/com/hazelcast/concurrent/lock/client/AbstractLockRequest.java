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

import com.hazelcast.client.KeyBasedClientRequest;
import com.hazelcast.client.SecureRequest;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.operations.LockOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public abstract class AbstractLockRequest extends KeyBasedClientRequest
        implements Portable, SecureRequest {

    protected Data key;
    protected long threadId;
    protected long ttl = -1;
    protected long timeout = -1;

    public AbstractLockRequest() {
    }

    public AbstractLockRequest(Data key, long threadId) {
        this.key = key;
        this.threadId = threadId;
    }

    public AbstractLockRequest(Data key, long threadId, long ttl, long timeout) {
        this.key = key;
        this.threadId = threadId;
        this.ttl = ttl;
        this.timeout = timeout;
    }

    protected String getName() {
        return serializationService.toObject(key);
    }

    @Override
    protected final Operation prepareOperation() {
        return new LockOperation(getNamespace(), key, threadId, ttl, timeout);
    }

    @Override
    protected final Object getKey() {
        return key;
    }

    protected abstract ObjectNamespace getNamespace();

    @Override
    public final String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeLong("tid", threadId);
        writer.writeLong("ttl", ttl);
        writer.writeLong("timeout", timeout);

        ObjectDataOutput out = writer.getRawDataOutput();
        key.writeData(out);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        threadId = reader.readLong("tid");
        ttl = reader.readLong("ttl");
        timeout = reader.readLong("timeout");

        ObjectDataInput in = reader.getRawDataInput();
        key = new Data();
        key.readData(in);
    }

    @Override
    public String getDistributedObjectName() {
        return serializationService.toObject(key);
    }

    @Override
    public String getMethodName() {
        if (timeout == -1) {
            return "lock";
        }
        return "tryLock";
    }

    @Override
    public Object[] getParameters() {
        if ((ttl == -1 && timeout == -1) || timeout == 0) {
            return new Object[]{key};
        } else if (timeout == -1) {
            return new Object[]{key, ttl, TimeUnit.MILLISECONDS};
        }
        return new Object[]{key, timeout, TimeUnit.MILLISECONDS};
    }
}
