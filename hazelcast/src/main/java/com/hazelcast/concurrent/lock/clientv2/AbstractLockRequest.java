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

package com.hazelcast.concurrent.lock.clientv2;

import com.hazelcast.clientv2.AbstractClientRequest;
import com.hazelcast.concurrent.lock.InternalLockNamespace;
import com.hazelcast.concurrent.lock.LockOperation;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * @mdogan 5/3/13
 */
public abstract class AbstractLockRequest extends AbstractClientRequest {

    private Data key;

    private int threadId;

    private long ttl = -1;

    private long timeout = -1;

    public AbstractLockRequest() {
    }

    public AbstractLockRequest(Data key, int threadId) {
        this.key = key;
        this.threadId = threadId;
    }

    public AbstractLockRequest(Data key, int threadId, long ttl, long timeout) {
        this.key = key;
        this.threadId = threadId;
        this.ttl = ttl;
        this.timeout = timeout;
    }

    @Override
    public final Object process() throws Exception {
        LockOperation op = new LockOperation(getNamespace(), key, threadId, ttl, timeout);
        return clientEngine.invoke(getServiceName(), op, key);
    }

    protected abstract InternalLockNamespace getNamespace();

    @Override
    public final String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    @Override
    public final void writePortable(PortableWriter writer) throws IOException {
        writer.writeInt("thread", threadId);
        writer.writeLong("ttl", ttl);
        writer.writeLong("timeout", timeout);

        ObjectDataOutput out = writer.getRawDataOutput();
        key.writeData(out);
    }

    @Override
    public final void readPortable(PortableReader reader) throws IOException {
        threadId = reader.readInt("thread");
        ttl = reader.readLong("ttl");
        timeout = reader.readLong("timeout");

        ObjectDataInput in = reader.getRawDataInput();
        key = new Data();
        key.readData(in);
    }
}
