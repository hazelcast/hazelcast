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
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.UnlockOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;

import java.io.IOException;

/**
 * @mdogan 5/3/13
 */
public abstract class AbstractUnlockRequest extends KeyBasedClientRequest implements Portable  {

    private Data key;

    private int threadId;

    private boolean force;

    public AbstractUnlockRequest() {
    }

    public AbstractUnlockRequest(Data key, int threadId) {
        this.key = key;
        this.threadId = threadId;
    }

    protected AbstractUnlockRequest(Data key, int threadId, boolean force) {
        this.key = key;
        this.threadId = threadId;
        this.force = force;
    }

    protected final Object getKey() {
        return key;
    }

    protected final Operation prepareOperation() {
        return new UnlockOperation(getNamespace(), key, threadId, force);
    }

    protected abstract ObjectNamespace getNamespace();

    public final String getServiceName() {
        return LockService.SERVICE_NAME;
    }

    public void writePortable(PortableWriter writer) throws IOException {

        writer.writeInt("thread", threadId);
        writer.writeBoolean("force", force);

        ObjectDataOutput out = writer.getRawDataOutput();
        key.writeData(out);
    }

    public void readPortable(PortableReader reader) throws IOException {

        threadId = reader.readInt("thread");
        force = reader.readBoolean("force");

        ObjectDataInput in = reader.getRawDataInput();
        key = new Data();
        key.readData(in);
    }

}
