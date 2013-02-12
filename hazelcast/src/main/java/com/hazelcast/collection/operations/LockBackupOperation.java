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

package com.hazelcast.collection.operations;

import com.hazelcast.collection.CollectionContainer;
import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

/**
 * @ali 1/16/13
 */
public class LockBackupOperation extends CollectionKeyBasedOperation implements BackupOperation {

    long ttl = LockOperation.DEFAULT_LOCK_TTL;

    int threadId;

    String firstCaller;

    public LockBackupOperation() {
    }

    public LockBackupOperation(CollectionProxyId proxyId, Data dataKey, long ttl, int threadId, String firstCaller) {
        super(proxyId, dataKey);
        this.ttl = ttl;
        this.threadId = threadId;
        this.firstCaller = firstCaller;
    }

    public void run() throws Exception {
        CollectionContainer container = getOrCreateContainer();
        response = container.lock(dataKey, firstCaller, threadId, ttl);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(firstCaller);
        out.writeInt(threadId);
        out.writeLong(ttl);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        firstCaller = in.readUTF();
        threadId = in.readInt();
        ttl = in.readLong();
    }
}
