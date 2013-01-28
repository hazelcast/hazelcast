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
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

/**
 * @ali 1/16/13
 */
public class UnlockBackupOperation extends CollectionKeyBasedOperation implements BackupOperation {

    int threadId;

    Address firstCaller;

    public UnlockBackupOperation() {
    }

    public UnlockBackupOperation(CollectionProxyId proxyId, Data dataKey, int threadId, Address firstCaller) {
        super(proxyId, dataKey);
        this.threadId = threadId;
        this.firstCaller = firstCaller;
    }

    public void run() throws Exception {
        CollectionContainer container = getOrCreateContainer();
        response = container.unlock(dataKey, firstCaller, threadId);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(threadId);
        firstCaller.writeData(out);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        threadId = in.readInt();
        firstCaller = new Address();
        firstCaller.readData(in);
    }
}
