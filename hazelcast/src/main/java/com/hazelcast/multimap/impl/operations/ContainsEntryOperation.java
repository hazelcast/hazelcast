/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.internal.locksupport.LockWaitNotifyKey;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.io.IOException;

public class ContainsEntryOperation extends AbstractMultiMapOperation implements BlockingOperation, ReadonlyOperation {

    private Data key;
    private Data value;
    private long threadId;

    public ContainsEntryOperation() {
    }

    public ContainsEntryOperation(String name, Data key, Data value) {
        super(name);
        this.key = key;
        this.value = value;
    }

    public ContainsEntryOperation(String name, Data key, Data value, long threadId) {
        super(name);
        this.key = key;
        this.value = value;
        this.threadId = threadId;
    }

    @Override
    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        if (key != null && value != null) {
            response = container.containsEntry(isBinary(), key, value);
        } else if (key != null) {
            response = container.containsKey(key);
        } else {
            response = container.containsValue(isBinary(), value);
        }
    }

    public long getThreadId() {
        return threadId;
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(threadId);
        IOUtil.writeData(out, key);
        IOUtil.writeData(out, value);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        threadId = in.readLong();
        key = IOUtil.readData(in);
        value = IOUtil.readData(in);
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.CONTAINS_ENTRY;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DistributedObjectNamespace(MultiMapService.SERVICE_NAME, name), key);
    }

    @Override
    public boolean shouldWait() {
        if (key == null) {
            return false;
        }
        MultiMapContainer container = getOrCreateContainer();
        if (container.isTransactionallyLocked(key)) {
            return !container.canAcquireLock(key, getCallerUuid(), threadId);
        }
        return false;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(new OperationTimeoutException("Cannot read transactionally locked entry!"));
    }
}
