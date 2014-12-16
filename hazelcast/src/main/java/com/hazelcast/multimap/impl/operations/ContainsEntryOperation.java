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

package com.hazelcast.multimap.impl.operations;

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapDataSerializerHook;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;
import java.io.IOException;

public class ContainsEntryOperation extends MultiMapOperation implements WaitSupport {

    Data key;

    Data value;

    long threadId;

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

    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        ((MultiMapService) getService()).getLocalMultiMapStatsImpl(name).incrementOtherOperations();
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

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(threadId);
        out.writeData(key);
        out.writeData(value);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        threadId = in.readLong();
        key = in.readData();
        value = in.readData();
    }

    public int getId() {
        return MultiMapDataSerializerHook.CONTAINS_ENTRY;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DefaultObjectNamespace(MultiMapService.SERVICE_NAME, name), key);
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
        getResponseHandler().sendResponse(new OperationTimeoutException("Cannot read transactionally locked entry!"));
    }
}
