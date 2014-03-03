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

package com.hazelcast.multimap.txn;

import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.multimap.MultiMapContainer;
import com.hazelcast.multimap.MultiMapDataSerializerHook;
import com.hazelcast.multimap.MultiMapService;
import com.hazelcast.multimap.MultiMapWrapper;
import com.hazelcast.multimap.operations.MultiMapKeyBasedOperation;
import com.hazelcast.multimap.operations.MultiMapResponse;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;

/**
 * @author ali 4/4/13
 */
public class TxnLockAndGetOperation extends MultiMapKeyBasedOperation implements WaitSupport, TxnMultiMapOperation {

    long timeout;
    long ttl;
    long threadId;

    public TxnLockAndGetOperation() {
    }

    public TxnLockAndGetOperation(String name, Data dataKey, long timeout, long ttl, long threadId) {
        super(name, dataKey);
        this.timeout = timeout;
        this.ttl = ttl;
        this.threadId = threadId;
    }

    public void run() throws Exception {
        MultiMapContainer container = getOrCreateContainer();
        if (!container.txnLock(dataKey, getCallerUuid(), threadId, ttl)) {
            throw new TransactionException("Transaction couldn't obtain lock!");
        }
        MultiMapWrapper wrapper = getOrCreateCollectionWrapper();

        final boolean isLocal = getResponseHandler().isLocal();
        final MultiMapResponse multiMapResponse = new MultiMapResponse(wrapper.getCollection(isLocal));
        multiMapResponse.setNextRecordId(container.nextId());
        multiMapResponse.setTxVersion(wrapper.incrementAndGetVersion());
        response = multiMapResponse;
    }

    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new DefaultObjectNamespace(MultiMapService.SERVICE_NAME, name), dataKey);
    }

    public boolean shouldWait() {
        return !getOrCreateContainer().canAcquireLock(dataKey, getCallerUuid(), threadId);
    }

    public long getWaitTimeoutMillis() {
        return timeout;
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(timeout);
        out.writeLong(ttl);
        out.writeLong(threadId);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        timeout = in.readLong();
        ttl = in.readLong();
        threadId = in.readLong();
    }

    public int getId() {
        return MultiMapDataSerializerHook.TXN_LOCK_AND_GET;
    }
}
