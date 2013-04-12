/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.multimap.tx;

import com.hazelcast.collection.*;
import com.hazelcast.collection.operations.CollectionKeyBasedOperation;
import com.hazelcast.collection.operations.CollectionResponse;
import com.hazelcast.concurrent.lock.LockNamespace;
import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;
import java.util.Collection;

/**
 * @ali 4/4/13
 */
public class TxnLockAndGetOperation extends CollectionKeyBasedOperation implements WaitSupport, TxnMultiMapOperation {

    long timeout;
    long ttl;
    int threadId;
    boolean getCollection = false;

    public TxnLockAndGetOperation() {
    }

    public TxnLockAndGetOperation(CollectionProxyId proxyId, Data dataKey, long timeout, long ttl, int threadId) {
        super(proxyId, dataKey);
        this.timeout = timeout;
        this.ttl = ttl;
        this.threadId = threadId;
    }

    public void run() throws Exception {
        CollectionContainer container =  getOrCreateContainer();
        if (!container.txnLock(dataKey, getCallerUuid(), threadId, ttl)) {
            throw new TransactionException("Lock failed.");
        }
        CollectionWrapper wrapper = getCollectionWrapper();
        Collection<CollectionRecord> coll = wrapper != null ? wrapper.getCollection() : null;
        response = new CollectionResponse(getCollection ? coll : null).setAttachment(container.nextId());
    }

    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new LockNamespace(CollectionService.SERVICE_NAME, proxyId), dataKey);
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

    public void setGetCollection(boolean getCollection) {
        this.getCollection = getCollection;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(timeout);
        out.writeLong(ttl);
        out.writeInt(threadId);
        out.writeBoolean(getCollection);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        timeout = in.readLong();
        ttl = in.readLong();
        threadId = in.readInt();
        getCollection = in.readBoolean();
    }
}
