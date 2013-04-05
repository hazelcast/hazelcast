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
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * @ali 4/4/13
 */
public class TxnLockAndGetOperation extends CollectionKeyBasedOperation implements WaitSupport, TxnMultiMapOperation {

    long timeout;
    long ttl;
    int threadId;
    Data value;
    int operationType;

    public TxnLockAndGetOperation() {
    }

    public TxnLockAndGetOperation(CollectionProxyId proxyId, Data dataKey, Data value, long timeout, long ttl, int threadId) {
        super(proxyId, dataKey);
        this.value = value;
        this.timeout = timeout;
        this.ttl = ttl;
        this.threadId = threadId;
    }

    public void run() throws Exception {
        CollectionContainer container =  getOrCreateContainer();
        if (!container.txnLock(dataKey, getCallerUuid(), threadId, ttl)) {
            throw new TransactionException("Lock failed.");
        }
        CollectionRecord record = new CollectionRecord(isBinary() ? value : toObject(value));
        CollectionWrapper wrapper = null;
        Collection<CollectionRecord> coll = null;
        long recordId = -1;
        switch (operationType){
            case PUT_OPERATION:
                wrapper = getOrCreateCollectionWrapper();
                coll = wrapper.getCollection();
                if (!(coll instanceof Set) || !coll.contains(record)){
                    recordId = container.nextId();
                }
                response = recordId;
                break;
            case REMOVE_OPERATION:
                wrapper = getCollectionWrapper();
                if (wrapper != null){
                    coll = wrapper.getCollection();
                    Iterator<CollectionRecord> iter = coll.iterator();
                    while (iter.hasNext()){
                        CollectionRecord r = iter.next();
                        if (r.equals(record)){
                            recordId = r.getRecordId();
                            break;
                        }
                    }
                }
                response = recordId;
                break;
            case REMOVE_ALL_OPERATION:
                wrapper = getCollectionWrapper();
                if (wrapper != null){
                    coll = wrapper.getCollection();
                }
                response = new CollectionResponse(coll, getNodeEngine());
                break;
        }
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

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeNullableData(out, value);
        value.writeData(out);
        out.writeLong(timeout);
        out.writeLong(ttl);
        out.writeInt(threadId);
        out.writeInt(operationType);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = IOUtil.readNullableData(in);
        timeout = in.readLong();
        ttl = in.readLong();
        threadId = in.readInt();
        operationType = in.readInt();
    }
}
