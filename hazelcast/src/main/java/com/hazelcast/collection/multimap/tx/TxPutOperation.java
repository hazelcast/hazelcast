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

import com.hazelcast.collection.CollectionProxyId;
import com.hazelcast.collection.CollectionService;
import com.hazelcast.collection.operations.PutBackupOperation;
import com.hazelcast.concurrent.lock.LockNamespace;
import com.hazelcast.concurrent.lock.LockWaitNotifyKey;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;

/**
 * @ali 3/13/13
 */
public class TxPutOperation extends TransactionalMultiMapOperation {

    Data value;

    public TxPutOperation() {
    }

    public TxPutOperation(CollectionProxyId proxyId, Data dataKey, Data value, int threadId, String txnId, long timeoutMillis) {
        super(proxyId, dataKey, threadId, txnId, timeoutMillis);
    }

    protected void process() throws TransactionException {
    }

    protected void prepare() throws TransactionException {
    }

    protected void commit() {
    }

    protected void rollback() {
    }

    public Operation getBackupOperation() {
        return new PutBackupOperation(proxyId, dataKey, value, -1);
    }

    public boolean shouldBackup() {
        return super.shouldBackup() && Boolean.TRUE.equals(response);
    }

    public WaitNotifyKey getWaitKey() {
        return new LockWaitNotifyKey(new LockNamespace(CollectionService.SERVICE_NAME, proxyId), dataKey);
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(false);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        value.writeData(out);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        value = new Data();
        value.readData(in);
    }
}
