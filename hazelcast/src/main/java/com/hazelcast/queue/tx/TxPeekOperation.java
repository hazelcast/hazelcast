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

package com.hazelcast.queue.tx;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.transaction.TransactionException;

/**
 * @ali 12/6/12
 */
public class TxPeekOperation extends TransactionalQueueOperation {

    private transient Data response;

    public TxPeekOperation() {
    }

    public TxPeekOperation(String name, String txnId) {
        super(name, txnId, 0);
    }

    protected void process() throws TransactionException {
        response = getOrCreateContainer().txPeek(getTransactionId());
    }

    protected void prepare() throws TransactionException {
    }

    protected void commit() {
        getOrCreateContainer().commit(getTransactionId());
    }

    protected void rollback() {
        getOrCreateContainer().rollback(getTransactionId());
    }

    public Object getResponse() {
        return response;
    }

    public boolean shouldBackup() {
        return false;
    }

    public Operation getBackupOperation() {
        return null;
    }

    public boolean shouldNotify() {
        return false;
    }

    public WaitNotifyKey getNotifiedKey() {
        return null;
    }

    public WaitNotifyKey getWaitKey() {
        return null;
    }

    public boolean shouldWait() {
        return false;
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
    }
}
