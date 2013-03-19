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

package com.hazelcast.queue.tx;

import com.hazelcast.core.ItemEventType;
import com.hazelcast.queue.PollBackupOperation;
import com.hazelcast.queue.QueueItem;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.transaction.TransactionException;

/**
 * @ali 12/6/12
 */
public class TxPollOperation extends TransactionalQueueOperation {

    private transient QueueItem item;

    public TxPollOperation() {
    }

    public TxPollOperation(String name, String txnId, long timeoutMillis) {
        super(name, txnId, timeoutMillis);
    }

    protected void process() throws TransactionException {
        item = getOrCreateContainer().txPoll(getTransactionId());
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
        return item;
    }

    public void innerAfterRun() throws Exception {
        if (item != null) {
            publishEvent(ItemEventType.REMOVED, item.getData());
        }
    }

    public boolean shouldBackup() {
        return item != null && isCommitted();
    }

    public Operation getBackupOperation() {
        return new PollBackupOperation(name, item.getItemId());
    }

    public boolean shouldNotify() {
        return item != null && isCommitted();
    }

    public WaitNotifyKey getNotifiedKey() {
        return getOrCreateContainer().getOfferWaitNotifyKey();
    }

    public WaitNotifyKey getWaitKey() {
        return getOrCreateContainer().getPollWaitNotifyKey();
    }

    public boolean shouldWait() {
        return getWaitTimeoutMillis() != 0 && getOrCreateContainer().size() == 0;
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
    }
}
