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
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.OfferBackupOperation;
import com.hazelcast.queue.QueueContainer;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.transaction.Transaction;
import com.hazelcast.transaction.TransactionException;

import java.io.IOException;

/**
 * User: ali
 * Date: 11/14/12
 * Time: 12:14 AM
 */
public class TxOfferOperation extends TransactionalQueueOperation {

    private Data data;

    private transient boolean offered;
    private transient long itemId;

    public TxOfferOperation() {
    }

    public TxOfferOperation(String name, String txnId, long timeoutMillis, Data data) {
        super(name, txnId, timeoutMillis);
        this.data = data;
    }

    protected void process() throws TransactionException {
        QueueContainer container = getOrCreateContainer();
        if (container.hasEnoughCapacity()) {
            itemId = container.txOffer(getTransactionId(), data);
            offered = true;
        }
        else {
            offered = false;
        }

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
        return offered;
    }

    public void innerAfterRun() throws Exception {
        if (offered) {
            publishEvent(ItemEventType.ADDED, data);
        }
    }

    public Operation getBackupOperation() {
        return new OfferBackupOperation(name, data, itemId);
    }

    public boolean shouldBackup() {
        return offered && isCommitted();
    }

    public boolean shouldNotify() {
        return offered && isCommitted();
    }

    public WaitNotifyKey getNotifiedKey() {
        return getOrCreateContainer().getPollWaitNotifyKey();
    }

    public WaitNotifyKey getWaitKey() {
        return getOrCreateContainer().getOfferWaitNotifyKey();
    }

    public boolean shouldWait() {
        QueueContainer container = getOrCreateContainer();
        return getState() == Transaction.State.ACTIVE && getWaitTimeoutMillis() != 0 && !container.hasEnoughCapacity();
    }

    public void onWaitExpire() {
        getResponseHandler().sendResponse(Boolean.FALSE);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        data.writeData(out);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        data = IOUtil.readData(in);
    }
}
