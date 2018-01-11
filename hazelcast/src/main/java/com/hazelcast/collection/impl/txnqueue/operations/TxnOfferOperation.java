/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.collection.impl.txnqueue.operations;

import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

/**
 * Transaction commit operation for a queue offer, executed on the primary replica.
 *
 * @see com.hazelcast.core.TransactionalQueue#offer(Object)
 * @see TxnReserveOfferOperation
 */
public class TxnOfferOperation extends BaseTxnQueueOperation implements Notifier, MutatingOperation {


    private Data data;

    public TxnOfferOperation() {
    }

    public TxnOfferOperation(String name, long itemId, Data data) {
        super(name, itemId);
        this.data = data;
    }

    @Override
    public void run() throws Exception {
        QueueContainer createContainer = getContainer();
        response = createContainer.txnCommitOffer(getItemId(), data, false);
    }

    @Override
    public void afterRun() throws Exception {
        LocalQueueStatsImpl queueStats = getQueueService().getLocalQueueStatsImpl(name);
        if (Boolean.TRUE.equals(response)) {
            queueStats.incrementOffers();
            publishEvent(ItemEventType.ADDED, data);
        } else {
            queueStats.incrementRejectedOffers();
        }
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new TxnOfferBackupOperation(name, getItemId(), data);
    }

    @Override
    public boolean shouldNotify() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getContainer().getPollWaitNotifyKey();
    }

    @Override
    public boolean isRemoveOperation() {
        return false;
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.TXN_OFFER;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(data);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        data = in.readData();
    }
}
