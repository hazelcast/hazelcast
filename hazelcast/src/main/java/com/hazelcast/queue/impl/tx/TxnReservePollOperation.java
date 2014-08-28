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

package com.hazelcast.queue.impl.tx;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.queue.impl.QueueBackupAwareOperation;
import com.hazelcast.queue.impl.QueueContainer;
import com.hazelcast.queue.impl.QueueDataSerializerHook;
import com.hazelcast.queue.impl.QueueItem;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;

import java.io.IOException;

/**
 * Reserve poll operation for the transactional queue.
 */
public class TxnReservePollOperation extends QueueBackupAwareOperation implements WaitSupport {

    private long reservedOfferId;
    private String transactionId;

    public TxnReservePollOperation() {
    }

    public TxnReservePollOperation(String name, long timeoutMillis, long reservedOfferId, String transactionId) {
        super(name, timeoutMillis);
        this.reservedOfferId = reservedOfferId;
        this.transactionId = transactionId;
    }

    @Override
    public void run() throws Exception {
        QueueContainer container = getOrCreateContainer();
        response = container.txnPollReserve(reservedOfferId, transactionId);
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        QueueContainer container = getOrCreateContainer();
        return container.getPollWaitNotifyKey();
    }

    @Override
    public boolean shouldWait() {
        final QueueContainer container = getOrCreateContainer();
        return getWaitTimeout() != 0 && (container.size() + container.txMapSize()) == 0;
    }

    @Override
    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.TXN_RESERVE_POLL;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(reservedOfferId);
        out.writeUTF(transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        reservedOfferId = in.readLong();
        transactionId = in.readUTF();
    }

    @Override
    public boolean shouldBackup() {
        return response != null;
    }

    @Override
    public Operation getBackupOperation() {
        final QueueItem item = (QueueItem) response;
        long itemId = item.getItemId();
        return new TxnReservePollBackupOperation(name, itemId, transactionId);
    }
}
