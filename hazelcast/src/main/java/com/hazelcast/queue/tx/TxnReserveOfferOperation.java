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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.queue.QueueContainer;
import com.hazelcast.queue.QueueDataSerializerHook;
import com.hazelcast.queue.QueueOperation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;

import java.io.IOException;

/**
 * Reserve offer operation for the transactional queue.
 */
public class TxnReserveOfferOperation extends QueueOperation implements WaitSupport {

    private int txSize;

    private String transactionId;

    public TxnReserveOfferOperation() {
    }

    public TxnReserveOfferOperation(String name, long timeoutMillis, int txSize, String transactionId) {
        super(name, timeoutMillis);
        this.txSize = txSize;
        this.transactionId = transactionId;
    }

    @Override
    public void run() throws Exception {
        QueueContainer container = getOrCreateContainer();
        if (container.hasEnoughCapacity(txSize + 1)) {
            response = container.txnOfferReserve(transactionId);
        }
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        QueueContainer container = getOrCreateContainer();
        return container.getOfferWaitNotifyKey();
    }

    @Override
    public boolean shouldWait() {
        QueueContainer container = getOrCreateContainer();
        return getWaitTimeout() != 0 && !container.hasEnoughCapacity(txSize + 1);
    }

    @Override
    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.TXN_RESERVE_OFFER;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(txSize);
        out.writeUTF(transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        txSize = in.readInt();
        transactionId = in.readUTF();
    }
}
