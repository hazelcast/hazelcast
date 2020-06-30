/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.collection.impl.queue.operations.QueueBackupAwareOperation;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.transaction.TransactionalQueue;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;

/**
 * Transaction prepare operation for a queue addAll, executed on the primary replica.
 * <p>
 * Checks if the queue can accommodate the items in addition to the number provided
 * to the constructor and returns the next item ID. This check is done on a scope of
 * one transaction and does not include other transactions. It can also happen that
 * after this check succeeds, the user will add more items to the queue which means that
 * the queue can no longer accommodate for the items for which it has returned an item ID.
 * <p>
 * The operation can also wait until there is enough room or the wait timeout has elapsed.
 *
 * @see TransactionalQueue#addAll(Collection)
 * @see TxnAddAllOperation
 */
public class TxnReserveAddAllOperation extends QueueBackupAwareOperation implements BlockingOperation, MutatingOperation {
    /** The number of items already offered in this transactional queue */
    private int txSize;
    private int requestedAllocation;
    private UUID transactionId;

    public TxnReserveAddAllOperation() {
    }

    public TxnReserveAddAllOperation(String name, long timeoutMillis, int txSize, int requestedAllocation, UUID transactionId) {
        super(name, timeoutMillis);
        this.txSize = txSize;
        this.requestedAllocation = requestedAllocation;
        this.transactionId = transactionId;
    }

    /**
     * {@inheritDoc}
     * Sets the response to the next item IDs if the queue can
     * accommodate {@code txSize + requestedAllocation} items.
     */
    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        if (queueContainer.hasEnoughCapacity(txSize + requestedAllocation)) {
            response = queueContainer.txnAddAllReserve(transactionId, requestedAllocation);
        }
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        QueueContainer queueContainer = getContainer();
        return queueContainer.getOfferWaitNotifyKey();
    }

    @Override
    public boolean shouldWait() {
        QueueContainer queueContainer = getContainer();
        return getWaitTimeout() != 0 && !queueContainer.hasEnoughCapacity(txSize + requestedAllocation);
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

    @Override
    public boolean shouldBackup() {
        return response != null;
    }

    @Override
    public Operation getBackupOperation() {
        return new TxnReserveAddAllBackupOperation(name, (Set<Long>) response, transactionId);
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.TXN_RESERVE_OFFER;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(txSize);
        out.writeInt(requestedAllocation);
        UUIDSerializationUtil.writeUUID(out, transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        txSize = in.readInt();
        requestedAllocation = in.readInt();
        transactionId = UUIDSerializationUtil.readUUID(in);
    }
}
