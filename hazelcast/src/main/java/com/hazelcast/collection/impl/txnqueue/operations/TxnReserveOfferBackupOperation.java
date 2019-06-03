/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.collection.impl.queue.operations.QueueOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;

/**
 * Transaction prepare operation for a queue offer, executed on the backup replica.
 * <p>
 * Adds the item ID to the collection of IDs offered by a transaction.
 * The check if the queue can accomodate for all items offered in a
 * transaction is done on the partition owner.
 *
 * @see TxnReserveOfferOperation
 * @see com.hazelcast.core.TransactionalQueue#offer(Object)
 */
public class TxnReserveOfferBackupOperation extends QueueOperation implements BackupOperation {

    private long itemId;

    private String transactionId;

    public TxnReserveOfferBackupOperation() {
    }

    public TxnReserveOfferBackupOperation(String name, long itemId, String transactionId) {
        super(name);
        this.itemId = itemId;
        this.transactionId = transactionId;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        queueContainer.txnOfferBackupReserve(itemId, transactionId);
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.TXN_RESERVE_OFFER_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
        out.writeUTF(transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
        transactionId = in.readUTF();
    }
}
