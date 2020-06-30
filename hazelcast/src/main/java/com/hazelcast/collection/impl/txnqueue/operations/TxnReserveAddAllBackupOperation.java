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
import com.hazelcast.collection.impl.queue.operations.QueueOperation;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.transaction.TransactionalQueue;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Transaction prepare operation for a queue add all, executed on the backup replica.
 * <p>
 * Adds the item IDs to the collection of IDs offered by a transaction.
 * The check if the queue can accommodate for all items offered in a
 * transaction is done on the partition owner.
 *
 * @see TxnReserveAddAllOperation
 * @see TransactionalQueue#addAll(Collection)
 */
public class TxnReserveAddAllBackupOperation extends QueueOperation implements BackupOperation {

    private Set<Long> itemIds;

    private UUID transactionId;

    public TxnReserveAddAllBackupOperation() {
    }

    public TxnReserveAddAllBackupOperation(String name, Set<Long> itemIds, UUID transactionId) {
        super(name);
        this.itemIds = itemIds;
        this.transactionId = transactionId;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        queueContainer.txnAddAllBackupReserve(itemIds, transactionId);
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.TXN_RESERVE_OFFER_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(itemIds.size());
        for (long itemId : itemIds) {
            out.writeLong(itemId);
        }
        UUIDSerializationUtil.writeUUID(out, transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        itemIds = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            itemIds.add(in.readLong());
        }
        transactionId = UUIDSerializationUtil.readUUID(in);
    }
}
