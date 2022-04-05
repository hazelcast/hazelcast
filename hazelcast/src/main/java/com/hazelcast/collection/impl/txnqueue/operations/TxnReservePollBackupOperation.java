/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.transaction.TransactionalQueue;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupOperation;

import java.io.IOException;
import java.util.UUID;

/**
 * Transaction prepare operation for a queue poll, executed on the backup replica.
 *
 * @see TransactionalQueue#poll
 * @see TxnPollOperation
 */
public class TxnReservePollBackupOperation extends QueueOperation implements BackupOperation {

    private long itemId;
    private UUID transactionId;

    public TxnReservePollBackupOperation() {
    }

    public TxnReservePollBackupOperation(String name, long itemId, UUID transactionId) {
        super(name);
        this.itemId = itemId;
        this.transactionId = transactionId;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        queueContainer.txnPollBackupReserve(itemId, transactionId);
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.TXN_RESERVE_POLL_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
        UUIDSerializationUtil.writeUUID(out, transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
        transactionId = UUIDSerializationUtil.readUUID(in);
    }
}
