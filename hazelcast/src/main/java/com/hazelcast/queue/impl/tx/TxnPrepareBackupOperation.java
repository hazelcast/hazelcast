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
import com.hazelcast.queue.impl.QueueContainer;
import com.hazelcast.queue.impl.QueueDataSerializerHook;
import com.hazelcast.queue.impl.operations.QueueOperation;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

/**
 * Provides backup operation during transactional prepare operation.
 */
public class TxnPrepareBackupOperation extends QueueOperation implements BackupOperation {

    private long itemId;
    private boolean pollOperation;
    private String transactionId;

    public TxnPrepareBackupOperation() {
    }

    public TxnPrepareBackupOperation(String name, long itemId, boolean pollOperation, String transactionId) {
        super(name);
        this.itemId = itemId;
        this.pollOperation = pollOperation;
        this.transactionId = transactionId;
    }

    @Override
    public void run() throws Exception {
        QueueContainer container = getOrCreateContainer();
        response = container.txnEnsureReserve(itemId);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
        out.writeBoolean(pollOperation);
        out.writeUTF(transactionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
        pollOperation = in.readBoolean();
        transactionId = in.readUTF();
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.TXN_PREPARE_BACKUP;
    }

}
