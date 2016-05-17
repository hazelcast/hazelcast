/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.collection.impl.CollectionTxnUtil;
import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.collection.impl.queue.operations.QueueOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupOperation;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * Provides backup operation during transactional prepare operation.
 */
public class TxnPrepareBackupOperation extends QueueOperation implements BackupOperation {

    private long[] itemIds;
    private String transactionId;

    public TxnPrepareBackupOperation() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public TxnPrepareBackupOperation(String name, long[] itemIds, String transactionId) {
        super(name);
        this.itemIds = itemIds;
        this.transactionId = transactionId;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getOrCreateContainer();
        for (long itemId : itemIds) {
            boolean remove = CollectionTxnUtil.isRemove(itemId);
            queueContainer.txnEnsureBackupReserve(Math.abs(itemId), transactionId, remove);
        }

    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.TXN_PREPARE_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(transactionId);
        out.writeLongArray(itemIds);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        transactionId = in.readUTF();
        itemIds = in.readLongArray();
    }
}
