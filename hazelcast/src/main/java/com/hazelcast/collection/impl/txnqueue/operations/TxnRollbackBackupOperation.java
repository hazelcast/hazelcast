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
 * Provides backup operation during transactional rollback operation.
 */
public class TxnRollbackBackupOperation extends QueueOperation implements BackupOperation {

    private long[] itemIds;

    public TxnRollbackBackupOperation() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public TxnRollbackBackupOperation(String name, long[] itemIds) {
        super(name);
        this.itemIds = itemIds;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        for (long itemId : itemIds) {
            if (CollectionTxnUtil.isRemove(itemId)) {
                response = queueContainer.txnRollbackPoll(itemId, true);
            } else {
                response = queueContainer.txnRollbackOfferBackup(-itemId);
            }
        }
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.TXN_ROLLBACK_BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLongArray(itemIds);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemIds = in.readLongArray();
    }
}
