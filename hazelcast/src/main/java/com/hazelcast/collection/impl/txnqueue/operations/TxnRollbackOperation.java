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

import com.hazelcast.collection.impl.CollectionTxnUtil;
import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.collection.impl.queue.operations.QueueBackupAwareOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * Rollback operation for the transactional queue.
 */
public class TxnRollbackOperation extends QueueBackupAwareOperation implements Notifier {

    private long[] itemIds;

    private transient long shouldNotify;

    public TxnRollbackOperation() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public TxnRollbackOperation(int partitionId, String name, long[] itemIds) {
        super(name);
        setPartitionId(partitionId);
        this.itemIds = itemIds;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        for (long itemId : itemIds) {
            if (CollectionTxnUtil.isRemove(itemId)) {
                response = queueContainer.txnRollbackPoll(itemId, false);
            } else {
                response = queueContainer.txnRollbackOffer(-itemId);
            }
        }
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return new TxnRollbackBackupOperation(name, itemIds);
    }

    @Override
    public boolean shouldNotify() {
        for (long itemId : itemIds) {
            shouldNotify += CollectionTxnUtil.isRemove(itemId) ? 1 : -1;
        }
        return shouldNotify != 0;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        QueueContainer queueContainer = getContainer();

        if (CollectionTxnUtil.isRemove(shouldNotify)) {
            return queueContainer.getPollWaitNotifyKey();
        }
        return queueContainer.getOfferWaitNotifyKey();
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.TXN_ROLLBACK;
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
