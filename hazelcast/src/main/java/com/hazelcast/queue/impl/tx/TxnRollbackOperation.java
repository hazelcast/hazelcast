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
import com.hazelcast.queue.impl.operations.QueueBackupAwareOperation;
import com.hazelcast.queue.impl.QueueContainer;
import com.hazelcast.queue.impl.QueueDataSerializerHook;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;

/**
 * Rollback operation for the transactional queue.
 */
public class TxnRollbackOperation extends QueueBackupAwareOperation implements Notifier {

    private long itemId;
    private boolean pollOperation;

    public TxnRollbackOperation() {
    }

    public TxnRollbackOperation(String name, long itemId, boolean pollOperation) {
        super(name);
        this.itemId = itemId;
        this.pollOperation = pollOperation;
    }

    @Override
    public void run() throws Exception {
        QueueContainer container = getOrCreateContainer();
        if (pollOperation) {
            response = container.txnRollbackPoll(itemId, false);
        } else {
            response = container.txnRollbackOffer(itemId);
        }
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public Operation getBackupOperation() {
        return new TxnRollbackBackupOperation(name, itemId, pollOperation);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
        out.writeBoolean(pollOperation);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
        pollOperation = in.readBoolean();
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.TXN_ROLLBACK;
    }

    @Override
    public boolean shouldNotify() {
        return true;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        if (pollOperation) {
            return getOrCreateContainer().getPollWaitNotifyKey();
        }
        return getOrCreateContainer().getOfferWaitNotifyKey();
    }
}
