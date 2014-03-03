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
import com.hazelcast.queue.QueueBackupAwareOperation;
import com.hazelcast.queue.QueueDataSerializerHook;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;

/**
 * @author ali 3/27/13
 */
public class TxnRollbackOperation extends QueueBackupAwareOperation implements Notifier {

    long itemId;

    boolean pollOperation;

    public TxnRollbackOperation() {
    }

    public TxnRollbackOperation(String name, long itemId, boolean pollOperation) {
        super(name);
        this.itemId = itemId;
        this.pollOperation = pollOperation;
    }

    public void run() throws Exception {
        if (pollOperation) {
            response = getOrCreateContainer().txnRollbackPoll(itemId, false);
        } else {
            response = getOrCreateContainer().txnRollbackOffer(itemId);
        }
    }

    public boolean shouldBackup() {
        return true;
    }

    public Operation getBackupOperation() {
        return new TxnRollbackBackupOperation(name, itemId, pollOperation);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
        out.writeBoolean(pollOperation);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
        pollOperation = in.readBoolean();
    }

    public int getId() {
        return QueueDataSerializerHook.TXN_ROLLBACK;
    }

    public boolean shouldNotify() {
        return true;
    }

    public WaitNotifyKey getNotifiedKey() {
        if (pollOperation) {
            return getOrCreateContainer().getPollWaitNotifyKey();
        }
        return getOrCreateContainer().getOfferWaitNotifyKey();

    }
}
