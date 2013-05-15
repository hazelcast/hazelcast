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

import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.QueueBackupAwareOperation;
import com.hazelcast.queue.QueueDataSerializerHook;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;

/**
 * @ali 3/25/13
 */
public class TxnPollOperation extends QueueBackupAwareOperation implements Notifier {

    long itemId;

    transient Data data;

    public TxnPollOperation() {
    }

    public TxnPollOperation(String name, long itemId) {
        super(name);
        this.itemId = itemId;
    }

    public void run() throws Exception {
        data = getOrCreateContainer().txnCommitPoll(itemId);
        response = data != null;
    }

    public void afterRun() throws Exception {
        if (response != null) {
            getQueueService().getLocalQueueStatsImpl(name).incrementPolls();
            publishEvent(ItemEventType.REMOVED, data);
        } else {
            getQueueService().getLocalQueueStatsImpl(name).incrementEmptyPolls();
        }
    }

    public boolean shouldNotify() {
        return Boolean.TRUE.equals(response);
    }

    public WaitNotifyKey getNotifiedKey() {
        return getOrCreateContainer().getOfferWaitNotifyKey();
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    public Operation getBackupOperation() {
        return new TxnPollBackupOperation(name, itemId);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(itemId);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        itemId = in.readLong();
    }

    public int getId() {
        return QueueDataSerializerHook.TXN_POLL;
    }

}
