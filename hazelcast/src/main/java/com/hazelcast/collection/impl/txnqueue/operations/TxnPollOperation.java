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
import com.hazelcast.core.ItemEventType;
import com.hazelcast.internal.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

/**
 * Poll operation for the transactional queue.
 */
public class TxnPollOperation extends BaseTxnQueueOperation implements Notifier, MutatingOperation {

    private Data data;

    public TxnPollOperation() {
    }

    public TxnPollOperation(String name, long itemId) {
        super(name, itemId);
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        data = queueContainer.txnCommitPoll(getItemId());
        response = data != null;
    }

    @Override
    public void afterRun() throws Exception {
        LocalQueueStatsImpl queueStats = getQueueService().getLocalQueueStatsImpl(name);
        if (data == null) {
            queueStats.incrementEmptyPolls();
        } else {
            queueStats.incrementPolls();
            publishEvent(ItemEventType.REMOVED, data);
        }
    }

    @Override
    public boolean shouldNotify() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        QueueContainer container = getContainer();
        return container.getOfferWaitNotifyKey();
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new TxnPollBackupOperation(name, getItemId());
    }

    @Override
    public boolean isRemoveOperation() {
        return true;
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.TXN_POLL;
    }

}
