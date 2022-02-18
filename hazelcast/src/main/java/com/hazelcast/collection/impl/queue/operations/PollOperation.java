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

package com.hazelcast.collection.impl.queue.operations;

import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.collection.impl.queue.QueueItem;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.internal.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

/**
 * Pool operation for Queue.
 */
public final class PollOperation extends QueueBackupAwareOperation
        implements BlockingOperation, Notifier, IdentifiedDataSerializable, MutatingOperation {

    private QueueItem item;

    public PollOperation() {
    }

    public PollOperation(String name, long timeoutMillis) {
        super(name, timeoutMillis);
    }

    @Override
    public void run() {
        QueueContainer queueContainer = getContainer();
        item = queueContainer.poll();
        if (item != null) {
            response = item.getSerializedObject();
        }
    }

    @Override
    public void afterRun() {
        LocalQueueStatsImpl stats = getQueueService().getLocalQueueStatsImpl(name);
        if (response != null) {
            stats.incrementPolls();
            publishEvent(ItemEventType.REMOVED, item.getSerializedObject());
        } else {
            stats.incrementEmptyPolls();
        }
    }

    @Override
    public boolean shouldBackup() {
        return response != null;
    }

    @Override
    public Operation getBackupOperation() {
        return new PollBackupOperation(name, item.getItemId());
    }

    @Override
    public boolean shouldNotify() {
        return response != null;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getContainer().getOfferWaitNotifyKey();
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return getContainer().getPollWaitNotifyKey();
    }

    @Override
    public boolean shouldWait() {
        return getWaitTimeout() != 0 && getContainer().size() == 0;
    }

    @Override
    public void onWaitExpire() {
        sendResponse(null);
    }

    @Override
    public int getFactoryId() {
        return QueueDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.POLL;
    }
}
