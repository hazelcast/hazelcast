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

package com.hazelcast.queue.impl;

import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitSupport;

/**
 * Pool operation for Queue.
 */
public final class PollOperation extends QueueBackupAwareOperation
        implements WaitSupport, Notifier, IdentifiedDataSerializable {

    private QueueItem item;

    public PollOperation() {
    }

    public PollOperation(String name, long timeoutMillis) {
        super(name, timeoutMillis);
    }

    @Override
    public void run() {
        item = getOrCreateContainer().poll();
        if (item != null) {
            response = item.getData();
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (response != null) {
            getQueueService().getLocalQueueStatsImpl(name).incrementPolls();
            publishEvent(ItemEventType.REMOVED, item.getData());
        } else {
            getQueueService().getLocalQueueStatsImpl(name).incrementEmptyPolls();
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
        return getOrCreateContainer().getOfferWaitNotifyKey();
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return getOrCreateContainer().getPollWaitNotifyKey();
    }

    @Override
    public boolean shouldWait() {
        return getWaitTimeout() != 0 && getOrCreateContainer().size() == 0;
    }

    @Override
    public void onWaitExpire() {
        getResponseHandler().sendResponse(null);
    }

    @Override
    public int getFactoryId() {
        return QueueDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.POLL;
    }
}
