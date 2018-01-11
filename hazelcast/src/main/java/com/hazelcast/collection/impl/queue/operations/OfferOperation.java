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

package com.hazelcast.collection.impl.queue.operations;

import com.hazelcast.collection.impl.queue.QueueContainer;
import com.hazelcast.collection.impl.queue.QueueDataSerializerHook;
import com.hazelcast.core.ItemEventType;
import com.hazelcast.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.impl.MutatingOperation;

import java.io.IOException;

/**
 * Contains offer operation for the Queue.
 */
public final class OfferOperation extends QueueBackupAwareOperation
        implements BlockingOperation, Notifier, IdentifiedDataSerializable, MutatingOperation {

    private Data data;
    private long itemId;

    public OfferOperation() {
    }

    public OfferOperation(final String name, final long timeout, final Data data) {
        super(name, timeout);
        this.data = data;
    }

    @Override
    public void run() {
        QueueContainer queueContainer = getContainer();
        if (queueContainer.hasEnoughCapacity()) {
            itemId = queueContainer.offer(data);
            response = true;
        } else {
            response = false;
        }
    }

    @Override
    public void afterRun() throws Exception {
        LocalQueueStatsImpl stats = getQueueService().getLocalQueueStatsImpl(name);
        if (Boolean.TRUE.equals(response)) {
            stats.incrementOffers();
            publishEvent(ItemEventType.ADDED, data);
        } else {
            stats.incrementRejectedOffers();
        }
    }

    @Override
    public Operation getBackupOperation() {
        return new OfferBackupOperation(name, data, itemId);
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public boolean shouldNotify() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getContainer().getPollWaitNotifyKey();
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        return getContainer().getOfferWaitNotifyKey();
    }

    @Override
    public boolean shouldWait() {
        QueueContainer container = getContainer();
        return getWaitTimeout() != 0 && !container.hasEnoughCapacity();
    }

    @Override
    public void onWaitExpire() {
        sendResponse(false);
    }

    @Override
    public int getFactoryId() {
        return QueueDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.OFFER;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(data);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        data = in.readData();
    }
}
