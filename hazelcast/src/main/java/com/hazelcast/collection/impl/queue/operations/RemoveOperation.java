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
import com.hazelcast.core.ItemEventType;
import com.hazelcast.internal.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

import java.io.IOException;

/**
 * Remove operation for the Queue.
 */
public class RemoveOperation extends QueueBackupAwareOperation implements Notifier, MutatingOperation {

    private Data data;
    private long itemId = -1;

    public RemoveOperation() {
    }

    public RemoveOperation(String name, Data data) {
        super(name);
        this.data = data;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        itemId = queueContainer.remove(data);
        response = itemId != -1;
    }

    @Override
    public void afterRun() throws Exception {
        LocalQueueStatsImpl stats = getQueueService().getLocalQueueStatsImpl(name);
        stats.incrementOtherOperations();
        if (itemId != -1) {
            publishEvent(ItemEventType.REMOVED, data);
        }
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new RemoveBackupOperation(name, itemId);
    }

    @Override
    public boolean shouldNotify() {
        return itemId != -1;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getContainer().getOfferWaitNotifyKey();
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.REMOVE;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        IOUtil.writeData(out, data);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        data = IOUtil.readData(in);
    }
}
