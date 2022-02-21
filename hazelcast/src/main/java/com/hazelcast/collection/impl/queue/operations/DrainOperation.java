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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;
import com.hazelcast.spi.impl.SerializableList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * This class drain items according to drain condition.
 */
public class DrainOperation extends QueueBackupAwareOperation implements Notifier, MutatingOperation {

    private int maxSize;
    private Map<Long, Data> dataMap;

    public DrainOperation() {
    }

    public DrainOperation(String name, int maxSize) {
        super(name);
        this.maxSize = maxSize;
    }

    @Override
    public void run() throws Exception {
        QueueContainer queueContainer = getContainer();
        dataMap = queueContainer.drain(maxSize);
        response = new SerializableList(new ArrayList<Data>(dataMap.values()));
    }

    @Override
    public void afterRun() throws Exception {
        LocalQueueStatsImpl stats = getQueueService().getLocalQueueStatsImpl(name);
        stats.incrementOtherOperations();
        for (Data data : dataMap.values()) {
            publishEvent(ItemEventType.REMOVED, data);
        }
    }

    @Override
    public boolean shouldBackup() {
        return dataMap.size() > 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new DrainBackupOperation(name, dataMap.keySet());
    }

    @Override
    public boolean shouldNotify() {
        return dataMap.size() > 0;
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getContainer().getOfferWaitNotifyKey();
    }

    @Override
    public int getClassId() {
        return QueueDataSerializerHook.DRAIN;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(maxSize);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        maxSize = in.readInt();
    }
}
