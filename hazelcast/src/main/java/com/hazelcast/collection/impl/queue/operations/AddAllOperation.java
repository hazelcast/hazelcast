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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Add collection items to the Queue.
 */
public class AddAllOperation extends QueueBackupAwareOperation implements Notifier, MutatingOperation {

    private Collection<Data> dataList;
    private Map<Long, Data> dataMap;

    public AddAllOperation() {
    }

    public AddAllOperation(String name, Collection<Data> dataList) {
        super(name);
        this.dataList = dataList;
    }

    @Override
    public void run() {
        QueueContainer queueContainer = getContainer();
        if (queueContainer.hasEnoughCapacity()) {
            dataMap = queueContainer.addAll(dataList);
            response = true;
        } else {
            response = false;
        }
    }

    @Override
    public void afterRun() throws Exception {
        LocalQueueStatsImpl stats = getQueueService().getLocalQueueStatsImpl(name);
        stats.incrementOtherOperations();
        if (Boolean.TRUE.equals(response)) {
            for (Data data : dataList) {
                publishEvent(ItemEventType.ADDED, data);
            }
        }
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new AddAllBackupOperation(name, dataMap);
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
    public int getClassId() {
        return QueueDataSerializerHook.ADD_ALL;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(dataList.size());
        for (Data data : dataList) {
            IOUtil.writeData(out, data);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        dataList = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            dataList.add(IOUtil.readData(in));
        }
    }
}
