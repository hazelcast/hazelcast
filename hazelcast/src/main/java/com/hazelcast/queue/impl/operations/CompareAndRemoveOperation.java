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

package com.hazelcast.queue.impl.operations;

import com.hazelcast.core.ItemEventType;
import com.hazelcast.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.queue.impl.QueueContainer;
import com.hazelcast.queue.impl.QueueDataSerializerHook;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * This class triggers iterator and if find same item in the Queue, remove this item.
 */
public class CompareAndRemoveOperation extends QueueBackupAwareOperation implements Notifier {

    private Collection<Data> dataList;
    private Map<Long, Data> dataMap;
    private boolean retain;

    public CompareAndRemoveOperation() {
    }

    public CompareAndRemoveOperation(String name, Collection<Data> dataList, boolean retain) {
        super(name);
        this.dataList = dataList;
        this.retain = retain;
    }

    @Override
    public void run() {
        QueueContainer container = getOrCreateContainer();
        dataMap = container.compareAndRemove(dataList, retain);
        response = dataMap.size() > 0;
    }

    @Override
    public void afterRun() throws Exception {
        LocalQueueStatsImpl stats = getQueueService().getLocalQueueStatsImpl(name);
        stats.incrementOtherOperations();
        if (hasListener()) {
            for (Data data : dataMap.values()) {
                publishEvent(ItemEventType.REMOVED, data);
            }
        }
    }

    @Override
    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public Operation getBackupOperation() {
        return new CompareAndRemoveBackupOperation(name, dataMap.keySet());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeBoolean(retain);
        out.writeInt(dataList.size());
        for (Data data : dataList) {
            data.writeData(out);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        retain = in.readBoolean();
        int size = in.readInt();
        dataList = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            dataList.add(IOUtil.readData(in));
        }
    }

    @Override
    public boolean shouldNotify() {
        return Boolean.TRUE.equals(response);
    }

    @Override
    public WaitNotifyKey getNotifiedKey() {
        return getOrCreateContainer().getOfferWaitNotifyKey();
    }

    @Override
    public int getId() {
        return QueueDataSerializerHook.COMPARE_AND_REMOVE;
    }
}
