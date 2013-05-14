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

package com.hazelcast.queue;

import com.hazelcast.core.ItemEventType;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * @ali 12/20/12
 */

public class AddAllOperation extends QueueBackupAwareOperation implements Notifier {

    private Collection<Data> dataList;
    private transient Map<Long, Data> dataMap;


    public AddAllOperation() {
    }

    public AddAllOperation(String name, Collection<Data> dataList) {
        super(name);
        this.dataList = dataList;
    }

    public void run() {
        QueueContainer container = getOrCreateContainer();
        if (container.hasEnoughCapacity()) {
            dataMap = container.addAll(dataList);
            response = true;
        } else {
            response = false;
        }

    }

    public void afterRun() throws Exception {
        getQueueService().getLocalQueueStatsImpl(name).incrementOtherOperations();
        if (Boolean.TRUE.equals(response)) {
            for (Data data : dataList) {
                publishEvent(ItemEventType.ADDED, data);
            }
        }
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    public Operation getBackupOperation() {
        return new AddAllBackupOperation(name, dataMap);
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(dataList.size());
        for (Data data : dataList) {
            data.writeData(out);
        }
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        dataList = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            dataList.add(IOUtil.readData(in));
        }
    }

    public boolean shouldNotify() {
        return Boolean.TRUE.equals(response);
    }

    public WaitNotifyKey getNotifiedKey() {
        return getOrCreateContainer().getPollWaitNotifyKey();
    }

    public int getId() {
        return QueueDataSerializerHook.ADD_ALL;
    }
}
