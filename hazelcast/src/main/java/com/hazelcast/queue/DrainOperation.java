/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.WaitNotifyKey;

import java.io.IOException;
import java.util.Collection;

/**
 * @ali 12/19/12
 */
public class DrainOperation extends QueueBackupAwareOperation implements Notifier {

    int maxSize = -1;

    transient Collection<Data> dataList;

    //TODO how about waiting polls

    public DrainOperation() {
    }

    public DrainOperation(String name, int maxSize) {
        super(name);
        this.maxSize = maxSize;
    }

    public void run() throws Exception {
        QueueContainer container = getContainer();
        dataList = container.drain(maxSize);
        response = new SerializableCollectionContainer(dataList);
    }

    public void afterRun() throws Exception {
        for (Data data : dataList) {
            publishEvent(ItemEventType.REMOVED, data);
        }
    }

    public boolean shouldBackup() {
        return dataList.size() > 0;
    }

    public Operation getBackupOperation() {
        return new DrainBackupOperation(name, maxSize);
    }

    public void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(maxSize);
    }

    public void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        maxSize = in.readInt();
    }

    public boolean shouldNotify() {
        return dataList.size() > 0;
    }

    public WaitNotifyKey getNotifiedKey() {
        return new QueueWaitNotifyKey(name, "offer");
    }
}
