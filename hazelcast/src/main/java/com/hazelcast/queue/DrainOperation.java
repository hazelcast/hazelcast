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
import com.hazelcast.nio.Data;
import com.hazelcast.spi.Operation;
import com.sun.tools.javac.util.Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * @ali 12/19/12
 */
public class DrainOperation extends QueueBackupAwareOperation {

    int maxSize = -1;

    transient List<QueueItem> itemList;

    //TODO how about waiting polls

    public DrainOperation() {
    }

    public DrainOperation(String name, int maxSize) {
        super(name);
        this.maxSize = maxSize;
    }

    public void run() throws Exception {
        QueueContainer container = getContainer();
        itemList = container.drain(maxSize);
        Set<Long> keySet = new HashSet<Long>(itemList.size());
        List<Data> dataList = new ArrayList<Data>(itemList.size());
        for (QueueItem item: itemList){
            keySet.add(item.getItemId());
            dataList.add(item.getData());
        }
        response = dataList;
        if (!container.isStoreAsync()) {
            container.getStore().deleteAll(keySet);
        }
    }

    public void afterRun() throws Exception {
        if (itemList.size() > 0){
            List<Data> dataList = (List<Data>)response;
            for (Data data: dataList){
                publishEvent(ItemEventType.REMOVED, data);
            }
        }
    }

    public boolean shouldBackup() {
        return itemList.size() > 0;
    }

    public Operation getBackupOperation() {
        return new DrainBackupOperation(name, maxSize);
    }

    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(maxSize);
    }

    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        maxSize = in.readInt();
    }
}
