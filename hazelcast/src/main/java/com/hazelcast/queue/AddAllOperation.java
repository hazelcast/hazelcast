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
import com.hazelcast.nio.IOUtil;
import com.hazelcast.spi.Operation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * @ali 12/20/12
 */

public class AddAllOperation extends QueueBackupAwareOperation {

    private List<Data> dataList;

    private transient List<QueueItem> itemList;

    public AddAllOperation() {
    }

    public AddAllOperation(String name, List<Data> dataList) {
        super(name);
        this.dataList = dataList;
    }

    public void run() throws Exception {
        response = false;
        QueueContainer container = getContainer();
        itemList = container.addAll(dataList);
        if (itemList.size() > 0){
            response = true;
            storeAll(false);
        }
    }

    public void afterRun() throws Exception {
        if (itemList.size() > 0 && storeAll(true)){
            for (QueueItem item: itemList){
                publishEvent(ItemEventType.ADDED, item.getData());
            }
        }
    }

    private boolean storeAll(boolean async){
        boolean published = false;
        QueueContainer container = getContainer();
        if (container.isStoreAsync() == async && container.getStore().isEnabled()) {
            Map<Long, QueueStoreValue> map = new HashMap<Long, QueueStoreValue>(itemList.size());
            for (QueueItem item: itemList){
                QueueStoreValue storeValue = new QueueStoreValue(item.getData());
                map.put(item.getItemId(), storeValue);
                published = true;
                publishEvent(ItemEventType.ADDED, item.getData());
            }
            container.getStore().storeAll(map);
        }
        return published;
    }

    public boolean shouldBackup() {
        return Boolean.TRUE.equals(response);
    }

    public Operation getBackupOperation() {
        return new AddAllBackupOperation(name, dataList);
    }

    public void writeInternal(DataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(dataList.size());
        for (Data data: dataList){
            IOUtil.writeNullableData(out, data);
        }
    }

    public void readInternal(DataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        dataList = new ArrayList<Data>(size);
        for (int i=0; i<size; i++){
            dataList.add(IOUtil.readNullableData(in));
        }
    }
}
