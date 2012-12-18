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

import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * User: ali
 * Date: 11/22/12
 * Time: 11:00 AM
 */
public class QueueContainer implements DataSerializable {

    private final Queue<QueueItem> itemQueue = new LinkedList<QueueItem>();

    int partitionId;

    private QueueConfig config;

    String name;

    QueueService queueService;

    final QueueStoreWrapper store = new QueueStoreWrapper();

    long idGen = 0;

    boolean fromBackup;

    public QueueContainer() {
    }

    public QueueContainer(QueueService queueService, int partitionId, QueueConfig config, String name) {
        this.queueService = queueService;
        this.partitionId = partitionId;
        this.name = name;
        setConfig(config);
        Set<Long> keys = store.loadAllKeys(fromBackup);
        for (Long key: keys){
            QueueItem item = new QueueItem(key);
            itemQueue.offer(item);
            idGen++;
        }
    }

    public boolean offer(Data data) {
        QueueItem item = new QueueItem(idGen++, data);
        store.store(item.getItemId(), data, fromBackup);
        return itemQueue.offer(item);
    }

    public int size() {
        return itemQueue.size();
    }

    public void clear() {
        itemQueue.clear(); //TODO how about remove event and store
    }

    public Data poll() {
        QueueItem item = itemQueue.poll();
        if (item == null) {
            return null;
        }
        Data data = item.getData();
        if (data == null) {
            data = store.load(item.getItemId(), fromBackup);
        }
        store.delete(item.getItemId(), fromBackup);
        return data;
    }

    public boolean remove(Data data) {
        Iterator<QueueItem> iter = itemQueue.iterator();
        while (iter.hasNext()){
            QueueItem item = iter.next();
            if (item.equals(data)){
                iter.remove();
                store.delete(item.getItemId(), fromBackup);
                return true;
            }
        }
        return false;
    }

    public Data peek() {
        QueueItem item = itemQueue.peek();
        if (item == null) {
            return null;
        }
        Data data = item.getData();
        if (data == null) {
            data = store.load(item.getItemId(), fromBackup);
            item.setData(data);
        }
        return data;
    }

    //TODO how about persisted data
    public boolean contains(Set<Data> dataSet) {
        return itemQueue.containsAll(dataSet);
    }

    public List<Data> getAsDataList(){
        List<Data> dataSet = new ArrayList<Data>(itemQueue.size());
        for (QueueItem item : itemQueue) {
            Data data = item.getData();
            if (data == null){
                data = store.load(item.getItemId(), fromBackup);
                item.setData(data);
            }
            dataSet.add(data);
        };
        return dataSet;
    }






    public QueueConfig getConfig() {
        return config;
    }

    public void setConfig(QueueConfig config) {
        this.config = new QueueConfig(config);
        QueueStoreConfig storeConfig = config.getQueueStoreConfig();
        store.setConfig(storeConfig);
    }

    public void setFromBackup(boolean fromBackup) {
        this.fromBackup = fromBackup;
    }

    public void writeData(DataOutput out) throws IOException {    //TODO listeners  and persisted Data
        out.writeInt(partitionId);
        out.writeUTF(name);
        out.writeInt(itemQueue.size());
        Iterator<QueueItem> iterator = itemQueue.iterator();
        while (iterator.hasNext()) {
            QueueItem item = iterator.next();
            item.writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        partitionId = in.readInt();
        name = in.readUTF();
        int size = in.readInt();
        for (int j = 0; j < size; j++) {
            QueueItem item = new QueueItem();
            item.readData(in);
            itemQueue.offer(item);
        }
    }

}