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

    private final LinkedList<QueueItem> itemQueue = new LinkedList<QueueItem>();

    int partitionId;

    private QueueConfig config;

    String name;

    QueueService queueService;

    final QueueStoreWrapper store = new QueueStoreWrapper();

    long idGen = 0;

    public QueueContainer() {
    }

    public QueueContainer(QueueService queueService, int partitionId, QueueConfig config, String name, boolean fromBackup) {
        this.queueService = queueService;
        this.partitionId = partitionId;
        this.name = name;
        setConfig(config);
        if (!fromBackup && store.isEnabled()){
            Set<Long> keys = store.loadAllKeys();
            for (Long key : keys) {
                QueueItem item = new QueueItem(key);
                itemQueue.offer(item);
                idGen++;
            }
        }
    }

    public boolean offer(Data data, boolean fromBackup) {
        QueueItem item = new QueueItem(idGen++, data);
        if (!fromBackup && store.isEnabled()){
            store.store(item.getItemId(), data);
        }
        return itemQueue.offer(item);
    }

    public int size() {
        return itemQueue.size();
    }

    public void clear(boolean fromBackup) {
        if (!fromBackup && store.isEnabled()){
            Set<Long> keySet = new HashSet<Long>(itemQueue.size());
            Iterator<QueueItem> iter = itemQueue.iterator();
            while (iter.hasNext()) {
                QueueItem item = iter.next();
                keySet.add(item.getItemId());
            }
            store.deleteAll(keySet);
        }
        itemQueue.clear(); //TODO how about remove event
    }

    public Data poll(boolean fromBackup) {
        QueueItem item = itemQueue.poll();
        if (item == null) {
            return null;
        }
        Data data = item.getData();
        if (!fromBackup && store.isEnabled()){
            if (data == null) {
                data = store.load(item.getItemId());
            }
            store.delete(item.getItemId());
        }
        return data;
    }

    public boolean remove(Data data, boolean fromBackup) {
        Iterator<QueueItem> iter = itemQueue.iterator();
        while (iter.hasNext()) {
            QueueItem item = iter.next();
            if (item.equals(data)) {
                if (!fromBackup && store.isEnabled()){
                    store.delete(item.getItemId());
                }
                iter.remove();
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
        if (store.isEnabled() && data == null){
            data = store.load(item.getItemId());
            item.setData(data);
        }
        return data;
    }

    //TODO how about persisted data, should it trigger load all data from store?
    public boolean contains(Set<Data> dataSet) {
        Set<QueueItem> set = new HashSet<QueueItem>(dataSet.size());
        for (Data data : dataSet) {
            set.add(new QueueItem(-1, data));
        }
        return itemQueue.containsAll(set);
    }

    public List<Data> getAsDataList() {
        List<Data> dataSet = new ArrayList<Data>(itemQueue.size());
        for (QueueItem item : itemQueue) {
            Data data = item.getData();
            if (store.isEnabled() && data == null){
                data = store.load(item.getItemId());
                item.setData(data);
            }
            dataSet.add(data);
        }
        return dataSet;
    }

    public List<Data> drain(int maxSize){
        if (maxSize < 0 || maxSize > itemQueue.size()){
            maxSize = itemQueue.size();
        }
        ArrayList<Data> list = new ArrayList<Data>(maxSize);
        for (int i=0; i < maxSize; i++){
            Data data = poll(false);
            list.add(data);
        }
        return list;
    }

    public void drainFromBackup(int maxSize){
        if (maxSize < 0){
            itemQueue.clear();
            return;
        }
        for (int i=0; i<maxSize; i++){
            itemQueue.poll();
        }

    }


    public QueueConfig getConfig() {
        return config;
    }

    public void setConfig(QueueConfig config) {
        this.config = new QueueConfig(config);
        QueueStoreConfig storeConfig = config.getQueueStoreConfig();
        store.setConfig(storeConfig);
    }

    public void writeData(DataOutput out) throws IOException {    //TODO listeners
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