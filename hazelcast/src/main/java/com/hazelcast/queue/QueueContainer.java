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

    private int partitionId;

    private QueueConfig config;

    private String name;

    private QueueService queueService;

    private final QueueStoreWrapper store = new QueueStoreWrapper();

    private long idGen = 0;

    public QueueContainer(String name) {
        this.name = name;
    }

    public QueueContainer(QueueService queueService, int partitionId, QueueConfig config, String name, boolean fromBackup) {
        this(name);
        this.queueService = queueService;
        this.partitionId = partitionId;
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
        itemQueue.clear();
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

    /**
     * iterates all items, checks equality with data
     * This method does not trigger store load.
     *
     * @param data
     * @param fromBackup
     * @return
     */
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

    /**
     * This method does not trigger store load.
     * @param dataSet
     * @return
     */
    public boolean contains(Set<Data> dataSet) {
        Set<QueueItem> set = new HashSet<QueueItem>(dataSet.size());
        for (Data data : dataSet) {
            set.add(new QueueItem(-1, data));
        }
        return itemQueue.containsAll(set);
    }

    /**
     * This method triggers store load.
     *
     * @return
     */
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

    public boolean addAll(Set<Data> dataSet, boolean fromBackup){
        boolean modified = false;
        for (Data data: dataSet){
            modified |= offer(data, fromBackup);
        }
        return modified;
    }

    /**
     * This method triggers store load
     *
     * @param dataSet
     * @param retain
     * @return
     */
    public Map<Long, Data> compareCollection(Set<Data> dataSet, boolean retain){
        Iterator<QueueItem> iter = itemQueue.iterator();
        Map<Long, Data> keySet = new HashMap<Long, Data>();
        while (iter.hasNext()){
            QueueItem item = iter.next();
            Data data = item.getData();
            if (data == null && store.isEnabled()){
                data = store.load(item.getItemId());
                item.setData(data);
            }
            boolean contains = dataSet.contains(data);
            if ((retain && !contains) || (!retain && contains)){
                keySet.put(item.getItemId(), data);
                iter.remove();
            }
        }
        return keySet;
    }

    public void compareCollectionBackup(Set<Long> keySet){
        Iterator<QueueItem> iter = itemQueue.iterator();
        while (iter.hasNext()){
            QueueItem item = iter.next();
            if (keySet.contains(item.getItemId())){
                iter.remove();
            }
        }
    }

    public int getPartitionId() {
        return partitionId;
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
        out.writeInt(itemQueue.size());
        Iterator<QueueItem> iterator = itemQueue.iterator();
        while (iterator.hasNext()) {
            QueueItem item = iterator.next();
            item.writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        partitionId = in.readInt();
        int size = in.readInt();
        for (int j = 0; j < size; j++) {
            QueueItem item = new QueueItem();
            item.readData(in);
            itemQueue.offer(item);
            idGen++;
        }
    }

}