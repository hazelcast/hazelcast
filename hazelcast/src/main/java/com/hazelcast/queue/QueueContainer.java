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

    private final HashMap<Long, Data> dataMap = new HashMap<Long, Data>();

    private int partitionId;

    private QueueConfig config;

    private long idGen = 0;

    private final QueueStoreWrapper store = new QueueStoreWrapper();

    public QueueContainer() {
    }

    public QueueContainer(int partitionId, QueueConfig config, boolean fromBackup) throws Exception {
        this.partitionId = partitionId;
        setConfig(config);
        if (!fromBackup && store.isEnabled()) {
            Set<Long> keys = store.loadAllKeys();
            for (Long key : keys) {
                QueueItem item = new QueueItem(this, key);
                itemQueue.offer(item);
                idGen++;
            }
        }
    }

    public QueueItem offer(Data data) {
        QueueItem item = new QueueItem(this, idGen++);
        if (!store.isEnabled() || store.getMemoryLimit() > itemQueue.size()){
            item.setData(data);
        }
        if (itemQueue.offer(item)) {
            return item;
        }
        return null;
    }

    public int size() {
        return itemQueue.size();
    }

    public void clear() {
        itemQueue.clear();
        dataMap.clear();
    }

    public QueueItem poll() throws Exception {
        QueueItem item = itemQueue.peek();
        if (item == null){
            return null;
        }
        if (store.isEnabled() && item.getData() == null) {
            load(item);
        }
        itemQueue.poll();
        return item;
    }

    public void pollBackup() {
        itemQueue.poll();
    }

    /**
     * iterates all items, checks equality with data
     * This method does not trigger store load.
     *
     */
    public long remove(Data data) {
        Iterator<QueueItem> iter = itemQueue.iterator();
        while (iter.hasNext()) {
            QueueItem item = iter.next();
            if (data.equals(item.getData())) {
                iter.remove();
                return item.getItemId();
            }
        }
        return -1;
    }

    public QueueItem peek() throws Exception {
        QueueItem item = itemQueue.peek();
        if (item == null) {
            return null;
        }
        if (store.isEnabled() && item.getData() == null) {
            load(item);
        }
        return item;
    }

    /**
     * This method does not trigger store load.
     */
    public boolean contains(List<Data> dataSet) {
        Set<QueueItem> set = new HashSet<QueueItem>(dataSet.size());
        for (Data data : dataSet) {
            set.add(new QueueItem(this, -1, data));
        }
        return itemQueue.containsAll(set);
    }

    /**
     * This method triggers store load.
     *
     */
    public List<QueueItem> itemList() throws Exception {
        List<QueueItem> itemList = new ArrayList<QueueItem>(itemQueue.size());
        for (QueueItem item : itemQueue) {
            if (store.isEnabled() && item.getData() == null) {
                load(item);
            }
            itemList.add(item);
        }
        return itemList;
    }

    public List<QueueItem> drain(int maxSize) throws Exception {
        if (maxSize < 0 || maxSize > itemQueue.size()) {
            maxSize = itemQueue.size();
        }
        List<QueueItem> itemList = new ArrayList<QueueItem>(maxSize);
        for (int i = 0; i < maxSize; i++) {
            QueueItem item = poll();
            itemList.add(item);
        }
        return itemList;
    }

    public void drainFromBackup(int maxSize) {
        if (maxSize < 0) {
            itemQueue.clear();
            return;
        }
        for (int i = 0; i < maxSize; i++) {
            pollBackup();
        }

    }

    public List<QueueItem> addAll(List<Data> dataSet) {
        List<QueueItem> itemSet = new ArrayList<QueueItem>(dataSet.size());
        for (Data data : dataSet) {
            QueueItem item = offer(data);
            if (item != null) {
                itemSet.add(item);
            }
        }
        return itemSet;
    }

    /**
     * This method triggers store load
     *
     */
    public Map<Long, Data> compareAndRemove(List<Data> dataList, boolean retain) throws Exception {
        Iterator<QueueItem> iter = itemQueue.iterator();
        LinkedHashMap<Long, Data> map = new LinkedHashMap<Long, Data>();
        while (iter.hasNext()) {
            QueueItem item = iter.next();
            if (item.getData() == null && store.isEnabled()) {
                try {
                    load(item);
                } catch (Exception e) {
                    int index = 0;
                    for (Map.Entry<Long, Data> entry: map.entrySet()){
                        itemQueue.add(index++, new QueueItem(this, entry.getKey(), entry.getValue()));
                    }
                    throw e;
                }
            }
            boolean contains = dataList.contains(item.getData());
            if ((retain && !contains) || (!retain && contains)) {
                map.put(item.getItemId(), item.getData());
                iter.remove();
            }
        }
        return map;
    }

    public void compareCollectionBackup(Set<Long> keySet) {
        Iterator<QueueItem> iter = itemQueue.iterator();
        while (iter.hasNext()) {
            QueueItem item = iter.next();
            if (keySet.contains(item.getItemId())) {
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

    public boolean isStoreAsync() {
        return store.isAsync();
    }

    public boolean isStoreEnabled() {
        return store.isEnabled();
    }

    public QueueStoreWrapper getStore() {
        return store;
    }

    public void setConfig(QueueConfig config) {
        this.config = new QueueConfig(config);
        QueueStoreConfig storeConfig = config.getQueueStoreConfig();
        store.setConfig(storeConfig);
    }

    private void load(QueueItem item) throws Exception {
        int bulkLoad = store.getBulkLoad();
        bulkLoad = Math.min(itemQueue.size(), bulkLoad);
        if (bulkLoad == 1) {
            item.setData(store.load(item.getItemId()));
        } else if (bulkLoad > 1) {
            ListIterator<QueueItem> iter = itemQueue.listIterator();
            HashSet<Long> keySet = new HashSet<Long>(bulkLoad);
            for (int i = 0; i < bulkLoad; i++) {
                keySet.add(iter.next().getItemId());
            }
            Map<Long, QueueStoreValue> values = store.loadAll(keySet);
            if (values == null){
                System.err.println("something wrong!");//TODO
                return;
            }
            for (Map.Entry<Long, QueueStoreValue> entry : values.entrySet()) {
                long id = entry.getKey();
                Data data = entry.getValue().getData();
                dataMap.put(id, data);
            }
            item.setData(getData(item.getItemId()));
        }
    }

    public Data getData(long itemId) {
        return dataMap.remove(itemId);
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(partitionId);
        out.writeInt(itemQueue.size());
        for (QueueItem item : itemQueue) {
            item.writeData(out);
        }
    }

    public void readData(DataInput in) throws IOException {
        partitionId = in.readInt();
        int size = in.readInt();
        for (int j = 0; j < size; j++) {
            QueueItem item = new QueueItem(this);
            item.readData(in);
            itemQueue.offer(item);
            idGen++;
        }
    }

    class KeyDataPair{

    }

}