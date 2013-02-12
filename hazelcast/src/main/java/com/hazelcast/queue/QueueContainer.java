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

import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QueueStoreConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.exception.RetryableHazelcastException;

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

    private QueueStoreWrapper store;

    public QueueContainer() {
    }

    public QueueContainer(int partitionId, QueueConfig config, SerializationService serializationService, boolean fromBackup) throws Exception {
        this.partitionId = partitionId;
        store = new QueueStoreWrapper(serializationService);
        setConfig(config);
        if (!fromBackup && store.isEnabled()) {
            Set<Long> keys = store.loadAllKeys();
            if (keys != null) {
                for (Long key : keys) {
                    QueueItem item = new QueueItem(this, key);
                    itemQueue.offer(item);
                    idGen++;
                }
            }
        }
    }

    public QueueItem offer(Data data) {
        QueueItem item = new QueueItem(this, idGen++);
        if (store.isEnabled()) {
            try {
                store.store(item.getItemId(), data);
            } catch (Exception e) {
                idGen--;
                throw new RetryableHazelcastException(e);
            }
        }
        if (!store.isEnabled() || store.getMemoryLimit() > itemQueue.size()) {
            item.setData(data);
        }
        itemQueue.offer(item);
        return item;
    }

    public QueueItem offerBackup(Data data) {
        QueueItem item = new QueueItem(this, idGen++);
        if (!store.isEnabled() || store.getMemoryLimit() > itemQueue.size()) {
            item.setData(data);
        }
        itemQueue.offer(item);
        return item;
    }

    public int size() {
        return itemQueue.size();
    }

    public List<Data> clear() {
        Set<Long> keySet = new HashSet<Long>(itemQueue.size());
        List<Data> dataList = new ArrayList<Data>(itemQueue.size());
        for (QueueItem item : itemQueue) {
            keySet.add(item.getItemId());
            dataList.add(item.getData());
        }
        if (store.isEnabled()) {
            try {
                store.deleteAll(keySet);
            } catch (Exception e) {
                throw new RetryableHazelcastException(e);
            }
        }
        itemQueue.clear();
        dataMap.clear();
        return dataList;
    }

    public void clearBackup() {
        itemQueue.clear();
        dataMap.clear();
    }

    public Data poll() {
        QueueItem item = peek();
        if (item == null) {
            return null;
        }
        if (store.isEnabled()) {
            try {
                store.delete(item.getItemId());
            } catch (Exception e) {
                throw new RetryableHazelcastException(e);
            }
        }
        itemQueue.poll();
        return item.getData();
    }

    public void pollBackup() {
        itemQueue.poll();
    }

    /**
     * iterates all items, checks equality with data
     * This method does not trigger store load.
     */
    public long remove(Data data) {
        Iterator<QueueItem> iter = itemQueue.iterator();
        while (iter.hasNext()) {
            QueueItem item = iter.next();
            if (data.equals(item.getData())) {
                if (store.isEnabled()) {
                    try {
                        store.delete(item.getItemId());
                    } catch (Exception e) {
                        throw new RetryableHazelcastException(e);
                    }
                }
                iter.remove();
                return item.getItemId();
            }
        }
        return -1;
    }

    public void removeBackup(long itemId) {
        Iterator<QueueItem> iter = itemQueue.iterator();
        while (iter.hasNext()) {
            QueueItem item = iter.next();
            if (item.getItemId() == itemId) {
                iter.remove();
                return;
            }
        }
    }

    public QueueItem peek() {
        QueueItem item = itemQueue.peek();
        if (item == null) {
            return null;
        }
        if (store.isEnabled() && item.getData() == null) {
            try {
                load(item);
            } catch (Exception e) {
                throw new RetryableHazelcastException(e);
            }
        }
        return item;
    }

    /**
     * This method does not trigger store load.
     */
    public boolean contains(Collection<Data> dataSet) {
        Set<QueueItem> set = new HashSet<QueueItem>(dataSet.size());
        for (Data data : dataSet) {
            set.add(new QueueItem(this, -1, data));
        }
        return itemQueue.containsAll(set);
    }

    /**
     * This method triggers store load.
     */
    public List<Data> getAsDataList() {
        List<Data> dataList = new ArrayList<Data>(itemQueue.size());
        for (QueueItem item : itemQueue) {
            if (store.isEnabled() && item.getData() == null) {
                try {
                    load(item);
                } catch (Exception e) {
                    throw new RetryableHazelcastException(e);
                }
            }
            dataList.add(item.getData());
        }
        return dataList;
    }

    public Collection<Data> drain(int maxSize) {
        if (maxSize < 0 || maxSize > itemQueue.size()) {
            maxSize = itemQueue.size();
        }
        LinkedHashMap<Long, Data> map = new LinkedHashMap<Long, Data>(maxSize);
        Iterator<QueueItem> iter = itemQueue.iterator();
        for (int i = 0; i < maxSize; i++) {
            QueueItem item = iter.next();
            if (store.isEnabled() && item.getData() == null) {
                try {
                    load(item);
                } catch (Exception e) {
                    throw new RetryableHazelcastException(e);
                }
            }
            map.put(item.getItemId(), item.getData());
        }
        if (store.isEnabled()) {
            try {
                store.deleteAll(map.keySet());
            } catch (Exception e) {
                throw new RetryableHazelcastException(e);
            }
        }
        for (int i = 0; i < maxSize; i++) {
            itemQueue.poll();
        }
        return map.values();
    }

    public void drainFromBackup(int maxSize) {
        if (maxSize < 0) {
            clearBackup();
            return;
        }
        if (maxSize > itemQueue.size()) {
            maxSize = itemQueue.size();
        }
        for (int i = 0; i < maxSize; i++) {
            pollBackup();
        }

    }

    public void addAll(Collection<Data> dataList) {
        Map<Long, Data> dataMap = new HashMap<Long, Data>(dataList.size());
        for (Data data : dataList) {
            QueueItem item = offerBackup(data);
            dataMap.put(item.getItemId(), data);
        }
        if (store.isEnabled()) {
            try {
                store.storeAll(dataMap);
            } catch (Exception e) {
                for (int i = 0; i < dataList.size(); i++) {
                    itemQueue.poll();
                    idGen--;
                }
                throw new RetryableHazelcastException(e);
            }
        }
    }

    public void addAllBackup(Collection<Data> dataList) {
        for (Data data : dataList) {
            offerBackup(data);
        }
    }

    /**
     * This method triggers store load
     */
    public Map<Long, Data> compareAndRemove(Collection<Data> dataList, boolean retain) {
        LinkedHashMap<Long, Data> map = new LinkedHashMap<Long, Data>();
        for (QueueItem item : itemQueue) {
            if (item.getData() == null && store.isEnabled()) {
                try {
                    load(item);
                } catch (Exception e) {
                    throw new RetryableHazelcastException(e);
                }
            }
            boolean contains = dataList.contains(item.getData());
            if ((retain && !contains) || (!retain && contains)) {
                map.put(item.getItemId(), item.getData());
            }
        }
        if (map.size() > 0) {
            if (store.isEnabled()) {
                try {
                    store.deleteAll(map.keySet());
                } catch (Exception e) {
                    throw new RetryableHazelcastException(e);
                }
            }
            Iterator<QueueItem> iter = itemQueue.iterator();
            while (iter.hasNext()) {
                QueueItem item = iter.next();
                if (map.containsKey(item.getItemId())) {
                    iter.remove();
                }
            }
        }
        return map;
    }

    public void compareAndRemoveBackup(Set<Long> keySet) {
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
            Map<Long, Data> values = store.loadAll(keySet);
            if (values == null) {
                // TODO: @mm - can loadAll return null map?
                return;
            }
            dataMap.putAll(values);
            item.setData(getDataFromMap(item.getItemId()));
        }
    }

    public boolean checkBound() {
        return checkBound(1);
    }

    public boolean checkBound(int delta) {
        if (itemQueue.size() + delta > config.getMaxSize()) {
            return false;
        }
        return true;
    }

    public Data getDataFromMap(long itemId) {
        return dataMap.remove(itemId);
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(partitionId);
        out.writeInt(itemQueue.size());
        for (QueueItem item : itemQueue) {
            item.writeData(out);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        partitionId = in.readInt();
        int size = in.readInt();
        for (int j = 0; j < size; j++) {
            QueueItem item = new QueueItem(this);
            item.readData(in);
            itemQueue.offer(item);
            idGen++;
        }
    }

}