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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.monitor.impl.LocalQueueStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.*;

/**
 * User: ali
 * Date: 11/22/12
 * Time: 11:00 AM
 */
public class QueueContainer implements DataSerializable {

    private LinkedList<QueueItem> itemQueue = null;
    private HashMap<Long, QueueItem> itemMap = new HashMap<Long, QueueItem>(1000);
    private final LinkedHashMap<String, List<QueueItem>> txOfferMap = new LinkedHashMap<String, List<QueueItem>>();
    private final LinkedHashMap<String, List<QueueItem>> txPollMap = new LinkedHashMap<String, List<QueueItem>>();
    private final HashMap<Long, Data> dataMap = new HashMap<Long, Data>();

    private int partitionId;
    private QueueConfig config;
    private QueueStoreWrapper store;

    private long idGenerator = 0;

    private final QueueWaitNotifyKey pollWaitNotifyKey;
    private final QueueWaitNotifyKey offerWaitNotifyKey;

    private volatile long minAge;

    private volatile long maxAge;

    private volatile long totalAge;

    private volatile long totalAgedCount;

    public QueueContainer(String name) {
        pollWaitNotifyKey = new QueueWaitNotifyKey(name, "poll");
        offerWaitNotifyKey = new QueueWaitNotifyKey(name, "offer");
    }

    public QueueContainer(String name, int partitionId, QueueConfig config, SerializationService serializationService, boolean fromBackup) throws Exception {
        this(name);
        this.partitionId = partitionId;
        setConfig(config, serializationService);
        if (!fromBackup && store.isEnabled()) {
            Set<Long> keys = store.loadAllKeys();
            if (keys != null) {
                for (Long key : keys) {
                    QueueItem item = new QueueItem(this, key);
                    getItemQueue().offer(item);
                    nextId();
                }
            }
        }
    }

    //TX Methods

    public void commit(String txId) {
        List<QueueItem> list = txOfferMap.remove(txId);
        if (list != null) {
            for (QueueItem item : list) {
                item.setItemId(nextId());
                getItemQueue().offer(item);
            }
        }
        list = txPollMap.remove(txId);
        if (list != null) {
            long current = Clock.currentTimeMillis();
            for (QueueItem item : list) {
                age(item, current);
            }
        }
    }

    public void rollback(String txId) {
        List<QueueItem> list = txPollMap.remove(txId);
        if (list != null) {
            ListIterator<QueueItem> iter = list.listIterator(list.size());
            while (iter.hasPrevious()) {
                QueueItem item = iter.previous();
                if (item.getItemId() != -1) {//TODO
                    getItemQueue().offerFirst(item);
                }
            }
        }
        txOfferMap.remove(txId);
    }

    public long txOffer(String txId, Data data) {
        List<QueueItem> list = getTxList(txId, txOfferMap);
        long itemId = nextId();
        list.add(new QueueItem(this, nextId(), data));
        return itemId;
    }

    public QueueItem txPoll(String txId) {
        QueueItem item = getItemQueue().poll();
        if (item == null) {
            List<QueueItem> list = getTxList(txId, txOfferMap);
            if (list.size() > 0) {
                item = list.remove(0);
            }
        }
        if (item != null) {
            List<QueueItem> list = getTxList(txId, txPollMap);
            list.add(item);
            return item;
        }
        return null;
    }

    public Data txPeek(String txId) {
        QueueItem item = getItemQueue().peek();
        if (item == null) {
            List<QueueItem> list = getTxList(txId, txOfferMap);
            if (list.size() > 0) {
                item = list.get(0);
            }
        }
        return item == null ? null : item.getData();
    }

    private List<QueueItem> getTxList(String txId, Map<String, List<QueueItem>> map) {
        List<QueueItem> list = map.get(txId);
        if (list == null) {
            list = new ArrayList<QueueItem>(3);
            map.put(txId, list);
        }
        return list;
    }


    public long offer(Data data) {
        QueueItem item = new QueueItem(this, nextId());
        if (store.isEnabled()) {
            try {
                store.store(item.getItemId(), data);
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
        if (!store.isEnabled() || store.getMemoryLimit() > getItemQueue().size()) {
            item.setData(data);
        }
        getItemQueue().offer(item);
        QueueItem replaced = itemMap.put(item.getItemId(), item);
        if (replaced != null) {
            System.err.println("something wrong!!!");
        }
        return item.getItemId();
    }

    public void offerBackup(Data data, long itemId) {
        QueueItem item = new QueueItem(this, itemId);
        if (!store.isEnabled() || store.getMemoryLimit() > getItemQueue().size()) {
            item.setData(data);
        }
        QueueItem replaced = itemMap.put(itemId, item);
        if (replaced != null) {
            System.err.println("something wrong!!!");
        }
    }

    public Map<Long, Data> addAll(Collection<Data> dataList) {
        Map<Long, Data> dataMap = new HashMap<Long, Data>(dataList.size());
        for (Data data : dataList) {
            QueueItem item = new QueueItem(this, nextId());
            if (!store.isEnabled() || store.getMemoryLimit() > getItemQueue().size()) {
                item.setData(data);
            }
            getItemQueue().offer(item);
            QueueItem replaced = itemMap.put(item.getItemId(), item);
            if (replaced != null) {
                System.err.println("something wrong!!!");
            }

            dataMap.put(item.getItemId(), data);
        }
        if (store.isEnabled()) {
            try {
                store.storeAll(dataMap);
            } catch (Exception e) {
                for (int i = 0; i < dataList.size(); i++) {
                    getItemQueue().poll();
                }
                throw new HazelcastException(e);
            }
        }
        return dataMap;
    }

    public void addAllBackup(Map<Long, Data> dataMap) {
        for (Map.Entry<Long, Data> entry : dataMap.entrySet()) {
            QueueItem item = new QueueItem(this, entry.getKey());
            if (!store.isEnabled() || store.getMemoryLimit() > getItemQueue().size()) {
                item.setData(entry.getValue());
            }
            itemMap.put(item.getItemId(), item);
        }
    }

    public QueueItem peek() {
        QueueItem item = getItemQueue().peek();
        if (item == null) {
            return null;
        }
        if (store.isEnabled() && item.getData() == null) {
            try {
                load(item);
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
        return item;
    }

    public QueueItem poll() {
        QueueItem item = peek();
        if (item == null) {
            return null;
        }
        if (store.isEnabled()) {
            try {
                store.delete(item.getItemId());
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
        getItemQueue().poll();
        QueueItem removed = itemMap.remove(item.getItemId());
        if (removed == null || removed.getItemId() != item.getItemId()) {
            System.err.println("something wrong!!!");
        }
        age(item, Clock.currentTimeMillis());
        return item;
    }

    public void pollBackup(long itemId) {
        QueueItem item = itemMap.remove(itemId);
        if (item != null) {
            age(item, Clock.currentTimeMillis());//For Stats
        } else {
            System.err.println("something wrong!!!");
        }
    }

    public Map<Long, Data> drain(int maxSize) {
        if (maxSize < 0 || maxSize > getItemQueue().size()) {
            maxSize = getItemQueue().size();
        }
        LinkedHashMap<Long, Data> map = new LinkedHashMap<Long, Data>(maxSize);
        Iterator<QueueItem> iter = getItemQueue().iterator();
        for (int i = 0; i < maxSize; i++) {
            QueueItem item = iter.next();
            if (store.isEnabled() && item.getData() == null) {
                try {
                    load(item);
                } catch (Exception e) {
                    throw new HazelcastException(e);
                }
            }
            map.put(item.getItemId(), item.getData());
        }
        if (store.isEnabled()) {
            try {
                store.deleteAll(map.keySet());
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
        long current = Clock.currentTimeMillis();
        for (int i = 0; i < maxSize; i++) {
            QueueItem item = getItemQueue().poll();
            QueueItem removed = itemMap.remove(item.getItemId());
            if (removed == null || removed.getItemId() != item.getItemId()) {
                System.err.println("something wrong!!!");
            }
            age(item, current); //For Stats
        }
        return map;
    }

    public void drainFromBackup(Set<Long> itemIdSet) {
        for (Long itemId : itemIdSet) {
            pollBackup(itemId);
        }
        dataMap.clear();
    }

    public int size() {
        return getItemQueue().size(); //TODO check max size
    }

    public Map<Long, Data> clear() {
        if (store.isEnabled()) {
            try {
                store.deleteAll(itemMap.keySet());
            } catch (Exception e) {
                throw new HazelcastException(e);
            }
        }
        long current = Clock.currentTimeMillis();
        LinkedHashMap<Long, Data> map = new LinkedHashMap<Long, Data>(itemMap.size());
        for (QueueItem item : itemMap.values()) {
            dataMap.put(item.getItemId(), item.getData());
            age(item, current); // For stats
        }
        getItemQueue().clear();
        itemMap.clear();
        dataMap.clear();
        return map;
    }

    public void clearBackup(Set<Long> itemIdSet) {
        drainFromBackup(itemIdSet);
    }

    /**
     * iterates all items, checks equality with data
     * This method does not trigger store load.
     */
    public long remove(Data data) {
        Iterator<QueueItem> iter = getItemQueue().iterator();
        while (iter.hasNext()) {
            QueueItem item = iter.next();
            if (data.equals(item.getData())) {
                if (store.isEnabled()) {
                    try {
                        store.delete(item.getItemId());
                    } catch (Exception e) {
                        throw new HazelcastException(e);
                    }
                }
                iter.remove();
                itemMap.remove(item.getItemId());
                age(item, Clock.currentTimeMillis()); //For Stats
                return item.getItemId();
            }
        }
        return -1;
    }

    public void removeBackup(long itemId) {
        itemMap.remove(itemId);
    }

    /**
     * This method does not trigger store load.
     */
    public boolean contains(Collection<Data> dataSet) {
        Set<QueueItem> set = new HashSet<QueueItem>(dataSet.size());
        for (Data data : dataSet) {
            set.add(new QueueItem(this, -1, data));
        }
        return getItemQueue().containsAll(set);
    }

    /**
     * This method triggers store load.
     */
    public List<Data> getAsDataList() {
        List<Data> dataList = new ArrayList<Data>(getItemQueue().size());
        for (QueueItem item : getItemQueue()) {
            if (store.isEnabled() && item.getData() == null) {
                try {
                    load(item);
                } catch (Exception e) {
                    throw new HazelcastException(e);
                }
            }
            dataList.add(item.getData());
        }
        return dataList;
    }

    /**
     * This method triggers store load
     */
    public Map<Long, Data> compareAndRemove(Collection<Data> dataList, boolean retain) {
        LinkedHashMap<Long, Data> map = new LinkedHashMap<Long, Data>();
        for (QueueItem item : getItemQueue()) {
            if (item.getData() == null && store.isEnabled()) {
                try {
                    load(item);
                } catch (Exception e) {
                    throw new HazelcastException(e);
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
                    throw new HazelcastException(e);
                }
            }
            Iterator<QueueItem> iter = getItemQueue().iterator();
            long current = Clock.currentTimeMillis();
            while (iter.hasNext()) {
                QueueItem item = iter.next();
                if (map.containsKey(item.getItemId())) {
                    iter.remove();
                    pollBackup(item.getItemId());
                }
            }
        }
        return map;
    }

    public void compareAndRemoveBackup(Set<Long> itemIdSet) {
        drainFromBackup(itemIdSet);
    }

    private void load(QueueItem item) throws Exception {
        int bulkLoad = store.getBulkLoad();
        bulkLoad = Math.min(getItemQueue().size(), bulkLoad);
        if (bulkLoad == 1) {
            item.setData(store.load(item.getItemId()));
        } else if (bulkLoad > 1) {
            ListIterator<QueueItem> iter = getItemQueue().listIterator();
            HashSet<Long> keySet = new HashSet<Long>(bulkLoad);
            for (int i = 0; i < bulkLoad; i++) {
                keySet.add(iter.next().getItemId());
            }
            Map<Long, Data> values = store.loadAll(keySet);
            dataMap.putAll(values);
            item.setData(getDataFromMap(item.getItemId()));
        }
    }

    public boolean hasEnoughCapacity() {
        return hasEnoughCapacity(1);
    }

    public boolean hasEnoughCapacity(int delta) {
        return (getItemQueue().size() + delta) <= config.getMaxSize();
    }

    LinkedList<QueueItem> getItemQueue() {
        if (itemQueue == null) {
            itemQueue = new LinkedList<QueueItem>();
            if (!itemMap.isEmpty()) {
                List<QueueItem> values = new ArrayList<QueueItem>(itemMap.values());
                Collections.sort(values);
                itemQueue.addAll(values);
            }
        }
        return itemQueue;
    }

    public Data getDataFromMap(long itemId) {
        return dataMap.remove(itemId);
    }

    public void setConfig(QueueConfig config, SerializationService serializationService) {
        store = new QueueStoreWrapper(serializationService);
        this.config = new QueueConfig(config);
        QueueStoreConfig storeConfig = config.getQueueStoreConfig();
        store.setConfig(storeConfig);
    }

    long nextId() {
        return idGenerator++;
    }

    void setId(long itemId) {
        idGenerator = Math.max(itemId, idGenerator);
    }

    public QueueWaitNotifyKey getPollWaitNotifyKey() {
        return pollWaitNotifyKey;
    }

    public QueueWaitNotifyKey getOfferWaitNotifyKey() {
        return offerWaitNotifyKey;
    }

    public QueueConfig getConfig() {
        return config;
    }

    public int getPartitionId() {
        return partitionId;
    }

    private void age(QueueItem item, long currentTime) {
        long elapsed = currentTime - item.getCreationTime();
        if (elapsed <= 0) {
            return;//elapsed time can not be a negative value, a system clock problem maybe. ignored
        }
        totalAgedCount++;
        totalAge += elapsed;
        if (minAge == 0) {
            minAge = elapsed;
            maxAge = elapsed;
        } else {
            minAge = Math.min(minAge, elapsed);
            maxAge = Math.max(maxAge, elapsed);
        }
    }

    public void setStats(LocalQueueStatsImpl stats) {
        stats.setMinAge(minAge);
        minAge = 0;
        stats.setMaxAge(maxAge);
        maxAge = 0;
        long totalAgeVal = totalAge;
        totalAge = 0;
        long totalAgedCountVal = totalAgedCount;
        totalAgedCount = 0;
        totalAgedCountVal = totalAgedCountVal == 0 ? 1 : totalAgedCountVal;
        stats.setAveAge(totalAgeVal / totalAgedCountVal);
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(partitionId);
        out.writeInt(getItemQueue().size());
        for (QueueItem item : getItemQueue()) {
            item.writeData(out);
        }
        out.writeInt(txOfferMap.size());
        for (Map.Entry<String, List<QueueItem>> entry : txOfferMap.entrySet()) {
            String txId = entry.getKey();
            out.writeUTF(txId);
            List<QueueItem> list = entry.getValue();
            out.writeInt(list.size());
            for (QueueItem item : list) {
                item.writeData(out);
            }
        }
        out.writeInt(txPollMap.size());
        for (Map.Entry<String, List<QueueItem>> entry : txPollMap.entrySet()) {
            String txId = entry.getKey();
            out.writeUTF(txId);
            List<QueueItem> list = entry.getValue();
            out.writeInt(list.size());
            for (QueueItem item : list) {
                item.writeData(out);
            }
        }

    }

    public void readData(ObjectDataInput in) throws IOException {
        partitionId = in.readInt();
        int size = in.readInt();
        for (int j = 0; j < size; j++) {
            QueueItem item = new QueueItem(this);
            item.readData(in);
            getItemQueue().offer(item);
            setId(item.getItemId());
        }
        int offerMapSize = in.readInt();
        for (int i = 0; i < offerMapSize; i++) {
            String txId = in.readUTF();
            int listSize = in.readInt();
            List<QueueItem> list = new ArrayList<QueueItem>(listSize);
            for (int j = 0; j < listSize; j++) {
                QueueItem item = new QueueItem(this);
                item.readData(in);
                list.add(item);
            }
            txOfferMap.put(txId, list);
        }
        int pollMapSize = in.readInt();
        for (int i = 0; i < pollMapSize; i++) {
            String txId = in.readUTF();
            int listSize = in.readInt();
            List<QueueItem> list = new ArrayList<QueueItem>(listSize);
            for (int j = 0; j < listSize; j++) {
                QueueItem item = new QueueItem(this);
                item.readData(in);
                list.add(item);
            }
            txPollMap.put(txId, list);
        }
    }
}