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

package com.hazelcast.map;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.util.*;
import com.hazelcast.util.AbstractMap.SimpleImmutableEntry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultRecordStore implements RecordStore {
    final String name;
    final PartitionInfo partitionInfo;
    final PartitionContainer partitionContainer;
    final ConcurrentMap<Data, Record> records = new ConcurrentHashMap<Data, Record>(1000);
    final ConcurrentMap<Data, LockInfo> locks = new ConcurrentHashMap<Data, LockInfo>(100);
    final ConcurrentHashSet<Data> removedDelayedKeys = new ConcurrentHashSet<Data>();
    final MapInfo mapInfo;

    final MapService mapService;

    public DefaultRecordStore(String name, PartitionContainer partitionContainer) {
        this.name = name;
        this.partitionInfo = partitionContainer.partitionInfo;
        this.partitionContainer = partitionContainer;
        mapService = partitionContainer.getMapService();
        this.mapInfo = mapService.getMapInfo(name);
    }

    public LockInfo getOrCreateLock(Data key) {
        LockInfo lock = locks.get(key);
        if (lock == null) {
            lock = new LockInfo();
            locks.put(key, lock);
        }
        return lock;
    }

    public LockInfo getLock(Data key) {
        return locks.get(key);
    }

    void removeLock(Data key) {
        locks.remove(key);
    }

    public MapInfo getMapInfo() {
        return mapInfo;
    }

    public boolean canRun(LockAwareOperation op) {
        LockInfo lock = locks.get(op.getKey());
        return lock == null || lock.testLock(op.getThreadId(), op.getCaller());
    }

    public ConcurrentMap<Data, Record> getRecords() {
        return records;
    }

    public void evictAll(List<Data> keys) {
        for (Data key : keys) {
            evict(key);
        }
    }

    void clear() {
        records.clear();
        locks.clear();
    }

    public boolean lock(Data dataKey, Address caller, int threadId, long ttl) {
        LockInfo lock = getOrCreateLock(dataKey);
        return lock.lock(caller, threadId, ttl);
    }

    public int size() {
        return records.size();
    }

    public boolean containsValue(Data dataValue) {
        for (Record record : records.values()) {
            Object value = toObject(dataValue);
            Object recordValue = record.getValue() == null ? toObject(record.getValueData()) : record.getValue();
            if (recordValue.equals(value))
                return true;
        }
        return false;
    }

    public boolean isLocked(Data dataKey) {
        LockInfo lock = getLock(dataKey);
        if (lock == null)
            return false;
        return lock.isLocked();
    }

    public boolean unlock(Data dataKey, Address caller, int threadId) {
        LockInfo lock = getLock(dataKey);
        if (lock == null)
            return false;
        if (lock.testLock(threadId, caller)) {
            if (lock.unlock(caller, threadId)) {
                return true;
            }
        }
        return false;
    }


    public boolean forceUnlock(Data dataKey) {
        LockInfo lock = getLock(dataKey);
        if (lock == null)
            return false;
        lock.clear();
        return true;
    }

    public boolean tryRemove(Data dataKey) {
        Record record = records.get(dataKey);
        Data oldValueData = null;
        boolean removed = false;
        if (record == null) {
            if (mapInfo.getStore() != null) {
                Object oldValue = mapInfo.getStore().load(toObject(dataKey));
                oldValueData = toData(oldValue);
            }
        } else {
            oldValueData = record.getValueData();
            records.remove(dataKey);
            removed = true;
        }
        if (oldValueData != null && mapInfo.getStore() != null && mapInfo.getWriteDelayMillis() == 0) {
            Object key = record.getKey();
            mapInfo.getStore().delete(key);
            mapStoreDelete(record);
            removed = true;
        }
        return removed;
    }

    public Set<Map.Entry<Data, Data>> entrySet() {
        Map<Data,Data> temp = new HashMap<Data,Data>(records.size());
        for (Data key : records.keySet()) {
            temp.put(key, records.get(key).getValueData());
        }
        return temp.entrySet();
    }

    public Map.Entry<Data, Data> getMapEntry(Data dataKey) {
        Record record = records.get(dataKey);
        return new SimpleImmutableEntry<Data, Data>(dataKey, record.getValueData());
    }

    public Set<Data> keySet() {
        Set<Data> keySet = new HashSet<Data>(records.size());
        for (Data data : records.keySet()) {
            keySet.add(data);
        }
        return keySet;
    }

    public Collection<Data> values() {
        Collection<Data> values = new ArrayList<Data>(records.size());
        for (Record record : records.values()) {
            values.add(record.getValueData());
        }
        return values;
    }

    public Data remove(Data dataKey) {
        Record record = records.get(dataKey);
        Data oldValueData = null;
        if (record == null) {
            if (mapInfo.getStore() != null) {
                Object oldValue = mapInfo.getStore().load(toObject(dataKey));
                oldValueData = toData(oldValue);
            }
        } else {
            oldValueData = record.getValueData();
            records.remove(dataKey);
        }
        if (oldValueData != null) {
            mapStoreDelete(record);
        }
        return oldValueData;
    }

    private Object toObject(Data dataKey) {
        return mapService.getNodeEngine().toObject(dataKey);
    }

    private Data toData(Object oldValue) {
        return mapService.getNodeEngine().toData(oldValue);
    }


    public boolean evict(Data dataKey) {
        return records.remove(dataKey) != null;
    }

    public boolean remove(Data dataKey, Data testValue) {
        Record record = records.get(dataKey);
        Data oldValueData = null;
        boolean removed = false;
        if (record == null) {
            if (mapInfo.getStore() != null) {
                Object oldValue = mapInfo.getStore().load(toObject(dataKey));
                oldValueData = toData(oldValue);
            }
            if (oldValueData == null)
                return false;
        } else {
            oldValueData = record.getValueData();
        }

        if (testValue.equals(oldValueData)) {
            records.remove(dataKey);
            mapStoreDelete(record);
            removed = true;
        }
        return removed;
    }

    public Data get(Data dataKey) {
        Record record = records.get(dataKey);
        Data result = null;
        if (record == null) {
            if (mapInfo.getStore() != null) {
                Object key = toObject(dataKey);
                Object value = mapInfo.getStore().load(key);
                if (value != null) {
                    result = toData(value);
                    record = mapService.createRecord(name, dataKey, result, -1);
                    records.put(dataKey, record);
                }
            }
        } else {
            result = record.getValueData();
        }
//        check if record has expired or removed but waiting delay millis
        if (record != null && record.getState().isExpired()) {
            return null;
        }
        return result;
    }

    public void put(Map.Entry<Data, Data> entry) {
        Data dataKey = entry.getKey();
        Data dataValue = entry.getValue();
        Record record = records.get(dataKey);
        if(record == null){
            record = mapService.createRecord(name, dataKey, dataValue, -1);
            records.put(dataKey, record);
        }
        else
            record.setValueData(entry.getValue());
    }

    public Data put(Data dataKey, Data dataValue, long ttl) {
        Record record = records.get(dataKey);
        Data oldValueData = null;
        if (record == null) {
            if (mapInfo.getStore() != null) {
                Object oldValue = mapInfo.getStore().load(toObject(dataKey));
                oldValueData = toData(oldValue);
            }
            record = mapService.createRecord(name, dataKey, dataValue, ttl);
            int timeToLiveSeconds = mapInfo.getMapConfig().getTimeToLiveSeconds() * 1000;
            if (ttl <= 0 && timeToLiveSeconds > 0) {
                record.getState().updateTtlExpireTime(timeToLiveSeconds);
                mapService.scheduleOperation(name, dataKey, timeToLiveSeconds);
            }
            int maxIdleSeconds = mapInfo.getMapConfig().getMaxIdleSeconds();
            if (maxIdleSeconds > 0) {
                record.getState().updateIdleExpireTime(maxIdleSeconds);
                mapService.scheduleOperation(name, dataKey, maxIdleSeconds);
            }
            records.put(dataKey, record);
        } else {
            oldValueData = record.getValueData();
            record.setValueData(dataValue);
        }

        if (ttl > 0) {
            record.getState().updateTtlExpireTime(ttl);
            mapService.scheduleOperation(name, dataKey, ttl);
        }
        mapStoreWrite(record);
        return oldValueData;
    }


    private void mapStoreWrite(Record record) {
        if (mapInfo.getStore() != null) {
            long writeDelayMillis = mapInfo.getWriteDelayMillis();
            if (writeDelayMillis <= 0) {
                mapInfo.getStore().store(toObject(record.getKey()), record.getValue());
            } else {
                if (record.getState().getStoreTime() <= 0) {
                    record.getState().updateStoreTime(writeDelayMillis);
                    mapService.scheduleOperation(name, record.getKey(), writeDelayMillis);
                }
            }
        }
    }

    private void mapStoreDelete(Record record) {
        if (mapInfo.getStore() != null) {
            long writeDelayMillis = mapInfo.getWriteDelayMillis();
            if (writeDelayMillis == 0) {
                mapInfo.getStore().delete(toObject(record.getKey()));
            } else {
                if (record.getState().getStoreTime() <= 0) {
                    record.getState().updateStoreTime(writeDelayMillis);
                    mapService.scheduleOperation(name, record.getKey(), writeDelayMillis);
                    removedDelayedKeys.add(record.getKey());
                }
            }
        }
    }

    public ConcurrentHashSet<Data> getRemovedDelayedKeys() {
        return removedDelayedKeys;
    }

    public Data replace(Data dataKey, Data dataValue) {
        Record record = records.get(dataKey);
        Data oldValueData = null;
        if (record != null) {
            oldValueData = record.getValueData();
            record.setValueData(dataValue);
        } else {
            return null;
        }
        mapStoreWrite(record);
        return oldValueData;
    }

    public boolean replace(Data dataKey, Data oldValue, Data newValue) {
        Record record = records.get(dataKey);
        boolean replaced = false;
        if(record == null)
            return false;
        Object recordValue = record.getValue() == null ? toObject(record.getValueData()) : record.getValue();
        if (recordValue != null && recordValue.equals(toObject(oldValue))) {
            record.setValueData(newValue);
            replaced = true;
        } else {
            return false;
        }
        mapStoreWrite(record);
        return replaced;
    }

    public void set(Data dataKey, Data dataValue, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            record = mapService.createRecord(name, dataKey, dataValue, ttl);
            records.put(dataKey, record);
        } else {
            record.setValueData(dataValue);
        }

        if (ttl > 0) {
            record.getState().updateTtlExpireTime(ttl);
            mapService.scheduleOperation(name, dataKey, ttl);
        }

        mapStoreWrite(record);
    }

    public void putTransient(Data dataKey, Data dataValue, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            record = mapService.createRecord(name, dataKey, dataValue, ttl);
            records.put(dataKey, record);
        } else {
            record.setValueData(dataValue);
        }

        if (ttl > 0) {
            record.getState().updateTtlExpireTime(ttl);
            mapService.scheduleOperation(name, dataKey, ttl);
        }
    }

    public boolean tryPut(Data dataKey, Data dataValue, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            record = mapService.createRecord(name, dataKey, dataValue, ttl);
            records.put(dataKey, record);
        } else {
            record.setValueData(dataValue);
        }

        if (ttl > 0) {
            record.getState().updateTtlExpireTime(ttl);
            mapService.scheduleOperation(name, dataKey, ttl);
        }
        mapStoreWrite(record);
        return true;
    }

    public Data putIfAbsent(Data dataKey, Data dataValue, long ttl) {
        Record record = records.get(dataKey);
        Data oldValueData = null;
        boolean absent = true;
        if (record == null) {
            if (mapInfo.getStore() != null) {
                Object oldValue = mapInfo.getStore().load(toObject(dataKey));
                oldValueData = toData(oldValue);
                absent = oldValueData == null;
            }
            if (absent) {
                record = mapService.createRecord(name, dataKey, dataValue, ttl);
                records.put(dataKey, record);
                if (ttl > 0) {
                    record.getState().updateTtlExpireTime(ttl);
                    mapService.scheduleOperation(name, dataKey, ttl);
                }
                mapStoreWrite(record);
            }
        } else {
            oldValueData = record.getValueData();
        }
        return oldValueData;
    }
}
