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

package com.hazelcast.map;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.PartitionInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultRecordStore implements RecordStore {
    final String name;
    final PartitionInfo partitionInfo;
    final PartitionContainer partitionContainer;
    final ConcurrentMap<Data, Record> records = new ConcurrentHashMap<Data, Record>(1000);
    final ConcurrentMap<Data, LockInfo> locks = new ConcurrentHashMap<Data, LockInfo>(100);
    // todo remove this.
    final Set<Data> removedDelayedKeys = Collections.newSetFromMap(new ConcurrentHashMap<Data, Boolean>());
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
            Object value = mapService.toObject(dataValue);
            Object recordValue = mapService.toObject(record.getValue());
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
        boolean removed = false;
        boolean evicted = false;
        if (record == null) {
            // already removed from map by eviction but still need to delete it
            if (mapInfo.getStore() != null && mapInfo.getStore().load(mapService.toObject(dataKey)) != null) {
                evicted = true;
            }
        } else {
            accessRecord(record);
            records.remove(dataKey);
            removed = true;
        }

        if ((removed || evicted) && mapInfo.getStore() != null) {
            mapStoreDelete(record);
        }
        return removed;
    }

    public Set<Map.Entry<Data, Data>> entrySet() {
        Map<Data, Data> temp = new HashMap<Data, Data>(records.size());
        for (Data key : records.keySet()) {
            temp.put(key, mapService.toData(records.get(key).getValue()));
        }
        return temp.entrySet();
    }

    public Map.Entry<Data, Data> getMapEntry(Data dataKey) {
        Record record = records.get(dataKey);
        return new AbstractMap.SimpleImmutableEntry<Data, Data>(dataKey, mapService.toData(record.getValue()) );
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
            values.add(mapService.toData(record.getValue()));
        }
        return values;
    }

    public Data remove(Data dataKey) {
        Record record = records.get(dataKey);
        Data oldValue = null;
        if (record == null) {
            if (mapInfo.getStore() != null) {
                oldValue = mapService.toData(mapInfo.getStore().load(mapService.toObject(dataKey)));
            }
        } else {
            oldValue = mapService.toData(record.getValue());
            records.remove(dataKey);
        }
        if (oldValue != null) {
            mapStoreDelete(record);
        }
        return oldValue;
    }

    public boolean evict(Data dataKey) {
        return records.remove(dataKey) != null;
    }

    public boolean remove(Data dataKey, Data testValue) {
        Record record = records.get(dataKey);
        Object oldValue = null;
        boolean removed = false;
        if (record == null) {
            if (mapInfo.getStore() != null) {
                oldValue = mapInfo.getStore().load(mapService.toObject(dataKey));
            }
            if (oldValue == null)
                return false;
        } else {
            oldValue = record.getValue();
        }

        if (mapService.toObject(testValue).equals(mapService.toObject(oldValue))) {
            records.remove(dataKey);
            removed = true;
            mapStoreDelete(record);
        }
        return removed;
    }

    public Data get(Data dataKey) {
        Record record = records.get(dataKey);
        Object value = null;
        if (record == null) {
            if (mapInfo.getStore() != null) {
                value = mapInfo.getStore().load(mapService.toObject(dataKey));
                if (value != null) {
                    record = mapService.createRecord(name, dataKey, value, -1);
                    records.put(dataKey, record);
                }
            }
        } else {
            accessRecord(record);
            value = record.getValue();
        }
//        check if record has expired or removed but waiting delay millis
        if (record != null && record.getState().isExpired()) {
            return null;
        }
        return mapService.toData(value);
    }

    public void put(Map.Entry<Data, Data> entry) {
        Data dataKey = entry.getKey();
        Data value = entry.getValue();
        Record record = records.get(dataKey);
        if (record == null) {
            record = mapService.createRecord(name, dataKey, value, -1);
            records.put(dataKey, record);
        } else {
            setRecordValue(record, entry.getValue());
        }
    }

    public Data put(Data dataKey, Data value, long ttl) {
        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record == null) {
            if (mapInfo.getStore() != null) {
                oldValue = mapInfo.getStore().load(mapService.toObject(dataKey));
            }
            record = mapService.createRecord(name, dataKey, value, ttl);
            records.put(dataKey, record);
        } else {
            oldValue = record.getValue();
            setRecordValue(record, value);
            updateTtl(record, ttl);
        }
        mapStoreWrite(record);
        return mapService.toData(oldValue);
    }

    public Set<Data> getRemovedDelayedKeys() {
        return removedDelayedKeys;
    }

    public Data replace(Data dataKey, Data value) {
        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record != null) {
            oldValue = record.getValue();
            setRecordValue(record, value);
        } else {
            return null;
        }
        mapStoreWrite(record);
        return mapService.toData(oldValue);
    }

    public boolean replace(Data dataKey, Data testValue, Data newValue) {
        Record record = records.get(dataKey);
        if (record == null)
            return false;
        Object recordValue = mapService.toObject(record.getValue());
        if (recordValue != null && recordValue.equals(mapService.toObject(testValue))) {
            setRecordValue(record, newValue);
        } else {
            return false;
        }
        mapStoreWrite(record);
        return true;
    }

    public void set(Data dataKey, Data value, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            record = mapService.createRecord(name, dataKey, value, ttl);
            records.put(dataKey, record);
        } else {
            setRecordValue(record, value);
            updateTtl(record, ttl);
        }
        mapStoreWrite(record);
    }

    public void putTransient(Data dataKey, Data value, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            record = mapService.createRecord(name, dataKey, value, ttl);
            records.put(dataKey, record);
        } else {
            setRecordValue(record, value);
            updateTtl(record, ttl);
        }
    }

    public boolean tryPut(Data dataKey, Data value, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            record = mapService.createRecord(name, dataKey, value, ttl);
            records.put(dataKey, record);
        } else {
            setRecordValue(record, value);
            updateTtl(record, ttl);
        }
        mapStoreWrite(record);
        return true;
    }

    public Data putIfAbsent(Data dataKey, Data value, long ttl) {
        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record == null) {
            if (mapInfo.getStore() != null) {
                oldValue = mapInfo.getStore().load(mapService.toObject(dataKey));
                if (oldValue != null) {
                    record = mapService.createRecord(name, dataKey, oldValue, -1);
                    records.put(dataKey, record);
                }
            }
        } else {
            accessRecord(record);
            oldValue = record.getValue();
        }

        if (oldValue == null) {
            record = mapService.createRecord(name, dataKey, value, ttl);
            records.put(dataKey, record);
            updateTtl(record, ttl);
            mapStoreWrite(record);
        }
        return mapService.toData(oldValue);
    }

    private void accessRecord(Record record) {
        record.access();
        int maxIdleSeconds = mapInfo.getMapConfig().getMaxIdleSeconds();
        if (maxIdleSeconds > 0) {
            record.getState().updateIdleExpireTime(maxIdleSeconds);
            mapService.scheduleOperation(name, record.getKey(), maxIdleSeconds);
        }
    }

    private void mapStoreWrite(Record record) {
        if (mapInfo.getStore() != null) {
            long writeDelayMillis = mapInfo.getWriteDelayMillis();
            if (writeDelayMillis <= 0) {
                mapInfo.getStore().store(mapService.toObject(record.getKey()), record.getValue());
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
                mapInfo.getStore().delete(mapService.toObject(record.getKey()));
            } else {
                if (record.getState().getStoreTime() <= 0) {
                    record.getState().updateStoreTime(writeDelayMillis);
                    mapService.scheduleOperation(name, record.getKey(), writeDelayMillis);
                    removedDelayedKeys.add(record.getKey());
                }
            }
        }
    }

    private void updateTtl(Record record, long ttl) {
        if (ttl > 0) {
            record.getState().updateTtlExpireTime(ttl);
            mapService.scheduleOperation(name, record.getKey(), ttl);
        }
    }

    public void setRecordValue(Record record, Object value) {
        accessRecord(record);
        if (record instanceof DataRecord)
            ((DataRecord) record).setValue(mapService.toData(value));
        else if (record instanceof ObjectRecord)
            ((ObjectRecord) record).setValue(mapService.toObject(value));
    }

  
}
