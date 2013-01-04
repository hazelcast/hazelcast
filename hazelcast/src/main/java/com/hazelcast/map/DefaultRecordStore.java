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

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import com.hazelcast.impl.Record;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.PartitionInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultRecordStore implements RecordStore {
    final String name;
    final PartitionInfo partitionInfo;
    final PartitionContainer partitionContainer;
    final ConcurrentMap<Data, Record> records = new ConcurrentHashMap<Data, Record>(1000);
    final ConcurrentMap<Data, LockInfo> locks = new ConcurrentHashMap<Data, LockInfo>(100);
    final MapLoader loader;
    final MapStore store;
    final long writeDelayMillis;
    final int backupCount;
    final int asyncBackupCount;
    final MapService mapService;

    public DefaultRecordStore(String name, PartitionContainer partitionContainer) {
        this.name = name;
        this.partitionInfo = partitionContainer.partitionInfo;
        this.partitionContainer = partitionContainer;
        loader = null;
        store = null;
        MapConfig mapConfig = partitionContainer.getMapConfig(name);
        backupCount = mapConfig.getBackupCount();
        asyncBackupCount = mapConfig.getAsyncBackupCount();
        writeDelayMillis = mapConfig.getMapStoreConfig() == null ? 0 : mapConfig.getMapStoreConfig().getWriteDelaySeconds() * 1000;
        mapService = partitionContainer.getMapService();
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

    public int getBackupCount() {
        return backupCount;
    }

    public int getAsyncBackupCount() {
        return asyncBackupCount;
    }

    public int getTotalBackupCount() {
        return backupCount + asyncBackupCount;
    }

    public boolean canRun(LockAwareOperation op) {
        LockInfo lock = locks.get(op.getKey());
        return lock == null || lock.testLock(op.getThreadId(), op.getCaller());
    }

    public ConcurrentMap<Data, Record> getRecords() {
        return records;
    }

    public void removeAll(List<Data> keys) {
        for (Data key : keys) {
            remove(key);
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
            Object value = mapService.getNodeEngine().toObject(dataValue);
            if (record.getValue().equals(value))
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
            if (loader != null) {
                Object oldValue = loader.load(mapService.getNodeEngine().toObject(dataKey));
                oldValueData = mapService.getNodeEngine().toData(oldValue);
            }
        } else {
            oldValueData = record.getValueData();
            records.remove(dataKey);
            removed = true;
        }
        if (oldValueData != null && store != null && writeDelayMillis == 0) {
            Object key = record.getKey();
            store.delete(key);
            removed = true;
        }
        return removed;
    }

    public Set<Data> keySet() {
        return records.keySet();
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
            if (loader != null) {
                Object oldValue = loader.load(mapService.getNodeEngine().toObject(dataKey));
                oldValueData = mapService.getNodeEngine().toData(oldValue);
            }
        } else {
            oldValueData = record.getValueData();
            records.remove(dataKey);
        }
        if (oldValueData != null && store != null && writeDelayMillis == 0) {
            Object key = record.getKey();
            store.delete(key);
        }
        return oldValueData;
    }

    public boolean evict(Data dataKey) {
        return records.remove(dataKey) != null;
    }

    public boolean remove(Data dataKey, Data testValue) {
        Record record = records.get(dataKey);
        Data oldValueData = null;
        boolean removed = false;
        if (record == null) {
            if (loader != null) {
                Object oldValue = loader.load(mapService.getNodeEngine().toObject(dataKey));
                oldValueData = mapService.getNodeEngine().toData(oldValue);
            }
            if (oldValueData == null)
                return false;
        } else {
            oldValueData = record.getValueData();
        }

        if (testValue.equals(oldValueData)) {
            records.remove(dataKey);
            if (store != null && writeDelayMillis == 0) {
                Object key = record.getKey();
                store.delete(key);
            }
            removed = true;
        }
        return removed;
    }

    public Data get(Data dataKey) {
        Record record = records.get(dataKey);
        Data result = null;
        if (record == null) {
            if (loader != null) {
                Object key = mapService.getNodeEngine().toObject(dataKey);
                Object value = loader.load(key);
                if (value != null) {
                    result = mapService.getNodeEngine().toData(value);
                    record = mapService.createRecord(name, dataKey, result, -1);
                    records.put(dataKey, record);
                }
            }
        } else {
            result = record.getValueData();
        }
        // check ıf record has expired or removed but waiting delay millis
        if (record != null && !record.isActive()) {
            return null;
        }
        return result;
    }

    public Data put(Data dataKey, Data dataValue, long ttl) {
        Record record = records.get(dataKey);
        Data oldValueData = null;
        if (record == null) {
            if (loader != null) {
                Object oldValue = loader.load(mapService.getNodeEngine().toObject(dataKey));
                oldValueData = mapService.getNodeEngine().toData(oldValue);
            }
            record = mapService.createRecord(name, dataKey, dataValue, ttl);
            records.put(dataKey, record);
        } else {
            oldValueData = record.getValueData();
            record.setValueData(dataValue);
            record.setTtl(ttl);
        }
        record.setActive(true);
        if (ttl > 0)
            mapService.notifyMapTtl(name);
        if (store != null && writeDelayMillis == 0) {
            store.store(record.getKey(), record.getValue());
        }
        return oldValueData;
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
        record.setActive(true);
        if (store != null && writeDelayMillis == 0) {
            store.store(record.getKey(), record.getValue());
        }
        return oldValueData;
    }


    public boolean replace(Data dataKey, Data oldValue, Data newValue) {
        Record record = records.get(dataKey);
        boolean replaced = false;
        if (record != null && record.getValue().equals(mapService.getNodeEngine().toObject(oldValue))) {
            record.setValueData(newValue);
            replaced = true;
        } else {
            return false;
        }
        record.setActive(true);
        if (store != null && writeDelayMillis == 0) {
            store.store(record.getKey(), record.getValue());
        }
        return replaced;
    }


    public void set(Data dataKey, Data dataValue, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            record = mapService.createRecord(name, dataKey, dataValue, ttl);
            records.put(dataKey, record);
        } else {
            record.setValueData(dataValue);
            record.setTtl(ttl);
        }
        record.setActive(true);
        if (ttl > 0)
            mapService.notifyMapTtl(name);
        if (store != null && writeDelayMillis == 0) {
            store.store(record.getKey(), record.getValue());
        }
    }


    public void putTransient(Data dataKey, Data dataValue, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            record = mapService.createRecord(name, dataKey, dataValue, ttl);
            records.put(dataKey, record);
        } else {
            record.setValueData(dataValue);
            record.setTtl(ttl);
        }
        record.setActive(true);
        if (ttl > 0)
            mapService.notifyMapTtl(name);
    }

    public boolean tryPut(Data dataKey, Data dataValue, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            record = mapService.createRecord(name, dataKey, dataValue, ttl);
            records.put(dataKey, record);
        } else {
            record.setValueData(dataValue);
        }
        if (ttl > 0)
            mapService.notifyMapTtl(name);
        record.setActive(true);
        if (store != null && writeDelayMillis == 0) {
            store.store(record.getKey(), record.getValue());
        }
        return true;
    }

    public Data putIfAbsent(Data dataKey, Data dataValue, long ttl) {
        Record record = records.get(dataKey);
        Data oldValueData = null;
        boolean absent = true;
        if (record == null) {
            if (loader != null) {
                Object oldValue = loader.load(mapService.getNodeEngine().toObject(dataKey));
                oldValueData = mapService.getNodeEngine().toData(oldValue);
                absent = oldValueData == null;
            }
            if (absent) {
                record = mapService.createRecord(name, dataKey, dataValue, ttl);
                records.put(dataKey, record);
                record.setActive(true);
                if (store != null && writeDelayMillis == 0) {
                    store.store(record.getKey(), record.getValue());
                }
                if (ttl > 0)
                    mapService.notifyMapTtl(name);
            }
        } else {
            oldValueData = record.getValueData();
        }
        return oldValueData;
    }
}
