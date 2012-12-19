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
import com.hazelcast.impl.DefaultRecord;
import com.hazelcast.impl.Record;
import com.hazelcast.nio.Data;
import com.hazelcast.partition.PartitionInfo;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

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

    boolean canRun(LockAwareOperation op) {
        LockInfo lock = locks.get(op.getKey());
        return lock == null || lock.testLock(op.getThreadId(), op.getCaller());
    }

    void clear() {
        records.clear();
        locks.clear();
    }

    public Data put(Data dataKey, Data dataValue, long ttl) {
        Record record = records.get(dataKey);
        Data oldValueData = null;
        if (record == null) {
            if (loader != null) {
                Object oldValue = loader.load(toObject(dataKey));
                oldValueData = toData(oldValue);
            }
            record = new DefaultRecord(partitionInfo.getPartitionId(), dataKey, dataValue, ttl, -1, mapService.nextId());
            records.put(dataKey, record);
        } else {
            oldValueData = record.getValueData();
            record.setValueData(dataValue);
        }
        record.setActive();
        record.setDirty(true);
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
        record.setActive();
        record.setDirty(true);
        if (store != null && writeDelayMillis == 0) {
            store.store(record.getKey(), record.getValue());
        }
        return oldValueData;
    }


    public boolean replace(Data dataKey, Data oldValue, Data newValue) {
        Record record = records.get(dataKey);
        boolean replaced = false;
        if (record != null && record.getValue().equals(toObject(oldValue))) {
            record.setValueData(newValue);
            replaced = true;
        } else {
            return false;
        }
        record.setActive();
        record.setDirty(true);
        if (store != null && writeDelayMillis == 0) {
            store.store(record.getKey(), record.getValue());
        }
        return replaced;
    }


    public void set(Data dataKey, Data dataValue, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            record = new DefaultRecord(partitionInfo.getPartitionId(), dataKey, dataValue, ttl, -1, mapService.nextId());
            records.put(dataKey, record);
        } else {
            record.setValueData(dataValue);
        }
        record.setActive();
        record.setDirty(true);
        if (store != null && writeDelayMillis == 0) {
            store.store(record.getKey(), record.getValue());
        }
    }


    public void putTransient(Data dataKey, Data dataValue, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            record = new DefaultRecord(partitionInfo.getPartitionId(), dataKey, dataValue, ttl, -1, mapService.nextId());
            records.put(dataKey, record);
        } else {
            record.setValueData(dataValue);
        }
        record.setActive();
        record.setDirty(true);
    }

    public boolean tryPut(Data dataKey, Data dataValue, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            record = new DefaultRecord(partitionInfo.getPartitionId(), dataKey, dataValue, ttl, -1, mapService.nextId());
            records.put(dataKey, record);
        } else {
            record.setValueData(dataValue);
        }
        record.setActive();
        record.setDirty(true);
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
                Object oldValue = loader.load(toObject(dataKey));
                oldValueData = toData(oldValue);
                absent = false;
            }
            if (absent) {
                record = new DefaultRecord(partitionInfo.getPartitionId(), dataKey, dataValue, ttl, -1, mapService.nextId());
                records.put(dataKey, record);
                record.setValueData(dataValue);
                record.setActive();
                record.setDirty(true);
                if (store != null && writeDelayMillis == 0) {
                    store.store(record.getKey(), record.getValue());
                }
            }
        } else {
            oldValueData = record.getValueData();
        }
        return oldValueData;
    }
}
