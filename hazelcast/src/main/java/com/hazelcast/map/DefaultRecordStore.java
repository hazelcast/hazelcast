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

import com.hazelcast.lock.LockInfo;
import com.hazelcast.lock.LockStore;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultRecordStore implements RecordStore {
    final String name;
    final PartitionInfo partitionInfo;
    final PartitionContainer partitionContainer;

    final ConcurrentMap<Data, Record> records = new ConcurrentHashMap<Data, Record>(1000);
    // todo remove this. do not forget to migrate if decide not to remove
    final Set<Data> removedDelayedKeys = Collections.newSetFromMap(new ConcurrentHashMap<Data, Boolean>());
    final MapContainer mapContainer;
    final MapService mapService;
    final LockStore lockStore;

    public DefaultRecordStore(String name, PartitionContainer partitionContainer) {
        this.name = name;
        this.partitionInfo = partitionContainer.partitionInfo;
        this.partitionContainer = partitionContainer;
        this.mapService = partitionContainer.getMapService();
        this.mapContainer = mapService.getMapContainer(name);
        this.lockStore = new LockStore();
    }

    public void flush(boolean flushAllRecords) {
        for (Record record : records.values()) {
            if (flushAllRecords || record.getState().isDirty())
                mapStoreWrite(record, true);
        }
    }

    public MapContainer getMapContainer() {
        return mapContainer;
    }

    public ConcurrentMap<Data, Record> getRecords() {
        return records;
    }

    void clear() {
        records.clear();
        lockStore.clear();
    }

    public int size() {
        return records.size();
    }

    public boolean containsValue(Object dataValue) {
        for (Record record : records.values()) {
            Object value = mapService.toObject(dataValue);
            Object recordValue = mapService.toObject(record.getValue());
            if (recordValue.equals(value))
                return true;
        }
        return false;
    }

    public boolean lock(Data dataKey, Address caller, int threadId, long ttl) {
        return lockStore.lock(dataKey, caller, threadId, ttl);
    }

    public boolean isLocked(Data dataKey) {
        return lockStore.isLocked(dataKey);
    }

    public boolean canRun(LockAwareOperation lockAwareOperation) {
        return lockStore.canAcquireLock(lockAwareOperation.getKey(),
                lockAwareOperation.getCaller(), lockAwareOperation.getThreadId());
    }

    public boolean unlock(Data dataKey, Address caller, int threadId) {
        return lockStore.unlock(dataKey, caller, threadId);
    }

    public boolean forceUnlock(Data dataKey) {
        return lockStore.forceUnlock(dataKey);
    }

    public Map<Data, LockInfo> getLocks() {
        return lockStore.getLocks();
    }

    public void putLock(Data key, LockInfo lock) {
        lockStore.putLock(key, lock);
    }

    public boolean tryRemove(Data dataKey) {
        Record record = records.get(dataKey);
        boolean removed = false;
        boolean evicted = false;
        if (record == null) {
            // already removed from map by eviction but still need to delete it
            if (mapContainer.getStore() != null && mapContainer.getStore().load(mapService.toObject(dataKey)) != null) {
                evicted = true;
            }
        } else {
            accessRecord(record);
            mapService.intercept(name, MapOperationType.REMOVE, dataKey, record.getValue(), record.getValue());
            records.remove(dataKey);
            removed = true;
        }
        if ((removed || evicted) && mapContainer.getStore() != null) {
            mapStoreDelete(record);
        }
        return removed;
    }

    public Set<Map.Entry<Data, Object>> entrySetObject() {
        Map<Data, Object> temp = new HashMap<Data, Object>(records.size());
        for (Data key : records.keySet()) {
            temp.put(key, mapService.toObject(records.get(key).getValue()));
        }
        return temp.entrySet();
    }

    public Set<Map.Entry<Data, Data>> entrySetData() {
        Map<Data, Data> temp = new HashMap<Data, Data>(records.size());
        for (Data key : records.keySet()) {
            temp.put(key, mapService.toData(records.get(key).getValue()));
        }
        return temp.entrySet();
    }

    public Map.Entry<Data, Data> getMapEntryData(Data dataKey) {
        Record record = records.get(dataKey);
        return new AbstractMap.SimpleImmutableEntry<Data, Data>(dataKey, mapService.toData(record.getValue()));
    }

    public Map.Entry<Data, Object> getMapEntryObject(Data dataKey) {
        Record record = records.get(dataKey);
        return new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, mapService.toObject(record.getValue()));
    }

    public Set<Data> keySet() {
        Set<Data> keySet = new HashSet<Data>(records.size());
        for (Data data : records.keySet()) {
            keySet.add(data);
        }
        return keySet;
    }

    public Collection<Object> valuesObject() {
        Collection<Object> values = new ArrayList<Object>(records.size());
        for (Record record : records.values()) {
            values.add(mapService.toObject(record.getValue()));
        }
        return values;
    }

    public Collection<Data> valuesData() {
        Collection<Data> values = new ArrayList<Data>(records.size());
        for (Record record : records.values()) {
            values.add(mapService.toData(record.getValue()));
        }
        return values;
    }

    public void removeAll() {
        Map<Data, LockInfo> locks = lockStore.getLocks();
        final ConcurrentMap<Data, Record> temp = new ConcurrentHashMap<Data, Record>(locks.size());
        // keys with locks will be re-inserted
        for (Data key : locks.keySet()) {
            temp.put(key, records.get(key));
        }
        records.clear();
        records.putAll(temp);
    }

    public Object remove(Data dataKey) {
        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                oldValue = mapContainer.getStore().load(mapService.toObject(dataKey));
            }
        } else {
            oldValue = record.getValue();
            oldValue = mapService.intercept(name, MapOperationType.REMOVE, dataKey, oldValue, oldValue);
            records.remove(dataKey);
        }
        if (oldValue != null) {
            mapStoreDelete(record);
        }
        return oldValue;
    }

    private void removeIndex(Data key) {
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            indexService.removeEntryIndex(key);
        }
    }

    public boolean evict(Data dataKey) {
        Record record = records.remove(dataKey);
        if (record != null) {
            mapService.intercept(name, MapOperationType.EVICT, dataKey, record.getValue(), record.getValue());
            removeIndex(dataKey);
            return true;
        }
        return false;
    }

    public boolean remove(Data dataKey, Object testValue) {
        Record record = records.get(dataKey);
        Object oldValue = null;
        boolean removed = false;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                oldValue = mapContainer.getStore().load(mapService.toObject(dataKey));
            }
            if (oldValue == null)
                return false;
        } else {
            oldValue = record.getValue();
        }
        if (mapService.toObject(testValue).equals(mapService.toObject(oldValue))) {
            mapService.intercept(name, MapOperationType.REMOVE, dataKey, oldValue, oldValue);
            records.remove(dataKey);
            removed = true;
            mapStoreDelete(record);
        }
        return removed;
    }

    // todo get tries to load from db even if it returned null just second ago. try to put a record with null with eviction
    public Object get(Data dataKey) {
        Record record = records.get(dataKey);
        Object value = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                value = mapContainer.getStore().load(mapService.toObject(dataKey));
                if (value != null) {
                    record = mapService.createRecord(name, dataKey, value, -1);
                    records.put(dataKey, record);
                }
            }
        } else {
            accessRecord(record);
            value = record.getValue();
        }
        value = mapService.intercept(name, MapOperationType.GET, dataKey, value, value);
//        check if record has expired or removed but waiting delay millis
        if (record != null && record.getState().isExpired()) {
            return null;
        }
        return value;
    }

    public void put(Map.Entry<Data, Object> entry) {
        Data dataKey = entry.getKey();
        Object value = entry.getValue();
        Record record = records.get(dataKey);
        if (record == null) {
            value = mapService.intercept(name, MapOperationType.PUT, dataKey, value, null);
            record = mapService.createRecord(name, dataKey, value, -1);
            records.put(dataKey, record);
        } else {
            value = mapService.intercept(name, MapOperationType.PUT, dataKey, value, record.getValue());
            setRecordValue(record, value);
        }
    }

    public Object put(Data dataKey, Object value, long ttl) {
        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                oldValue = mapContainer.getStore().load(mapService.toObject(dataKey));
            }
            value = mapService.intercept(name, MapOperationType.PUT, dataKey, value, null);
            record = mapService.createRecord(name, dataKey, value, ttl);
            records.put(dataKey, record);
        } else {
            oldValue = record.getValue();
            value = mapService.intercept(name, MapOperationType.PUT, dataKey, value, oldValue);
            setRecordValue(record, value);
            updateTtl(record, ttl);
        }
        mapStoreWrite(record);
        return oldValue;
    }

    private void saveIndex(Record record) {
        Data dataKey = record.getKey();
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            SerializationServiceImpl ss = (SerializationServiceImpl) mapService.getSerializationService();
            QueryableEntry queryableEntry = new QueryEntry(ss, dataKey, dataKey, record.getValue());
            indexService.saveEntryIndex(queryableEntry);
        }
    }

    public Set<Data> getRemovedDelayedKeys() {
        return removedDelayedKeys;
    }

    public Object replace(Data dataKey, Object value) {
        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record != null) {
            oldValue = record.getValue();
            value = mapService.intercept(name, MapOperationType.PUT, dataKey, value, oldValue);
            setRecordValue(record, value);
        } else {
            return null;
        }
        mapStoreWrite(record);
        return oldValue;
    }

    public boolean replace(Data dataKey, Object testValue, Object newValue) {
        Record record = records.get(dataKey);
        if (record == null)
            return false;
        Object recordValue = mapService.toObject(record.getValue());
        if (recordValue != null && recordValue.equals(mapService.toObject(testValue))) {
            newValue = mapService.intercept(name, MapOperationType.PUT, dataKey, newValue, recordValue);
            setRecordValue(record, newValue);
        } else {
            return false;
        }
        mapStoreWrite(record);
        return true;
    }

    public void set(Data dataKey, Object value, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            value = mapService.intercept(name, MapOperationType.PUT, dataKey, value, null);
            record = mapService.createRecord(name, dataKey, value, ttl);
            records.put(dataKey, record);
        } else {
            value = mapService.intercept(name, MapOperationType.PUT, dataKey, value, record.getValue());
            setRecordValue(record, value);
            updateTtl(record, ttl);
        }
        mapStoreWrite(record);
    }

    public void putTransient(Data dataKey, Object value, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            value = mapService.intercept(name, MapOperationType.PUT, dataKey, value, null);
            record = mapService.createRecord(name, dataKey, value, ttl);
            records.put(dataKey, record);
        } else {
            value = mapService.intercept(name, MapOperationType.PUT, dataKey, value, record.getValue());
            setRecordValue(record, value);
            updateTtl(record, ttl);
        }
    }

    public boolean tryPut(Data dataKey, Object value, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            value = mapService.intercept(name, MapOperationType.PUT, dataKey, value, null);
            record = mapService.createRecord(name, dataKey, value, ttl);
            records.put(dataKey, record);
        } else {
            value = mapService.intercept(name, MapOperationType.PUT, dataKey, value, record.getValue());
            setRecordValue(record, value);
            updateTtl(record, ttl);
        }
        mapStoreWrite(record);
        return true;
    }

    public Object putIfAbsent(Data dataKey, Object value, long ttl) {
        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                oldValue = mapContainer.getStore().load(mapService.toObject(dataKey));
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
            value = mapService.intercept(name, MapOperationType.PUT, dataKey, value, null);
            record = mapService.createRecord(name, dataKey, value, ttl);
            records.put(dataKey, record);
            updateTtl(record, ttl);
            mapStoreWrite(record);
        }
        return oldValue;
    }

    private void accessRecord(Record record) {
        record.access();
        int maxIdleSeconds = mapContainer.getMapConfig().getMaxIdleSeconds();
        if (maxIdleSeconds > 0) {
            record.getState().updateIdleExpireTime(maxIdleSeconds);
            mapService.scheduleOperation(name, record.getKey(), maxIdleSeconds);
        }
    }

    private void mapStoreWrite(Record record) {
        mapStoreWrite(record, false);
        saveIndex(record);
    }

    private void mapStoreWrite(Record record, boolean flush) {
        if (mapContainer.getStore() != null) {
            long writeDelayMillis = mapContainer.getWriteDelayMillis();
            if (writeDelayMillis <= 0 || flush) {
                mapContainer.getStore().store(mapService.toObject(record.getKey()), mapService.toObject(record.getValue()));
            } else {
                if (record.getState().getStoreTime() <= 0) {
                    record.getState().updateStoreTime(writeDelayMillis);
                    mapService.scheduleOperation(name, record.getKey(), writeDelayMillis);
                }
            }
        }
    }

    private void mapStoreDelete(Record record) {
        removeIndex(record.getKey());
        if (mapContainer.getStore() != null) {
            long writeDelayMillis = mapContainer.getWriteDelayMillis();
            if (writeDelayMillis == 0) {
                mapContainer.getStore().delete(mapService.toObject(record.getKey()));
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
