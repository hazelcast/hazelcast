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

import com.hazelcast.concurrent.lock.LockNamespace;
import com.hazelcast.concurrent.lock.LockStore;
import com.hazelcast.concurrent.lock.SharedLockService;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PartitionRecordStore implements RecordStore {
    final String name;
    final PartitionContainer partitionContainer;
    final ConcurrentMap<Data, Record> records = new ConcurrentHashMap<Data, Record>(1000);
    final Set<Data> toBeRemovedKeys = new HashSet<Data>();
    final MapContainer mapContainer;
    final MapService mapService;
    final LockStore lockStore;

    public PartitionRecordStore(String name, PartitionContainer partitionContainer) {
        this.name = name;
        this.partitionContainer = partitionContainer;
        this.mapService = partitionContainer.getMapService();
        this.mapContainer = mapService.getMapContainer(name);
        final SharedLockService lockService = mapService.getNodeEngine().getSharedService(SharedLockService.SERVICE_NAME);
        this.lockStore = lockService == null ? null :
                lockService.createLockStore(partitionContainer.partitionId, new LockNamespace(MapService.SERVICE_NAME, name),
                        mapContainer.getBackupCount(), mapContainer.getAsyncBackupCount());
    }

    // todo flush operation should cancel scheduled mapstore operations
    public void flush(boolean flushAllRecords) {
        for (Record record : records.values()) {
            if (flushAllRecords || record.getState().isDirty()) {
                mapContainer.getStore().store(mapService.toObject(record.getKey()), mapService.toObject(record.getValue()));
                record.onStore();
            }
        }
        for (Data key : toBeRemovedKeys) {
            if (toBeRemovedKeys.remove(key))
                mapContainer.getStore().delete(mapService.toObject(key));
        }
    }

    public MapContainer getMapContainer() {
        return mapContainer;
    }

    public ConcurrentMap<Data, Record> getRecords() {
        return records;
    }

    void clear() {
        final SharedLockService lockService = mapService.getNodeEngine().getSharedService(SharedLockService.SERVICE_NAME);
        if (lockService != null) {
            lockService.destroyLockStore(partitionContainer.partitionId, new LockNamespace(MapService.SERVICE_NAME, name));
        }
        records.clear();
    }

    public int size() {
        return records.size();
    }

    public boolean containsValue(Object value) {
        for (Record record : records.values()) {
            Object testValue;
            if (record instanceof DataRecord) {
                testValue = mapService.toData(value);
            } else {
                testValue = mapService.toObject(value);
            }

            if (record.getValue().equals(testValue))
                return true;
        }
        return false;
    }

    public boolean lock(Data key, String caller, int threadId, long ttl) {
        return lockStore != null && lockStore.lock(key, caller, threadId, ttl);
    }

    public boolean extendLock(Data key, String caller, int threadId, long ttl) {
        return lockStore != null && lockStore.extendTTL(key, caller, threadId, ttl);
    }

    public boolean unlock(Data key, String caller, int threadId) {
        return lockStore != null && lockStore.unlock(key, caller, threadId);
    }

    public boolean forceUnlock(Data dataKey) {
        return lockStore != null && lockStore.forceUnlock(dataKey);
    }

    public boolean isLocked(Data dataKey) {
        return lockStore != null && lockStore.isLocked(dataKey);
    }

    public boolean isLockedBy(Data key, String caller, int threadId) {
        return lockStore != null && lockStore.isLockedBy(key, caller, threadId);
    }

    public boolean canAcquireLock(Data key, String caller, int threadId) {
        return lockStore == null || lockStore.canAcquireLock(key, caller, threadId);
    }

    public boolean canRun(LockAwareOperation lockAwareOperation) {
        return lockStore == null || lockStore.canAcquireLock(lockAwareOperation.getKey(),
                lockAwareOperation.getCallerUuid(), lockAwareOperation.getThreadId());
    }

    public Object tryRemove(Data dataKey) {
        Record record = records.get(dataKey);
        boolean removed = false;
        boolean evicted = false;
        Object oldValue = null;
        if (record == null) {
            // already removed from map by eviction but still need to delete it
            if (mapContainer.getStore() != null && mapContainer.getStore().load(mapService.toObject(dataKey)) != null) {
                mapStoreDelete(record, dataKey);
            }
        } else {
            accessRecord(record);
            mapService.interceptRemove(name, record.getValue());
            mapStoreDelete(record, dataKey);
            Record removedRecord = records.remove(dataKey);
            oldValue = removedRecord.getValue();
            removed = true;
        }

        return oldValue;
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
        final Collection<Data> locks = lockStore != null ? lockStore.getLockedKeys() : Collections.<Data>emptySet();
        final ConcurrentMap<Data, Record> temp = new ConcurrentHashMap<Data, Record>(locks.size());
//        keys with locks will be re-inserted
        for (Data key : locks) {
            temp.put(key, records.get(key));
        }
        records.clear();
        records.putAll(temp);
    }

    public void reset() {
        records.clear();
    }


    public Object remove(Data dataKey) {
        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                oldValue = mapContainer.getStore().load(mapService.toObject(dataKey));
                if (oldValue != null) {
                    mapStoreDelete(record, dataKey);
                }
            }
        } else {
            oldValue = record.getValue();
            oldValue = mapService.interceptRemove(name, oldValue);
            if (oldValue != null) {
                mapStoreDelete(record, dataKey);
            }
            records.remove(dataKey);
        }
        return oldValue;
    }

    public void delete(Data dataKey) {
        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                oldValue = mapContainer.getStore().load(mapService.toObject(dataKey));
                if (oldValue != null) {
                    mapStoreDelete(record, dataKey);
                }
            }
        } else {
            oldValue = record.getValue();
            oldValue = mapService.interceptRemove(name, oldValue);
            if (oldValue != null) {
                mapStoreDelete(record, dataKey);
            }
            records.remove(dataKey);
        }
    }

    private void removeIndex(Data key) {
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            indexService.removeEntryIndex(key);
        }
    }

    public Object evict(Data dataKey) {
        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record != null) {
            mapService.interceptRemove(name, record.getValue());
            oldValue = record.getValue();
            records.remove(dataKey);
            removeIndex(dataKey);
        }
        return oldValue;
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
        if(mapService.compare(name, testValue, oldValue)) {
            mapService.interceptRemove(name, oldValue);
            mapStoreDelete(record, dataKey);
            records.remove(dataKey);
            removed = true;
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
        value = mapService.interceptGet(name, value);
//        check if record has expired or removed but waiting delay millis
        if (record != null && record.getState().isExpired()) {
            System.out.println("record has been waiting to be deleted");
            return null;
        }
        return value;
    }

    // todo containsKey tries to load from db even if it returned null just second ago. try to put a record with null with eviction
    public boolean containsKey(Data dataKey) {
        Record record = records.get(dataKey);
        if (record == null) {
            if (mapContainer.getStore() != null) {
                Object value = mapContainer.getStore().load(mapService.toObject(dataKey));
                if (value != null) {
                    record = mapService.createRecord(name, dataKey, value, -1);
                    records.put(dataKey, record);
                }
            }
        }
        return record != null;
    }

    public void put(Map.Entry<Data, Object> entry) {
        Data dataKey = entry.getKey();
        Object value = entry.getValue();
        Record record = records.get(dataKey);
        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            record = mapService.createRecord(name, dataKey, value, -1);
            mapStoreWrite(record, dataKey, value);
            records.put(dataKey, record);
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            mapStoreWrite(record, dataKey, value);
            setRecordValue(record, value);
        }
        saveIndex(record);
    }

    public Object put(Data dataKey, Object value, long ttl) {
        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                oldValue = mapContainer.getStore().load(mapService.toObject(dataKey));
            }
            value = mapService.interceptPut(name, null, value);
            record = mapService.createRecord(name, dataKey, value, ttl);
            mapStoreWrite(record, dataKey, value);
            records.put(dataKey, record);
        } else {
            oldValue = record.getValue();
            value = mapService.interceptPut(name, oldValue, value);
            mapStoreWrite(record, dataKey, value);
            setRecordValue(record, value);
            updateTtl(record, ttl);
        }
        saveIndex(record);
        return oldValue;
    }

    public boolean merge(Data dataKey, EntryView mergingEntry, MapMergePolicy mergePolicy) {
        Record record = records.get(dataKey);
        Object newValue = null;
        if (record == null) {
            newValue = mergingEntry.getValue();
            record = mapService.createRecord(name, dataKey, newValue, -1);
            mapStoreWrite(record, dataKey, newValue);
            records.put(dataKey, record);
        } else {
            EntryView existingEntry = new SimpleEntryView(mapService.toObject(record.getKey()), mapService.toObject(record.getValue()), record);
            newValue = mergePolicy.merge(name, mergingEntry, existingEntry);
            if (newValue == null) { // existing entry will stay
                return false;
            }
            mapStoreWrite(record, dataKey, newValue);
            if (record instanceof DataRecord)
                ((DataRecord) record).setValue(mapService.toData(newValue));
            else if (record instanceof ObjectRecord)
                ((ObjectRecord) record).setValue(mapService.toObject(newValue));
        }
        saveIndex(record);
        return newValue != null;
    }

    private void saveIndex(Record record) {
        Data dataKey = record.getKey();
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            SerializationService ss = mapService.getSerializationService();
            QueryableEntry queryableEntry = new QueryEntry(ss, dataKey, dataKey, record.getValue());
            indexService.saveEntryIndex(queryableEntry);
        }
    }

    public Object replace(Data dataKey, Object value) {
        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record != null) {
            oldValue = record.getValue();
            value = mapService.interceptPut(name, oldValue, value);
            mapStoreWrite(record, dataKey, value);
            setRecordValue(record, value);
        } else {
            return null;
        }
        saveIndex(record);

        return oldValue;
    }

    public boolean replace(Data dataKey, Object testValue, Object newValue) {
        Record record = records.get(dataKey);
        if (record == null)
            return false;
        if (mapService.compare(name, record.getValue(), testValue)) {
            newValue = mapService.interceptPut(name, record.getValue(), newValue);
            mapStoreWrite(record, dataKey, newValue);
            setRecordValue(record, newValue);
        } else {
            return false;
        }
        saveIndex(record);

        return true;
    }

    public void set(Data dataKey, Object value, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            record = mapService.createRecord(name, dataKey, value, ttl);
            mapStoreWrite(record, dataKey, value);
            records.put(dataKey, record);
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            mapStoreWrite(record, dataKey, value);
            setRecordValue(record, value);
            updateTtl(record, ttl);
        }
        saveIndex(record);

    }

    public void putTransient(Data dataKey, Object value, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            record = mapService.createRecord(name, dataKey, value, ttl);
            records.put(dataKey, record);
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            setRecordValue(record, value);
            updateTtl(record, ttl);
        }
    }

    public boolean tryPut(Data dataKey, Object value, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            record = mapService.createRecord(name, dataKey, value, ttl);
            mapStoreWrite(record, dataKey, value);
            records.put(dataKey, record);
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            mapStoreWrite(record, dataKey, value);
            setRecordValue(record, value);
            updateTtl(record, ttl);
        }
        saveIndex(record);

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
            value = mapService.interceptPut(name, null, value);
            record = mapService.createRecord(name, dataKey, value, ttl);
            mapStoreWrite(record, dataKey, value);
            records.put(dataKey, record);
            updateTtl(record, ttl);
        }
        saveIndex(record);

        return oldValue;
    }

    private void accessRecord(Record record) {
        record.onAccess();
        int maxIdleSeconds = mapContainer.getMapConfig().getMaxIdleSeconds();
        if (maxIdleSeconds > 0) {
            record.getState().updateIdleExpireTime(maxIdleSeconds * 1000);
            mapService.scheduleIdleEviction(name, record.getKey(), maxIdleSeconds * 1000);
        }
    }

    private void mapStoreWrite(Record record, Data key, Object value) {
        if (mapContainer.getStore() != null) {
            long writeDelayMillis = mapContainer.getWriteDelayMillis();
            if (writeDelayMillis <= 0) {
                mapContainer.getStore().store(mapService.toObject(key), mapService.toObject(value));
                if (record != null)
                    record.onStore();
            } else {
                // check if there is already scheduled store task
                if (record != null && record.getState().getStoreTime() <= 0) {
                    record.getState().updateStoreTime(writeDelayMillis);
                    mapService.scheduleMapStoreWrite(name, key, value, writeDelayMillis);
                }
            }
        }
    }

    private void mapStoreDelete(Record record, Data key) {
        removeIndex(key);
        if (mapContainer.getStore() != null) {
            long writeDelayMillis = mapContainer.getWriteDelayMillis();
            if (writeDelayMillis == 0) {
                mapContainer.getStore().delete(mapService.toObject(key));
                if (record != null)
                    record.onStore();
            } else {
                mapService.scheduleMapStoreDelete(name, key, writeDelayMillis);
                toBeRemovedKeys.add(key);
            }
        }
    }

    private void updateTtl(Record record, long ttl) {
        if (ttl > 0) {
            record.getState().updateTtlExpireTime(ttl);
            mapService.scheduleTtlEviction(name, record.getKey(), ttl);
        }
    }

    public void setRecordValue(Record record, Object value) {
        accessRecord(record);
        record.onUpdate();
        if (record instanceof DataRecord)
            ((DataRecord) record).setValue(mapService.toData(value));
        else if (record instanceof ObjectRecord)
            ((ObjectRecord) record).setValue(mapService.toObject(value));
    }
}
