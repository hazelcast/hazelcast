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

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockStore;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.record.DataRecord;
import com.hazelcast.map.record.ObjectRecord;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.util.scheduler.EntryTaskScheduler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author enesakar 1/17/13
 */
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
        final LockService lockService = mapService.getNodeEngine().getSharedService(LockService.SERVICE_NAME);
        this.lockStore = lockService == null ? null :
                lockService.createLockStore(partitionContainer.getPartitionId(), new DefaultObjectNamespace(MapService.SERVICE_NAME, name));
    }

    public void flush() {
        Set<Data> keys = new HashSet<Data>();
        for (Record record : records.values()) {
            keys.add(record.getKey());
        }
        EntryTaskScheduler writeScheduler = mapContainer.getMapStoreWriteScheduler();
        if (writeScheduler != null) {
            Set<Data> processedKeys = writeScheduler.flush(keys);
            for (Data key : processedKeys) {
                records.get(key).onStore();
            }
        }
        EntryTaskScheduler deleteScheduler = mapContainer.getMapStoreDeleteScheduler();
        if (deleteScheduler != null) {
            deleteScheduler.flush(toBeRemovedKeys);
            toBeRemovedKeys.clear();
        }
    }

    private void flush(Data key) {
        EntryTaskScheduler writeScheduler = mapContainer.getMapStoreWriteScheduler();
        Set<Data> keys = new HashSet<Data>(1);
        keys.add(key);
        if (writeScheduler != null) {
            Set<Data> processedKeys = writeScheduler.flush(keys);
            for (Data pkey : processedKeys) {
                records.get(pkey).onStore();
            }
        }
        EntryTaskScheduler deleteScheduler = mapContainer.getMapStoreDeleteScheduler();
        if (deleteScheduler != null) {
            if (toBeRemovedKeys.contains(key)) {
                deleteScheduler.flush(keys);
                toBeRemovedKeys.remove(key);
            }
        }
    }

    public MapContainer getMapContainer() {
        return mapContainer;
    }

    public Map<Data, Record> getRecords() {
        return records;
    }

    void clear() {
        final LockService lockService = mapService.getNodeEngine().getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            lockService.clearLockStore(partitionContainer.getPartitionId(), new DefaultObjectNamespace(MapService.SERVICE_NAME, name));
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

            if (record.getValue().equals(testValue)) {
                return true;
            }
        }
        return false;
    }

    public boolean lock(Data key, String caller, int threadId, long ttl) {
        return lockStore != null && lockStore.lock(key, caller, threadId, ttl);
    }

    public boolean txnLock(Data key, String caller, int threadId, long ttl) {
        return lockStore != null && lockStore.txnLock(key, caller, threadId, ttl);
    }

    public boolean extendLock(Data key, String caller, int threadId, long ttl) {
        return lockStore != null && lockStore.extendLeaseTime(key, caller, threadId, ttl);
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

    public String getLockOwnerInfo(Data key) {
        return lockStore != null ? lockStore.getOwnerInfo(key) : null;
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
        Data data = record != null ? mapService.toData(record.getValue()) : null;
        return new AbstractMap.SimpleImmutableEntry<Data, Data>(dataKey, data);
    }

    public Map.Entry<Data, Object> getMapEntryObject(Data dataKey) {
        Record record = records.get(dataKey);
        Object value = record != null ? mapService.toObject(record.getValue()) : null;
        return new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, value);
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
        final Map<Data, Record> lockRecords = new HashMap<Data, Record>(locks.size());
//        keys with locks will be re-inserted
        for (Data key : locks) {
            Record record = records.get(key);
            if (record != null) {
                lockRecords.put(key, record);
            }
        }
        Set<Data> keysToDelete = records.keySet();
        keysToDelete.removeAll(lockRecords.keySet());

        final MapStoreWrapper store = mapContainer.getStore();
        if (store != null) {
            Set<Object> keysObject = new HashSet<Object>();
            for (Data key : keysToDelete) {
                // todo ea have a removeAllIndexes(Keys) method for optimizations
                removeIndex(key);
                keysObject.add(mapService.toObject(key));
            }
            store.deleteAll(keysObject);
            toBeRemovedKeys.clear();
        }

        records.clear();
        records.putAll(lockRecords);
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
                    removeIndex(dataKey);
                    mapStoreDelete(null, dataKey);
                }
            }
        } else {
            oldValue = record.getValue();
            oldValue = mapService.interceptRemove(name, oldValue);
            if (oldValue != null) {
                removeIndex(dataKey);
                mapStoreDelete(record, dataKey);
            }
            records.remove(dataKey);
        }
        return oldValue;
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
            flush(dataKey);
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
        if (mapService.compare(name, testValue, oldValue)) {
            mapService.interceptRemove(name, oldValue);
            removeIndex(dataKey);
            mapStoreDelete(record, dataKey);
            records.remove(dataKey);
            removed = true;
        }
        return removed;
    }

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
                // below is an optimization. if the record does not exist the next get will return null without looking at mapStore.
                if (value == null) {
                    record = mapService.createRecord(name, dataKey, null, 100);
                    records.put(dataKey, record);
                }
            }

        } else {
            accessRecord(record);
            value = record.getValue();
        }
        value = mapService.interceptGet(name, value);

        return value;
    }

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

        // because of a get optimization (see above), there may be a record with a null value,
        // which means map-store returned null while loading the key.
        boolean contains = record != null && record.getValue() != null;
        if(contains) {
            accessRecord(record);
        }
        return contains;
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

    public boolean set(Data dataKey, Object value, long ttl) {
        Record record = records.get(dataKey);
        boolean newRecord = false;
        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            record = mapService.createRecord(name, dataKey, value, ttl);
            mapStoreWrite(record, dataKey, value);
            records.put(dataKey, record);
            newRecord = true;
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            mapStoreWrite(record, dataKey, value);
            setRecordValue(record, value);
            updateTtl(record, ttl);
        }
        saveIndex(record);

        return newRecord;
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
            Object oldValue = record.getValue();
            EntryView existingEntry = new SimpleEntryView(mapService.toObject(record.getKey()), mapService.toObject(record.getValue()), record);
            newValue = mergePolicy.merge(name, mergingEntry, existingEntry);
            if (newValue == null) { // existing entry will be removed
                records.remove(dataKey);
                removeIndex(dataKey);
                mapStoreDelete(record, dataKey);
                return true;
            }
            // same with the existing entry so no need to mapstore etc operations.
            if (mapService.compare(name, newValue, oldValue)) {
                return true;
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
            mapService.scheduleIdleEviction(name, record.getKey(), maxIdleSeconds * 1000);
        }
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

    private void mapStoreWrite(Record record, Data key, Object value) {
        final MapStoreWrapper store = mapContainer.getStore();
        if (store != null) {
            long writeDelayMillis = mapContainer.getWriteDelayMillis();
            if (writeDelayMillis <= 0) {
                store.store(mapService.toObject(key), mapService.toObject(value));
                if (record != null)
                    record.onStore();
            } else {
                mapService.scheduleMapStoreWrite(name, key, value, writeDelayMillis);
            }
        }
    }

    private void mapStoreDelete(Record record, Data key) {
        final MapStoreWrapper store = mapContainer.getStore();
        if (store != null) {
            long writeDelayMillis = mapContainer.getWriteDelayMillis();
            if (writeDelayMillis == 0) {
                store.delete(mapService.toObject(key));
                // todo ea record will be deleted then why calling onStore
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
            mapService.scheduleTtlEviction(name, record, ttl);
        } else if (ttl == 0) {
            mapContainer.getTtlEvictionScheduler().cancel(record.getKey());
        }

    }

    private void setRecordValue(Record record, Object value) {
        accessRecord(record);
        record.onUpdate();
        if (record instanceof DataRecord)
            ((DataRecord) record).setValue(mapService.toData(value));
        else if (record instanceof ObjectRecord)
            ((ObjectRecord) record).setValue(mapService.toObject(value));
    }
}
