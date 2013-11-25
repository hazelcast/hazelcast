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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.operation.PutAllOperation;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.scheduler.EntryTaskScheduler;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author enesakar 1/17/13
 */
public class DefaultRecordStore implements RecordStore {
    private final String name;
    private final int partitionId;
    private final ConcurrentMap<Data, Record> records = new ConcurrentHashMap<Data, Record>(1000);
    private final Set<Data> toBeRemovedKeys = new HashSet<Data>();
    private final MapContainer mapContainer;
    private final MapService mapService;
    private final LockStore lockStore;
    private final RecordFactory recordFactory;
    final SizeEstimator sizeEstimator;
    final AtomicBoolean loaded = new AtomicBoolean(false);

    public DefaultRecordStore(String name, MapService mapService, int partitionId) {
        this.name = name;
        this.partitionId = partitionId;
        this.mapService = mapService;
        this.mapContainer = mapService.getMapContainer(name);
        recordFactory = mapContainer.getRecordFactory();
        NodeEngine nodeEngine = mapService.getNodeEngine();
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        this.lockStore = lockService == null ? null :
                lockService.createLockStore(partitionId, new DefaultObjectNamespace(MapService.SERVICE_NAME, name));
        this.sizeEstimator = SizeEstimators.createMapSizeEstimator();
        if (nodeEngine.getThisAddress().equals(nodeEngine.getPartitionService().getPartitionOwner(partitionId))) {
            if (mapContainer.getStore() != null && !loaded.get()) {
                Map<Data, Object> loadedKeys = mapContainer.getInitialKeys();
                if (loadedKeys != null && !loadedKeys.isEmpty()) {
                    Map<Data, Object> partitionKeys = new HashMap<Data, Object>();
                    Iterator<Map.Entry<Data, Object>> iterator = loadedKeys.entrySet().iterator();
                    while (iterator.hasNext()) {
                        final Map.Entry<Data, Object> entry = iterator.next();
                        final Data data = entry.getKey();
                        if (partitionId == nodeEngine.getPartitionService().getPartitionId(data)) {
                            partitionKeys.put(data, entry.getValue());
                            iterator.remove();
                        }
                    }
                    try {
                        nodeEngine.getExecutionService().submit("hz:map-load", new MapLoadAllTask(partitionKeys));
                    } catch (Throwable t) {
                        throw ExceptionUtil.rethrow(t);
                    }
                } else {
                    loaded.set(true);
                }
            }
        } else {
            loaded.set(true);
        }
    }

    public boolean isLoaded() {
        return loaded.get();
    }

    public void setLoaded(boolean isLoaded) {
        loaded.set(isLoaded);
    }

    private void checkIfLoaded() {
        if (mapContainer.getStore() != null && !loaded.get()) {
            throw ExceptionUtil.rethrow(new RetryableHazelcastException("Map is not ready!!!"));
        }
    }

    public String getName() {
        return name;
    }

    public void flush() {
        checkIfLoaded();
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
        checkIfLoaded();
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

    public Record getRecord(Data key) {
        return records.get(key);
    }

    public void putRecord(Data key, Record record) {
        records.put(key, record);
    }

    public void deleteRecord(Data key) {
        Record record = records.remove(key);
        if (record != null) {
            record.invalidate();
        }
    }

    public Map<Data, Record> getReadonlyRecordMap() {
        return Collections.unmodifiableMap(records);
    }

    public void clearPartition() {
        final LockService lockService = mapService.getNodeEngine().getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            lockService.clearLockStore(partitionId, new DefaultObjectNamespace(MapService.SERVICE_NAME, name));
        }
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            for (Data key : records.keySet()) {
                indexService.removeEntryIndex(key);
            }
        }
        clearRecordsMap(Collections.<Data, Record>emptyMap());
        resetSizeEstimator();
        cancelAssociatedSchedulers(records.keySet());

    }

    private void clearRecordsMap(Map<Data, Record> excludeRecords) {
        InMemoryFormat inMemoryFormat = recordFactory.getStorageFormat();
        switch (inMemoryFormat) {
            case BINARY:
            case OBJECT:
                records.clear();
                if (excludeRecords != null && !excludeRecords.isEmpty()) {
                    records.putAll(excludeRecords);
                }
                return;

            case OFFHEAP:
                Iterator<Record> iter = records.values().iterator();
                while (iter.hasNext()) {
                    Record record = iter.next();
                    if (excludeRecords == null || !excludeRecords.containsKey(record.getKey())) {
                        record.invalidate();
                        iter.remove();
                    }
                }
                return;

            default:
                throw new IllegalArgumentException("Unknown storage format: " + inMemoryFormat);
        }
    }

    public int size() {
        checkIfLoaded();
        return records.size();
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public boolean containsValue(Object value) {
        checkIfLoaded();
        for (Record record : records.values()) {
            if (mapService.compare(name, value, record.getValue()))
                return true;
        }
        return false;
    }

    public boolean lock(Data key, String caller, int threadId, long ttl) {
        checkIfLoaded();
        return lockStore != null && lockStore.lock(key, caller, threadId, ttl);
    }

    public boolean txnLock(Data key, String caller, int threadId, long ttl) {
        checkIfLoaded();
        return lockStore != null && lockStore.txnLock(key, caller, threadId, ttl);
    }

    public boolean extendLock(Data key, String caller, int threadId, long ttl) {
        checkIfLoaded();
        return lockStore != null && lockStore.extendLeaseTime(key, caller, threadId, ttl);
    }

    public boolean unlock(Data key, String caller, int threadId) {
        checkIfLoaded();
        return lockStore != null && lockStore.unlock(key, caller, threadId);
    }

    public boolean forceUnlock(Data dataKey) {
        return lockStore != null && lockStore.forceUnlock(dataKey);
    }

    @Override
    public long getHeapCost() {
        return getSizeEstimator().getSize();
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
        checkIfLoaded();
        Map<Data, Object> temp = new HashMap<Data, Object>(records.size());
        for (Data key : records.keySet()) {
            temp.put(key, mapService.toObject(records.get(key).getValue()));
        }
        return temp.entrySet();
    }

    public Set<Map.Entry<Data, Data>> entrySetData() {
        checkIfLoaded();
        Map<Data, Data> temp = new HashMap<Data, Data>(records.size());
        for (Data key : records.keySet()) {
            temp.put(key, mapService.toData(records.get(key).getValue()));
        }
        return temp.entrySet();
    }

    public Map.Entry<Data, Data> getMapEntryData(Data dataKey) {
        checkIfLoaded();
        Record record = records.get(dataKey);
        Object value;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                value = mapContainer.getStore().load(mapService.toObject(dataKey));
                if (value != null) {
                    record = mapService.createRecord(name, dataKey, value, -1);
                    records.put(dataKey, record);
                    saveIndex(record);
                    updateSizeEstimator(calculateRecordSize(record));
                }
            }
        } else {
            accessRecord(record);
        }

        Data data = record != null ? mapService.toData(record.getValue()) : null;
        return new AbstractMap.SimpleImmutableEntry<Data, Data>(dataKey, data);
    }

    public Map.Entry<Data, Object> getMapEntryObject(Data dataKey) {
        checkIfLoaded();
        Record record = records.get(dataKey);
        Object value = record != null ? mapService.toObject(record.getValue()) : null;
        return new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, value);
    }

    public Set<Data> keySet() {
        checkIfLoaded();
        Set<Data> keySet = new HashSet<Data>(records.size());
        for (Data data : records.keySet()) {
            keySet.add(data);
        }
        return keySet;
    }

    public Collection<Object> valuesObject() {
        checkIfLoaded();
        Collection<Object> values = new ArrayList<Object>(records.size());
        for (Record record : records.values()) {
            values.add(mapService.toObject(record.getValue()));
        }
        return values;
    }

    public Collection<Data> valuesData() {
        checkIfLoaded();
        Collection<Data> values = new ArrayList<Data>(records.size());
        for (Record record : records.values()) {
            values.add(mapService.toData(record.getValue()));
        }
        return values;
    }

    public void clear() {
        checkIfLoaded();
        resetSizeEstimator();
        final Collection<Data> lockedKeys = lockStore != null ? lockStore.getLockedKeys() : Collections.<Data>emptySet();
        final Map<Data, Record> lockedRecords = new HashMap<Data, Record>(lockedKeys.size());
        // Locked records should not be removed!
        for (Data key : lockedKeys) {
            Record record = records.get(key);
            if (record != null) {
                lockedRecords.put(key, record);
                updateSizeEstimator(calculateRecordSize(record));
            }
        }
        Set<Data> keysToDelete = records.keySet();
        keysToDelete.removeAll(lockedRecords.keySet());

        final MapStoreWrapper store = mapContainer.getStore();
        Set<Object> keysObject = new HashSet<Object>();
        for (Data key : keysToDelete) {
            // todo ea have a clear(Keys) method for optimizations
            removeIndex(key);
            keysObject.add(mapService.toObject(key));
        }

        if (store != null) {
            store.deleteAll(keysObject);
            toBeRemovedKeys.removeAll(keysToDelete);
        }

        clearRecordsMap(lockedRecords);
        cancelAssociatedSchedulers(keysToDelete);
    }

    public void reset() {
        checkIfLoaded();
        clearRecordsMap(Collections.<Data, Record>emptyMap());
        resetSizeEstimator();
        cancelAssociatedSchedulers(records.keySet());
    }

    public Object remove(Data dataKey) {
        checkIfLoaded();
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
            // reduce size
            updateSizeEstimator(-calculateRecordSize(record));
            deleteRecord(dataKey);
            cancelAssociatedSchedulers(dataKey);
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
        checkIfLoaded();
        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record != null) {
            flush(dataKey);
            mapService.interceptRemove(name, record.getValue());
            oldValue = record.getValue();
            deleteRecord(dataKey);
            // reduce size
            updateSizeEstimator(-calculateRecordSize(record));
            removeIndex(dataKey);
            cancelAssociatedSchedulers(dataKey);
        }
        return oldValue;
    }

    public boolean remove(Data dataKey, Object testValue) {
        checkIfLoaded();
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
            deleteRecord(dataKey);
            // reduce size
            updateSizeEstimator(-calculateRecordSize(record));
            cancelAssociatedSchedulers(dataKey);
            removed = true;
        }
        return removed;
    }

    public Object get(Data dataKey) {
        checkIfLoaded();
        Record record = records.get(dataKey);
        Object value = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                value = mapContainer.getStore().load(mapService.toObject(dataKey));
                if (value != null) {
                    record = mapService.createRecord(name, dataKey, value, -1);
                    records.put(dataKey, record);
                    saveIndex(record);
                    updateSizeEstimator(calculateRecordSize(record));
                }
            }

        } else {
            accessRecord(record);
            value = record.getValue();
        }
        value = mapService.interceptGet(name, value);

        return value;
    }

    public MapEntrySet getAll(Set<Data> keySet) {
        checkIfLoaded();
        final MapEntrySet mapEntrySet = new MapEntrySet();
        Map<Object, Data> keyMapForLoader = null;
        if (mapContainer.getStore() != null) {
            keyMapForLoader = new HashMap<Object, Data>();
        }
        for (Data dataKey : keySet) {
            Record record = records.get(dataKey);
            if (record == null) {
                if (mapContainer.getStore() != null) {
                    keyMapForLoader.put(mapService.toObject(dataKey), dataKey);
                }
            } else {
                accessRecord(record);
                Object value = record.getValue();
                value = mapService.interceptGet(name, value);
                if (value != null) {
                    mapEntrySet.add(new AbstractMap.SimpleImmutableEntry(dataKey, mapService.toData(value)));
                }
            }
        }
        if (mapContainer.getStore() == null || keyMapForLoader.size() == 0) {
            return mapEntrySet;
        }
        final Map<Object, Object> loadedKeys = mapContainer.getStore().loadAll(keyMapForLoader.keySet());
        for (Map.Entry entry : loadedKeys.entrySet()) {
            final Object objectKey = entry.getKey();
            Object value = entry.getValue();
            Data dataKey = keyMapForLoader.get(objectKey);
            if (value != null) {
                Record record = mapService.createRecord(name, dataKey, value, -1);
                records.put(dataKey, record);
                saveIndex(record);
                updateSizeEstimator(calculateRecordSize(record));
            }
            value = mapService.interceptGet(name, value);
            if (value != null) {
                mapEntrySet.add(new AbstractMap.SimpleImmutableEntry(dataKey, mapService.toData(value)));
            }
        }
        return mapEntrySet;
    }

    public boolean containsKey(Data dataKey) {
        checkIfLoaded();
        Record record = records.get(dataKey);
        if (record == null) {
            if (mapContainer.getStore() != null) {
                Object value = mapContainer.getStore().load(mapService.toObject(dataKey));
                if (value != null) {
                    record = mapService.createRecord(name, dataKey, value, -1);
                    records.put(dataKey, record);
                    updateSizeEstimator(calculateRecordSize(record));
                }
            }
        }

        boolean contains = record != null;
        if (contains) {
            accessRecord(record);
        }
        return contains;
    }

    public void put(Map.Entry<Data, Object> entry) {
        checkIfLoaded();
        Data dataKey = entry.getKey();
        Object value = entry.getValue();
        Record record = records.get(dataKey);
        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            record = mapService.createRecord(name, dataKey, value, -1);
            mapStoreWrite(record, dataKey, value);
            records.put(dataKey, record);
            // increase size.
            updateSizeEstimator(calculateRecordSize(record));
            saveIndex(record);
        } else {
            final Object oldValue = record.getValue();
            value = mapService.interceptPut(name, oldValue, value);
            //check if putting the same value again.
            if (mapService.compare(name, oldValue, value)) {
                accessRecord(record);
            }//otherwise this is a full update operation.
            else {
                mapStoreWrite(record, dataKey, value);
                // if key exists before, first reduce size
                updateSizeEstimator(-calculateRecordSize(record));
                setRecordValue(record, value);
                // then increase size
                updateSizeEstimator(calculateRecordSize(record));
                saveIndex(record);
            }
        }

    }

    public Object put(Data dataKey, Object value, long ttl) {
        checkIfLoaded();
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
            updateSizeEstimator(calculateRecordSize(record));
            saveIndex(record);
        } else {
            oldValue = record.getValue();
            value = mapService.interceptPut(name, oldValue, value);
            //check if putting the same value again.
            if (mapService.compare(name, oldValue, value)) {
                accessRecord(record);
                updateTtl(record, ttl);
            }//otherwise this is a full update operation.
            else {
                mapStoreWrite(record, dataKey, value);
                // if key exists before, first reduce size
                updateSizeEstimator(-calculateRecordSize(record));
                setRecordValue(record, value);
                // then increase size.
                updateSizeEstimator(calculateRecordSize(record));
                updateTtl(record, ttl);
                saveIndex(record);
            }
        }
        return oldValue;
    }

    public boolean set(Data dataKey, Object value, long ttl) {
        checkIfLoaded();
        Record record = records.get(dataKey);
        boolean newRecord = false;
        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            record = mapService.createRecord(name, dataKey, value, ttl);
            mapStoreWrite(record, dataKey, value);
            records.put(dataKey, record);
            updateSizeEstimator(calculateRecordSize(record));
            newRecord = true;
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            mapStoreWrite(record, dataKey, value);
            // if key exists before, first reduce size
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value);
            // then increase size.
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);

        return newRecord;
    }

    public boolean merge(Data dataKey, EntryView mergingEntry, MapMergePolicy mergePolicy) {
        checkIfLoaded();
        Record record = records.get(dataKey);
        Object newValue = null;
        if (record == null) {
            newValue = mergingEntry.getValue();
            record = mapService.createRecord(name, dataKey, newValue, -1);
            mapStoreWrite(record, dataKey, newValue);
            records.put(dataKey, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            Object oldValue = record.getValue();
            EntryView existingEntry = new SimpleEntryView(mapService.toObject(record.getKey()), mapService.toObject(record.getValue()),
                    record.getStatistics(), record.getVersion());
            newValue = mergePolicy.merge(name, mergingEntry, existingEntry);
            if (newValue == null) { // existing entry will be removed
                deleteRecord(dataKey);
                removeIndex(dataKey);
                mapStoreDelete(record, dataKey);
                // reduce size.
                updateSizeEstimator(-calculateRecordSize(record));
                return true;
            }
            // same with the existing entry so no need to mapstore etc operations.
            if (mapService.compare(name, newValue, oldValue)) {
                return true;
            }
            mapStoreWrite(record, dataKey, newValue);
            updateSizeEstimator(-calculateRecordSize(record));
            recordFactory.setValue(record, newValue);
            updateSizeEstimator(calculateRecordSize(record));
        }
        saveIndex(record);
        return newValue != null;
    }

    public Object replace(Data dataKey, Object value) {
        checkIfLoaded();
        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record != null && record.getValue() != null) {
            oldValue = record.getValue();
            value = mapService.interceptPut(name, oldValue, value);
            mapStoreWrite(record, dataKey, value);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            return null;
        }
        saveIndex(record);

        return oldValue;
    }

    public boolean replace(Data dataKey, Object testValue, Object newValue) {
        checkIfLoaded();
        Record record = records.get(dataKey);
        if (record == null)
            return false;
        if (mapService.compare(name, record.getValue(), testValue)) {
            newValue = mapService.interceptPut(name, record.getValue(), newValue);
            mapStoreWrite(record, dataKey, newValue);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, newValue);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            return false;
        }
        saveIndex(record);

        return true;
    }

    public void putTransient(Data dataKey, Object value, long ttl) {
        checkIfLoaded();
        Record record = records.get(dataKey);
        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            record = mapService.createRecord(name, dataKey, value, ttl);
            records.put(dataKey, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value);
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);
    }

    public void putFromLoad(Data dataKey, Object value, long ttl) {
        Record record = records.get(dataKey);
        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            record = mapService.createRecord(name, dataKey, value, ttl);
            records.put(dataKey, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value);
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);
    }

    public boolean tryPut(Data dataKey, Object value, long ttl) {
        checkIfLoaded();
        Record record = records.get(dataKey);
        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            record = mapService.createRecord(name, dataKey, value, ttl);
            mapStoreWrite(record, dataKey, value);
            records.put(dataKey, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            mapStoreWrite(record, dataKey, value);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value);
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);

        return true;
    }

    public Object putIfAbsent(Data dataKey, Object value, long ttl) {
        checkIfLoaded();
        Record record = records.get(dataKey);
        Object oldValue = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                oldValue = mapContainer.getStore().load(mapService.toObject(dataKey));
                if (oldValue != null) {
                    record = mapService.createRecord(name, dataKey, oldValue, -1);
                    records.put(dataKey, record);
                    updateSizeEstimator(calculateRecordSize(record));
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
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);

        return oldValue;
    }

    private void accessRecord(Record record) {
        record.onAccess();
        final int maxIdleSeconds = mapContainer.getMapConfig().getMaxIdleSeconds();
        if (maxIdleSeconds > 0) {
            mapService.scheduleIdleEviction(name, record.getKey(), TimeUnit.SECONDS.toMillis(maxIdleSeconds));
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

    public SizeEstimator getSizeEstimator() {
        return sizeEstimator;
    }

    private void updateTtl(Record record, long ttl) {
        if (ttl > 0) {
            mapService.scheduleTtlEviction(name, record, ttl);
        } else if (ttl == 0) {
            mapContainer.getTtlEvictionScheduler().cancel(record.getKey());
        }
    }

    private void updateSizeEstimator(long recordSize) {
        sizeEstimator.add(recordSize);
    }

    private long calculateRecordSize(Record record) {
        return sizeEstimator.getCost(record);
    }

    private void resetSizeEstimator() {
        sizeEstimator.reset();
    }

    private void setRecordValue(Record record, Object value) {
        accessRecord(record);
        record.onUpdate();
        recordFactory.setValue(record, value);
    }

    private void cancelAssociatedSchedulers(Data key) {
        mapContainer.getIdleEvictionScheduler().cancel(key);
        mapContainer.getTtlEvictionScheduler().cancel(key);
    }

    private void cancelAssociatedSchedulers(Set<Data> keySet) {
        if(keySet == null || keySet.isEmpty() ) return;

        for (Data key : keySet ){
            cancelAssociatedSchedulers(key);
        }
    }

    private class MapLoadAllTask implements Runnable {
        private Map<Data, Object> keys;

        private MapLoadAllTask(Map<Data, Object> keys) {
            this.keys = keys;
        }

        public void run() {
            final NodeEngine nodeEngine = mapService.getNodeEngine();

            Map values = mapContainer.getStore().loadAll(keys.values());

            MapEntrySet entrySet = new MapEntrySet();
            for (Data dataKey : keys.keySet()) {
                Object key = keys.get(dataKey);
                Data dataValue = mapService.toData(values.get(key));
                entrySet.add(dataKey, dataValue);
            }
            PutAllOperation operation = new PutAllOperation(name, entrySet, true);
            operation.setNodeEngine(nodeEngine);
            operation.setResponseHandler(new ResponseHandler() {
                @Override
                public void sendResponse(Object obj) {
                    if (!(obj instanceof Exception)) {
                        loaded.set(true);
                    } else {
                        Exception e = (Exception) obj;
                        nodeEngine.getLogger(RecordStore.class).finest(e.getMessage());
                    }
                }

                public boolean isLocal() {
                    return true;
                }
            });
            operation.setPartitionId(partitionId);
            OperationAccessor.setCallerAddress(operation, nodeEngine.getThisAddress());
            operation.setServiceName(MapService.SERVICE_NAME);
            nodeEngine.getOperationService().executeOperation(operation);
        }
    }
}
