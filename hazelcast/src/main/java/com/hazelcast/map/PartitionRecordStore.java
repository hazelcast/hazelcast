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
import com.hazelcast.map.operation.PutAllOperation;
import com.hazelcast.map.record.DataRecord;
import com.hazelcast.map.record.ObjectRecord;
import com.hazelcast.map.record.Record;
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
import java.util.concurrent.atomic.AtomicBoolean;

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
    final AtomicBoolean loaded = new AtomicBoolean(false);

    public PartitionRecordStore(String name, PartitionContainer partitionContainer) {
        this.name = name;
        this.partitionContainer = partitionContainer;
        this.mapService = partitionContainer.getMapService();
        this.mapContainer = mapService.getMapContainer(name);
        NodeEngine nodeEngine = mapService.getNodeEngine();
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        int partitionId = partitionContainer.getPartitionId();
        this.lockStore = lockService == null ? null :
                lockService.createLockStore(partitionId, new DefaultObjectNamespace(MapService.SERVICE_NAME, name));

        if (nodeEngine.getThisAddress().equals(nodeEngine.getPartitionService().getPartitionOwner(partitionId))) {
            if (mapContainer.getStore() != null && !loaded.get()) {
                Map<Data, Object> loadedKeys = mapContainer.getInitialKeys();
                if (loadedKeys != null && !loadedKeys.isEmpty()) {
                    Map<Data, Object> partitionKeys = new HashMap<Data, Object>();
                    for (Data data : loadedKeys.keySet()) {
                        if (partitionId == nodeEngine.getPartitionService().getPartitionId(data)) {
                            partitionKeys.put(data, loadedKeys.remove(data));
                        }
                    }
                    try {
                        nodeEngine.getExecutionService().submit("hz:map-load", new MapLoadAllTask(partitionKeys));
                    } catch (Throwable t) {
                        ExceptionUtil.rethrow(t);
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
            ExceptionUtil.rethrow(new RetryableHazelcastException("Map is not ready!!!"));
        }
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

    public Map<Data, Record> getRecords() {
        return records;
    }

    void clear() {
        clear(false);
    }

    void clear(boolean force) {
        if (!force) {
            checkIfLoaded();
        }
        final LockService lockService = mapService.getNodeEngine().getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            lockService.clearLockStore(partitionContainer.getPartitionId(), new DefaultObjectNamespace(MapService.SERVICE_NAME, name));
        }
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            for (Data key : records.keySet()) {
                indexService.removeEntryIndex(key);
            }
        }
        records.clear();
    }

    public int size() {
        checkIfLoaded();
        int size = records.size();
        return size;
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

    public void removeAll() {
        checkIfLoaded();
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
        Set<Object> keysObject = new HashSet<Object>();
        for (Data key : keysToDelete) {
            // todo ea have a clear(Keys) method for optimizations
            removeIndex(key);
            keysObject.add(mapService.toObject(key));
        }

        if (store != null) {
            store.deleteAll(keysObject);
            toBeRemovedKeys.clear();
        }

        records.clear();
        records.putAll(lockRecords);
    }

    public void reset() {
        checkIfLoaded();
        records.clear();
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
        checkIfLoaded();
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
            records.remove(dataKey);
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
                }
                // below is an optimization. if the record does not exist the next get will return null without looking at mapStore.
                if (value == null) {
                    int ttlForNull = mapService.getNodeEngine().getGroupProperties().CACHED_NULL_TTL_SECONDS.getInteger();
                    if (ttlForNull > 0) {
                        record = mapService.createRecord(name, dataKey, null, ttlForNull * 1000);
                        records.put(dataKey, record);
                    }
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
        final Map<Object, Object> loaded = mapContainer.getStore().loadAll(keyMapForLoader.keySet());
        for (Map.Entry entry : loaded.entrySet()) {
            final Object objectKey = entry.getKey();
            Object value = entry.getValue();
            Data dataKey = keyMapForLoader.get(objectKey);
            if (value != null) {
                Record record = mapService.createRecord(name, dataKey, value, -1);
                records.put(dataKey, record);
                saveIndex(record);
            }
            // below is an optimization. if the record does not exist the next get will return null without looking at mapStore.
            else {
                int ttlForNull = mapService.getNodeEngine().getGroupProperties().CACHED_NULL_TTL_SECONDS.getInteger();
                if (ttlForNull > 0) {
                    Record record = mapService.createRecord(name, dataKey, null, ttlForNull*1000);
                    records.put(dataKey, record);
                }
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
                }
            }
        }

        // because of a get optimization (see above), there may be a record with a null value,
        // which means map-store returned null while loading the key.
        boolean contains = record != null && record.getValue() != null;
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
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            mapStoreWrite(record, dataKey, value);
            setRecordValue(record, value);
        }
        saveIndex(record);
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
        checkIfLoaded();
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
        checkIfLoaded();
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
        checkIfLoaded();
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
        checkIfLoaded();
        Record record = records.get(dataKey);
        if (record == null)
            return false;
        if (mapService.compare(name, record.getValue(), testValue)) {
            newValue = mapService.interceptPut(name, record.getValue(), newValue);
            mapStoreWrite(record, dataKey, newValue);
            setRecordValue(record, newValue);
        }
        else
        {
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
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            setRecordValue(record, value);
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
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            setRecordValue(record, value);
            updateTtl(record, ttl);
        }
        saveIndex(record);
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
        checkIfLoaded();
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

    private class MapLoadAllTask implements Runnable {
        private Map<Data, Object> keys;

        private MapLoadAllTask(Map<Data, Object> keys) {
            this.keys = keys;
        }

        public void run() {
            NodeEngine nodeEngine = mapService.getNodeEngine();

            int partitionId = partitionContainer.getPartitionId();
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
                        e.printStackTrace();
                    }
                }
            });
            operation.setPartitionId(partitionId);
            OperationAccessor.setCallerAddress(operation, nodeEngine.getThisAddress());
            operation.setServiceName(MapService.SERVICE_NAME);
            nodeEngine.getOperationService().executeOperation(operation);
        }
    }


}
