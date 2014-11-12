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

package com.hazelcast.map.impl;


import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockStore;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.util.ExceptionUtil;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.map.impl.ExpirationTimeSetter.updateExpiryTime;

/**
 * Default implementation of record-store.
 */
public class DefaultRecordStore extends AbstractEvictableRecordStore implements RecordStore {

    private final LockStore lockStore;
    private final MapDataStore<Data, Object> mapDataStore;
    private final RecordStoreLoader recordStoreLoader;

    public DefaultRecordStore(MapContainer mapContainer, int partitionId) {
        super(mapContainer, partitionId);
        this.lockStore = createLockStore();
        this.mapDataStore
                = mapContainer.getMapStoreContext().getMapStoreManager().getMapDataStore(partitionId);
        this.recordStoreLoader = createRecordStoreLoader();
        this.recordStoreLoader.loadAllKeys();
    }

    @Override
    public boolean isLoaded() {
        return recordStoreLoader.isLoaded();
    }

    @Override
    public void setLoaded(boolean loaded) {
        recordStoreLoader.setLoaded(loaded);
    }

    @Override
    public void checkIfLoaded() {
        Throwable throwable = null;
        final RecordStoreLoader recordStoreLoader = this.recordStoreLoader;
        final Throwable exception = recordStoreLoader.getExceptionOrNull();
        if (exception == null && !recordStoreLoader.isLoaded()) {
            throwable = new RetryableHazelcastException("Map "
                    + getName() + " is still loading data from external store");
        } else if (exception != null) {
            throwable = exception;
        }
        if (throwable != null) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    @Override
    public void flush() {
        final long now = getNow();
        final Collection<Data> processedKeys = mapDataStore.flush();
        for (Data key : processedKeys) {
            final Record record = getRecordOrNull(key, now, false);
            if (record != null) {
                record.onStore();
            }
        }
    }

    @Override
    public Record getRecord(Data key) {
        return records.get(key);
    }

    @Override
    public void putRecord(Data key, Record record) {
        final Record existingRecord = records.put(key, record);
        updateSizeEstimator(-calculateRecordHeapCost(existingRecord));
        updateSizeEstimator(calculateRecordHeapCost(record));
    }

    @Override
    public Record putBackup(Data key, Object value) {
        return putBackup(key, value, DEFAULT_TTL);
    }

    /**
     * @param key   the key to be processed.
     * @param value the value to be processed.
     * @param ttl   milliseconds. Check out {@link com.hazelcast.map.impl.proxy.MapProxySupport#putInternal}
     * @return previous record if exists otherwise null.
     */
    @Override
    public Record putBackup(Data key, Object value, long ttl) {
        final long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecordOrNull(key, now, true);
        if (record == null) {
            record = createRecord(key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordHeapCost(record));
        } else {
            updateSizeEstimator(-calculateRecordHeapCost(record));
            updateRecord(record, value, now);
            updateSizeEstimator(calculateRecordHeapCost(record));
        }
        mapDataStore.addBackup(key, value, now);
        return record;
    }

    @Override
    public Iterator<Record> iterator() {
        return new ReadOnlyRecordIterator(records.values());
    }

    @Override
    public Iterator<Record> iterator(long now, boolean backup) {
        return new ReadOnlyRecordIterator(records.values(), now, backup);
    }

    @Override
    public Iterator<Record> loadAwareIterator(long now, boolean backup) {
        checkIfLoaded();
        return iterator(now, backup);
    }

    @Override
    public Map<Data, Record> getRecordMap() {
        return records;
    }


    @Override
    public void clearPartition() {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            final DefaultObjectNamespace namespace = new DefaultObjectNamespace(MapService.SERVICE_NAME, name);
            lockService.clearLockStore(partitionId, namespace);
        }
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            for (Data key : records.keySet()) {
                indexService.removeEntryIndex(key);
            }
        }
        clearRecordsMap(Collections.<Data, Record>emptyMap());
        resetSizeEstimator();
        resetAccessSequenceNumber();
        mapDataStore.reset();
    }

    /**
     * Size may not give precise size at a specific moment
     * due to the expiration logic. But eventually, it should be correct.
     *
     * @return record store size.
     */
    @Override
    public int size() {
        // do not add checkIfLoaded(), size() is also used internally
        return records.size();
    }

    @Override
    public boolean isEmpty() {
        checkIfLoaded();
        return records.isEmpty();
    }


    @Override
    public boolean containsValue(Object value) {
        checkIfLoaded();
        final long now = getNow();
        for (Record record : records.values()) {
            if (getOrNullIfExpired(record, now, false) == null) {
                continue;
            }
            if (mapServiceContext.compare(name, value, record.getValue())) {
                return true;
            }
        }
        postReadCleanUp(now, false);
        return false;
    }

    @Override
    public boolean txnLock(Data key, String caller, long threadId, long ttl) {
        checkIfLoaded();
        return lockStore != null && lockStore.txnLock(key, caller, threadId, ttl);
    }

    @Override
    public boolean extendLock(Data key, String caller, long threadId, long ttl) {
        checkIfLoaded();
        return lockStore != null && lockStore.extendLeaseTime(key, caller, threadId, ttl);
    }

    @Override
    public boolean unlock(Data key, String caller, long threadId) {
        checkIfLoaded();
        return lockStore != null && lockStore.unlock(key, caller, threadId);
    }

    @Override
    public boolean forceUnlock(Data dataKey) {
        return lockStore != null && lockStore.forceUnlock(dataKey);
    }

    @Override
    public boolean isLocked(Data dataKey) {
        return lockStore != null && lockStore.isLocked(dataKey);
    }

    @Override
    public boolean isTransactionallyLocked(Data key) {
        return lockStore != null && lockStore.isTransactionallyLocked(key);
    }

    @Override
    public boolean canAcquireLock(Data key, String caller, long threadId) {
        return lockStore == null || lockStore.canAcquireLock(key, caller, threadId);
    }

    @Override
    public String getLockOwnerInfo(Data key) {
        return lockStore != null ? lockStore.getOwnerInfo(key) : null;
    }

    @Override
    public Set<Map.Entry<Data, Data>> entrySetData() {
        checkIfLoaded();
        final long now = getNow();

        final ConcurrentMap<Data, Record> records = this.records;
        final Collection<Record> values = records.values();
        Map<Data, Data> tempMap = null;
        for (Record record : values) {
            record = getOrNullIfExpired(record, now, false);
            if (record == null) {
                continue;
            }
            if (tempMap == null) {
                tempMap = new HashMap<Data, Data>();
            }
            final Data key = record.getKey();
            final Data value = toData(record.getValue());
            tempMap.put(key, value);
        }
        if (tempMap == null) {
            return Collections.emptySet();
        }
        return tempMap.entrySet();
    }

    @Override
    public Map.Entry<Data, Object> getMapEntry(Data key, long now) {
        checkIfLoaded();

        Record record = getRecordOrNull(key, now, false);
        if (record == null) {
            record = loadRecordOrNull(key, false);
        } else {
            accessRecord(record);
        }
        final Object value = record != null ? record.getValue() : null;
        return new AbstractMap.SimpleImmutableEntry<Data, Object>(key, value);
    }

    private Record loadRecordOrNull(Data key, boolean backup) {
        Record record = null;
        final Object value = mapDataStore.load(key);
        if (value != null) {
            record = createRecord(key, value, getNow());
            records.put(key, record);
            if (!backup) {
                saveIndex(record);
            }
            updateSizeEstimator(calculateRecordHeapCost(record));
        }
        return record;
    }

    @Override
    public Set<Data> keySet() {
        checkIfLoaded();
        final long now = getNow();

        final ConcurrentMap<Data, Record> records = this.records;
        final Collection<Record> values = records.values();
        Set<Data> keySet = null;
        for (Record record : values) {
            record = getOrNullIfExpired(record, now, false);
            if (record == null) {
                continue;
            }
            if (keySet == null) {
                keySet = new HashSet<Data>();
            }
            keySet.add(record.getKey());
        }

        if (keySet == null) {
            return Collections.emptySet();
        }
        return keySet;
    }

    @Override
    public Collection<Data> valuesData() {
        checkIfLoaded();
        final long now = getNow();

        final ConcurrentMap<Data, Record> records = this.records;
        final Collection<Record> values = records.values();
        List<Data> dataValueList = null;
        for (Record record : values) {
            record = getOrNullIfExpired(record, now, false);
            if (record == null) {
                continue;
            }
            if (dataValueList == null) {
                dataValueList = new ArrayList<Data>();
            }
            final Data dataValue = toData(record.getValue());
            dataValueList.add(dataValue);
        }

        if (dataValueList == null) {
            return Collections.emptyList();
        }
        return dataValueList;
    }

    @Override
    public int clear() {
        checkIfLoaded();
        resetSizeEstimator();
        final Collection<Data> lockedKeys = lockStore != null ? lockStore.getLockedKeys() : Collections.<Data>emptySet();
        final Map<Data, Record> lockedRecords = new HashMap<Data, Record>(lockedKeys.size());
        // Locked records should not be removed!
        for (Data key : lockedKeys) {
            Record record = records.get(key);
            if (record != null) {
                lockedRecords.put(key, record);
                updateSizeEstimator(calculateRecordHeapCost(record));
            }
        }
        final Set<Data> keysToDelete = records.keySet();
        keysToDelete.removeAll(lockedRecords.keySet());

        mapDataStore.removeAll(keysToDelete);

        final int numOfClearedEntries = keysToDelete.size();
        removeIndex(keysToDelete);

        clearRecordsMap(lockedRecords);
        resetAccessSequenceNumber();
        mapDataStore.reset();
        return numOfClearedEntries;
    }

    @Override
    public void reset() {
        checkIfLoaded();

        clearRecordsMap(Collections.<Data, Record>emptyMap());
        resetSizeEstimator();
        resetAccessSequenceNumber();
        mapDataStore.reset();
    }


    @Override
    public Object evict(Data key, boolean backup) {
        checkIfLoaded();

        return evictInternal(key, backup);
    }

    @Override
    Object evictInternal(Data key, boolean backup) {
        Record record = records.get(key);
        Object value = null;
        if (record != null) {
            value = record.getValue();
            final long lastUpdateTime = record.getLastUpdateTime();
            mapDataStore.flush(key, value, lastUpdateTime, backup);
            if (!backup) {
                mapServiceContext.interceptRemove(name, value);
            }
            updateSizeEstimator(-calculateRecordHeapCost(record));
            deleteRecord(key);
            removeIndex(key);
        }
        return value;
    }

    @Override
    public int evictAll(boolean backup) {
        checkIfLoaded();
        final int sizeBeforeEviction = size();
        resetSizeEstimator();
        resetAccessSequenceNumber();

        Map<Data, Record> recordsToPreserve = getLockedRecords();
        updateSizeEstimator(calculateRecordHeapCost(recordsToPreserve.values()));

        flush(recordsToPreserve, backup);
        removeIndexByPreservingKeys(records.keySet(), recordsToPreserve.keySet());
        clearRecordsMap(recordsToPreserve);

        return sizeBeforeEviction - recordsToPreserve.size();
    }

    /**
     * Flushes evicted records to map store.
     *
     * @param excludeRecords Records which should not be flushed.
     * @param backup         <code>true</code> if backup, false otherwise.
     */
    private void flush(Map<Data, Record> excludeRecords, boolean backup) {
        final Collection<Record> values = records.values();
        final MapDataStore<Data, Object> mapDataStore = this.mapDataStore;
        for (Record record : values) {
            if (excludeRecords == null || !excludeRecords.containsKey(record.getKey())) {
                final Data key = record.getKey();
                final long lastUpdateTime = record.getLastUpdateTime();
                mapDataStore.flush(key, record.getValue(), lastUpdateTime, backup);
            }
        }
    }

    /**
     * Returns locked records.
     *
     * @return map of locked records.
     */
    private Map<Data, Record> getLockedRecords() {
        if (lockStore == null) {
            return Collections.emptyMap();
        }
        final Collection<Data> lockedKeys = lockStore.getLockedKeys();
        if (lockedKeys.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<Data, Record> lockedRecords = new HashMap<Data, Record>(lockedKeys.size());
        // Locked records should not be removed!
        for (Data key : lockedKeys) {
            final Record record = records.get(key);
            if (record != null) {
                lockedRecords.put(key, record);
            }
        }
        return lockedRecords;
    }

    @Override
    public void removeBackup(Data key) {
        final long now = getNow();

        final Record record = getRecordOrNull(key, now, true);
        if (record == null) {
            return;
        }
        // reduce size
        updateSizeEstimator(-calculateRecordHeapCost(record));
        deleteRecord(key);
        mapDataStore.removeBackup(key, now);
    }

    @Override
    public Object remove(Data key) {
        checkIfLoaded();
        final long now = getNow();

        final Record record = getRecordOrNull(key, now, false);
        Object oldValue;
        if (record == null) {
            oldValue = mapDataStore.load(key);
            if (oldValue != null) {
                removeIndex(key);
                mapDataStore.remove(key, now);
            }
        } else {
            oldValue = removeRecord(key, record, now);
        }
        return oldValue;
    }

    @Override
    public boolean remove(Data key, Object testValue) {
        checkIfLoaded();
        final long now = getNow();

        final Record record = getRecordOrNull(key, now, false);
        Object oldValue;
        boolean removed = false;
        if (record == null) {
            oldValue = mapDataStore.load(key);
            if (oldValue == null) {
                return false;
            }
        } else {
            oldValue = record.getValue();
        }
        if (mapServiceContext.compare(name, testValue, oldValue)) {
            mapServiceContext.interceptRemove(name, oldValue);
            removeIndex(key);
            mapDataStore.remove(key, now);
            onStore(record);
            // reduce size
            updateSizeEstimator(-calculateRecordHeapCost(record));
            deleteRecord(key);
            removed = true;
        }
        return removed;
    }

    @Override
    public boolean delete(Data key) {
        checkIfLoaded();
        final long now = getNow();

        final Record record = getRecordOrNull(key, now, false);
        if (record == null) {
            removeIndex(key);
            mapDataStore.remove(key, now);
        } else {
            return removeRecord(key, record, now) != null;
        }
        return false;
    }

    @Override
    public Object get(Data key, boolean backup) {
        checkIfLoaded();
        final long now = getNow();

        Record record = getRecordOrNull(key, now, backup);
        if (record == null) {
            record = loadRecordOrNull(key, backup);
        } else {
            accessRecord(record, now);
        }
        Object value = record == null ? null : record.getValue();
        value = mapServiceContext.interceptGet(name, value);

        postReadCleanUp(now, false);
        return value;
    }

    @Override
    public Data readBackupData(Data key) {
        final long now = getNow();

        final Record record = getRecord(key);
        // expiration has delay on backups, but reading backup data should not be affected by this delay.
        // this is the reason why we are passing `false` to isExpired() method.
        final boolean expired = isExpired(record, now, false);
        if (expired) {
            return null;
        }
        final MapServiceContext mapServiceContext = this.mapServiceContext;
        final Object value = record.getValue();
        mapServiceContext.interceptAfterGet(name, value);
        // this serialization step is needed not to expose the object, see issue 1292
        return mapServiceContext.toData(value);
    }

    @Override
    public MapEntrySet getAll(Set<Data> keys) {
        checkIfLoaded();
        final long now = getNow();

        final MapEntrySet mapEntrySet = new MapEntrySet();

        final Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            final Data key = iterator.next();
            final Record record = getRecordOrNull(key, now, false);
            if (record != null) {
                addMapEntrySet(record.getKey(), record.getValue(), mapEntrySet);
                accessRecord(record);
                iterator.remove();
            }
        }
        final Map entriesLoaded = mapDataStore.loadAll(keys);
        addMapEntrySet(entriesLoaded, mapEntrySet);
        postReadCleanUp(now, false);
        return mapEntrySet;
    }

    private void addMapEntrySet(Object key, Object value, MapEntrySet mapEntrySet) {
        if (key == null || value == null) {
            return;
        }
        final Data dataKey = mapServiceContext.toData(key);
        final Data dataValue = mapServiceContext.toData(value);
        mapEntrySet.add(dataKey, dataValue);
    }


    private void addMapEntrySet(Map<Object, Object> entries, MapEntrySet mapEntrySet) {
        for (Map.Entry<Object, Object> entry : entries.entrySet()) {
            addMapEntrySet(entry.getKey(), entry.getValue(), mapEntrySet);
        }
    }


    @Override
    public boolean containsKey(Data key) {
        checkIfLoaded();
        final long now = getNow();

        Record record = getRecordOrNull(key, now, false);
        if (record == null) {
            Object value = mapDataStore.load(key);
            if (value != null) {
                record = createRecord(key, value, now);
                records.put(key, record);
                updateSizeEstimator(calculateRecordHeapCost(record));
            }
        }
        boolean contains = record != null;
        if (contains) {
            accessRecord(record, now);
        }

        postReadCleanUp(now, false);
        return contains;
    }

    @Override
    public void put(Map.Entry<Data, Object> entry) {
        checkIfLoaded();
        final long now = getNow();

        Data key = entry.getKey();
        Object value = entry.getValue();
        Record record = getRecordOrNull(key, now, false);
        if (record == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            value = mapDataStore.add(key, value, now);
            record = createRecord(key, value, now);
            records.put(key, record);
            // increase size.
            updateSizeEstimator(calculateRecordHeapCost(record));
            saveIndex(record);
        } else {
            final Object oldValue = record.getValue();
            value = mapServiceContext.interceptPut(name, oldValue, value);
            value = mapDataStore.add(key, value, now);
            onStore(record);
            // if key exists before, first reduce size
            updateSizeEstimator(-calculateRecordHeapCost(record));
            updateRecord(record, value, now);
            // then increase size
            updateSizeEstimator(calculateRecordHeapCost(record));
            saveIndex(record);
        }
    }

    @Override
    public Object put(Data key, Object value, long ttl) {
        checkIfLoaded();
        final long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecordOrNull(key, now, false);
        Object oldValue;
        if (record == null) {
            oldValue = mapDataStore.load(key);
            value = mapServiceContext.interceptPut(name, null, value);
            value = mapDataStore.add(key, value, now);
            record = createRecord(key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordHeapCost(record));
            saveIndex(record);
        } else {
            oldValue = record.getValue();
            value = mapServiceContext.interceptPut(name, oldValue, value);
            value = mapDataStore.add(key, value, now);
            onStore(record);
            // if key exists before, first reduce size
            updateSizeEstimator(-calculateRecordHeapCost(record));
            updateRecord(record, value, now);
            // then increase size.
            updateSizeEstimator(calculateRecordHeapCost(record));
            updateExpiryTime(record, ttl, mapContainer.getMaxIdleMillis());
            saveIndex(record);
        }
        return oldValue;
    }

    @Override
    public boolean set(Data key, Object value, long ttl) {
        checkIfLoaded();
        final long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecordOrNull(key, now, false);
        boolean newRecord = false;
        if (record == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            value = mapDataStore.add(key, value, now);
            record = createRecord(key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordHeapCost(record));
            newRecord = true;
        } else {
            value = mapServiceContext.interceptPut(name, record.getValue(), value);
            value = mapDataStore.add(key, value, now);
            onStore(record);
            // if key exists before, first reduce size
            updateSizeEstimator(-calculateRecordHeapCost(record));
            updateRecord(record, value, now);
            // then increase size.
            updateSizeEstimator(calculateRecordHeapCost(record));
            updateExpiryTime(record, ttl, mapContainer.getMaxIdleMillis());
        }
        saveIndex(record);
        return newRecord;
    }

    @Override
    public boolean merge(Data key, EntryView mergingEntry, MapMergePolicy mergePolicy) {
        checkIfLoaded();
        final long now = getNow();

        Record record = getRecordOrNull(key, now, false);
        Object newValue;
        if (record == null) {
            final Object notExistingKey = mapServiceContext.toObject(key);
            final EntryView nullEntryView = EntryViews.createNullEntryView(notExistingKey);
            newValue = mergePolicy.merge(name, mergingEntry, nullEntryView);
            if (newValue == null) {
                return false;
            }
            newValue = mapDataStore.add(key, newValue, now);
            record = createRecord(key, newValue, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordHeapCost(record));
        } else {
            Object oldValue = record.getValue();
            EntryView existingEntry = EntryViews.createLazyEntryView(record.getKey(), record.getValue(),
                    record, serializationService, mergePolicy);
            newValue = mergePolicy.merge(name, mergingEntry, existingEntry);
            // existing entry will be removed
            if (newValue == null) {
                removeIndex(key);
                mapDataStore.remove(key, now);
                onStore(record);
                // reduce size.
                updateSizeEstimator(-calculateRecordHeapCost(record));
                //remove from map & invalidate.
                deleteRecord(key);
                return true;
            }
            // same with the existing entry so no need to map-store etc operations.
            if (mapServiceContext.compare(name, newValue, oldValue)) {
                return true;
            }
            newValue = mapDataStore.add(key, newValue, now);
            onStore(record);
            updateSizeEstimator(-calculateRecordHeapCost(record));
            recordFactory.setValue(record, newValue);
            updateSizeEstimator(calculateRecordHeapCost(record));
        }
        saveIndex(record);
        return newValue != null;
    }

    // TODO why does not replace method load data from map store if currently not available in memory.
    @Override
    public Object replace(Data key, Object update) {
        checkIfLoaded();
        final long now = getNow();

        final Record record = getRecordOrNull(key, now, false);
        if (record == null || record.getValue() == null) {
            return null;
        }
        final Object oldValue = record.getValue();
        update = mapServiceContext.interceptPut(name, oldValue, update);
        update = mapDataStore.add(key, update, now);
        onStore(record);
        updateSizeEstimator(-calculateRecordHeapCost(record));
        updateRecord(record, update, now);
        updateSizeEstimator(calculateRecordHeapCost(record));
        saveIndex(record);
        return oldValue;
    }

    @Override
    public boolean replace(Data key, Object expect, Object update) {
        checkIfLoaded();
        final long now = getNow();

        final Record record = getRecordOrNull(key, now, false);
        if (record == null) {
            return false;
        }
        final MapServiceContext mapServiceContext = this.mapServiceContext;
        final Object current = record.getValue();
        final String mapName = this.name;
        if (!mapServiceContext.compare(mapName, current, expect)) {
            return false;
        }
        update = mapServiceContext.interceptPut(mapName, current, update);
        update = mapDataStore.add(key, update, now);
        onStore(record);
        updateSizeEstimator(-calculateRecordHeapCost(record));
        updateRecord(record, update, now);
        updateSizeEstimator(calculateRecordHeapCost(record));
        saveIndex(record);
        return true;
    }

    @Override
    public void putTransient(Data key, Object value, long ttl) {
        checkIfLoaded();
        final long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecordOrNull(key, now, false);
        if (record == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            record = createRecord(key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordHeapCost(record));
        } else {
            value = mapServiceContext.interceptPut(name, record.getValue(), value);
            updateSizeEstimator(-calculateRecordHeapCost(record));
            updateRecord(record, value, now);
            updateSizeEstimator(calculateRecordHeapCost(record));
            updateExpiryTime(record, ttl, mapContainer.getMaxIdleMillis());
        }
        saveIndex(record);
        mapDataStore.addTransient(key, now);
    }

    @Override
    public Object putFromLoad(Data key, Object value) {
        return putFromLoad(key, value, DEFAULT_TTL);
    }

    @Override
    public Object putFromLoad(Data key, Object value, long ttl) {
        final long now = getNow();

        if (shouldEvict(now)) {
            return null;
        }
        markRecordStoreExpirable(ttl);

        Record record = getRecordOrNull(key, now, false);
        Object oldValue = null;
        if (record == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            record = createRecord(key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordHeapCost(record));
        } else {
            oldValue = record.getValue();
            value = mapServiceContext.interceptPut(name, record.getValue(), value);
            updateSizeEstimator(-calculateRecordHeapCost(record));
            updateRecord(record, value, now);
            updateSizeEstimator(calculateRecordHeapCost(record));
            updateExpiryTime(record, ttl, mapContainer.getMaxIdleMillis());
        }
        saveIndex(record);

        return oldValue;
    }

    @Override
    public boolean tryPut(Data key, Object value, long ttl) {
        checkIfLoaded();
        final long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecordOrNull(key, now, false);
        if (record == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            value = mapDataStore.add(key, value, now);
            record = createRecord(key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordHeapCost(record));
        } else {
            value = mapServiceContext.interceptPut(name, record.getValue(), value);
            value = mapDataStore.add(key, value, now);
            onStore(record);
            updateSizeEstimator(-calculateRecordHeapCost(record));
            updateRecord(record, value, now);
            updateSizeEstimator(calculateRecordHeapCost(record));
            updateExpiryTime(record, ttl, mapContainer.getMaxIdleMillis());
        }
        saveIndex(record);
        return true;
    }

    @Override
    public Object putIfAbsent(Data key, Object value, long ttl) {
        checkIfLoaded();
        final long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecordOrNull(key, now, false);
        Object oldValue;
        if (record == null) {
            oldValue = mapDataStore.load(key);
            if (oldValue != null) {
                record = createRecord(key, oldValue, now);
                records.put(key, record);
                updateSizeEstimator(calculateRecordHeapCost(record));
            }
        } else {
            accessRecord(record, now);
            oldValue = record.getValue();
        }
        if (oldValue == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            value = mapDataStore.add(key, value, now);
            onStore(record);
            record = createRecord(key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordHeapCost(record));
            updateExpiryTime(record, ttl, mapContainer.getMaxIdleMillis());
        }
        saveIndex(record);
        return oldValue;
    }


    @Override
    public void loadAllFromStore(List<Data> keys, boolean replaceExistingValues) {
        if (keys.isEmpty()) {
            return;
        }
        recordStoreLoader.loadKeys(keys, replaceExistingValues);
    }

    @Override
    public MapDataStore<Data, Object> getMapDataStore() {
        return mapDataStore;
    }

    private Object removeRecord(Data key, Record record, long now) {
        Object oldValue = record.getValue();
        oldValue = mapServiceContext.interceptRemove(name, oldValue);
        if (oldValue != null) {
            removeIndex(key);
            mapDataStore.remove(key, now);
            onStore(record);
        }
        // reduce size
        updateSizeEstimator(-calculateRecordHeapCost(record));
        deleteRecord(key);
        return oldValue;
    }

    @Override
    public Record getRecordOrNull(Data key) {
        final long now = getNow();

        return getRecordOrNull(key, now, false);
    }

    private Record getRecordOrNull(Data key, long now, boolean backup) {
        Record record = records.get(key);
        if (record == null) {
            return null;
        }
        return getOrNullIfExpired(record, now, backup);
    }

    private void deleteRecord(Data key) {
        Record record = records.remove(key);
        if (record != null) {
            record.invalidate();
        }
    }
}
