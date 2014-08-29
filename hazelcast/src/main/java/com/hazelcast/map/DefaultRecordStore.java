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
import com.hazelcast.map.mapstore.MapDataStore;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.record.Record;
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
                = mapContainer.getMapStoreManager().getMapDataStore(partitionId);
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
            throwable = new RetryableHazelcastException("Map is not ready!!!");
        } else if (exception != null) {
            throwable = exception;
        }
        if (throwable != null) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    @Override
    public void flush() {
        final Collection<Data> processedKeys = mapDataStore.flush();
        for (Data key : processedKeys) {
            final Record record = getRecord(key, false);
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
        final long now = getNow();
        final Record existingRecord = records.put(key, record);
        updateSizeEstimator(-calculateRecordHeapCost(existingRecord));
        updateSizeEstimator(calculateRecordHeapCost(record));
        evictEntries(now, true);
    }

    @Override
    public Record putBackup(Data key, Object value) {
        return putBackup(key, value, DEFAULT_TTL);
    }

    /**
     * @param key   the key to be processed.
     * @param value the value to be processed.
     * @param ttl   milliseconds. Check out {@link com.hazelcast.map.proxy.MapProxySupport#putInternal}
     * @return previous record if exists otherwise null.
     */
    @Override
    public Record putBackup(Data key, Object value, long ttl) {
        final long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecord(key, true);
        if (record == null) {
            record = createRecord(key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordHeapCost(record));
        } else {
            updateSizeEstimator(-calculateRecordHeapCost(record));
            setRecordValue(record, value, now);
            updateSizeEstimator(calculateRecordHeapCost(record));
        }
        evictEntries(now, true);
        mapDataStore.addBackup(key, value, now);
        return record;
    }

    @Override
    public void deleteRecord(Data key) {
        Record record = records.remove(key);
        if (record != null) {
            record.invalidate();
        }
    }

    @Override
    public Iterator<Record> iterator() {
        return new ReadOnlyRecordIterator(records.values());
    }

    @Override
    public Iterator<Record> loadAwareIterator() {
        checkIfLoaded();
        return iterator();
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
            if (nullIfExpired(record, false) == null) {
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
        Map<Data, Data> temp = new HashMap<Data, Data>(records.size());
        for (Record record : records.values()) {
            record = nullIfExpired(record, false);
            if (record == null) {
                continue;
            }
            final Data key = record.getKey();
            final Data value = toData(record.getValue());
            temp.put(key, value);
        }

        return temp.entrySet();
    }

    @Override
    public Map.Entry<Data, Object> getMapEntry(Data key) {
        checkIfLoaded();
        Record record = getRecord(key, false);
        if (record == null) {
            record = loadAndCreateRecordOrNull(key, true);
        } else {
            accessRecord(record);
        }
        final Object value = record != null ? record.getValue() : null;
        return new AbstractMap.SimpleImmutableEntry<Data, Object>(key, value);
    }


    // TODO Does it need to load from store on backup?
    @Override
    public Map.Entry<Data, Object> getMapEntryForBackup(Data dataKey) {
        checkIfLoaded();
        Record record = getRecord(dataKey, true);
        if (record == null) {
            record = loadAndCreateRecordOrNull(dataKey, false);
        } else {
            accessRecord(record);
        }
        final Object data = record != null ? record.getValue() : null;
        return new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, data);
    }

    private Record loadAndCreateRecordOrNull(Data key, boolean enableIndex) {
        Record record = null;
        final Object value = mapDataStore.load(key);
        if (value != null) {
            record = createRecord(key, value, getNow());
            records.put(key, record);
            if (enableIndex) {
                saveIndex(record);
            }
            updateSizeEstimator(calculateRecordHeapCost(record));
        }
        return record;
    }

    @Override
    public Set<Data> keySet() {
        checkIfLoaded();
        Set<Data> keySet = new HashSet<Data>(records.size());
        for (Record record : records.values()) {
            record = nullIfExpired(record, false);
            if (record == null) {
                continue;
            }
            keySet.add(record.getKey());
        }
        return keySet;
    }

    @Override
    public Collection<Data> valuesData() {
        checkIfLoaded();
        Collection<Data> values = new ArrayList<Data>(records.size());
        for (Record record : records.values()) {
            record = nullIfExpired(record, false);
            if (record == null) {
                continue;
            }
            values.add(toData(record.getValue()));
        }
        return values;
    }

    @Override
    public int clear() {
        checkIfLoaded();
        resetSizeEstimator();
        final Collection<Data> lockedKeys = lockStore != null ? lockStore.getLockedKeys() : Collections.<Data>emptySet();
        final Map<Data, Record> lockedRecords = new HashMap<Data, Record>(lockedKeys.size());
        // Locked records should not be removed!
        for (Data key : lockedKeys) {
            Record record = getRecord(key, false);
            if (record != null) {
                lockedRecords.put(key, record);
                updateSizeEstimator(calculateRecordHeapCost(record));
            }
        }
        Set<Data> keysToDelete = records.keySet();
        keysToDelete.removeAll(lockedRecords.keySet());

        mapDataStore.removeAll(keysToDelete);

        int numOfClearedEntries = keysToDelete.size();
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
        final int size = size();
        final Set<Data> keysToPreserve = evictAllInternal(backup);
        removeIndexByPreservingKeys(keysToPreserve);
        return size - keysToPreserve.size();
    }

    /**
     * Internal evict all provides common functionality to all {@link #evictAll(boolean)} ()}
     *
     * @return preserved keys.
     */
    private Set<Data> evictAllInternal(boolean backup) {
        resetSizeEstimator();
        resetAccessSequenceNumber();

        Set<Data> keysToPreserve = Collections.emptySet();
        final Map<Data, Record> recordsToPreserve = getLockedRecords(backup);
        if (!recordsToPreserve.isEmpty()) {
            keysToPreserve = recordsToPreserve.keySet();
            updateSizeEstimator(calculateRecordHeapCost(recordsToPreserve.values()));
        }
        flush(recordsToPreserve, backup);
        clearRecordsMap(recordsToPreserve);
        return keysToPreserve;
    }

    /**
     * Flushes evicted records to map store.
     *
     * @param excludeRecords Records which should not be flushed.
     * @param backup         <code>true</code> if backup, false otherwise.
     */
    private void flush(Map<Data, Record> excludeRecords, boolean backup) {
        Iterator<Record> iterator = records.values().iterator();
        while (iterator.hasNext()) {
            Record record = iterator.next();
            if (excludeRecords == null || !excludeRecords.containsKey(record.getKey())) {
                final Data key = record.getKey();
                final long lastUpdateTime = record.getLastUpdateTime();
                mapDataStore.flush(key, record.getValue(), lastUpdateTime, backup);
            }
        }
    }

    /**
     * Removes indexes by excluding keysToPreserve.
     *
     * @param keysToPreserve should not be removed from index.
     */
    private void removeIndexByPreservingKeys(Set<Data> keysToPreserve) {
        final Set<Data> currentKeySet = records.keySet();
        currentKeySet.removeAll(keysToPreserve);

        removeIndex(currentKeySet);
    }

    /**
     * Returns locked records.
     *
     * @return map of locked records.
     */
    private Map<Data, Record> getLockedRecords(boolean backup) {
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
            Record record = getRecord(key, backup);
            if (record != null) {
                lockedRecords.put(key, record);
            }
        }
        return lockedRecords;
    }

    @Override
    public void removeBackup(Data key) {
        final long now = getNow();

        final Record record = getRecord(key, true);
        if (record == null) {
            return;
        }
        // reduce size
        updateSizeEstimator(-calculateRecordHeapCost(record));
        deleteRecord(key);
        evictEntries(now, true);
        mapDataStore.removeBackup(key, now);
    }

    @Override
    public Object remove(Data key) {
        checkIfLoaded();
        final long now = getNow();

        final Record record = getRecord(key, false);
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
        evictEntries(now, false);
        return oldValue;
    }

    @Override
    public boolean remove(Data key, Object testValue) {
        checkIfLoaded();
        final long now = getNow();

        Record record = getRecord(key, false);
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
        evictEntries(now, false);
        return removed;
    }

    @Override
    public boolean delete(Data key) {
        checkIfLoaded();
        final long now = getNow();

        Record record = getRecord(key, false);
        if (record == null) {
            removeIndex(key);
            mapDataStore.remove(key, now);
        } else {
            return removeRecord(key, record, now) != null;
        }
        evictEntries(now, false);
        return false;
    }

    @Override
    public Object get(Data key) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecord(key, false);
        if (record == null) {
            record = loadAndCreateRecordOrNull(key, true);
        } else {
            accessRecord(record, now);
        }
        Object value = record == null ? null : record.getValue();
        value = mapServiceContext.interceptGet(name, value);

        postReadCleanUp(now, false);
        return value;
    }


    @Override
    public MapEntrySet getAll(Set<Data> keys) {
        checkIfLoaded();
        final long now = getNow();

        final MapEntrySet mapEntrySet = new MapEntrySet();

        final Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            final Data key = iterator.next();
            Record record = getRecord(key, false);
            if (record != null) {
                addMapEntrySet(record.getKey(), record.getValue(), mapEntrySet);
                accessRecord(record);
                iterator.remove();
            }
        }
        addMapEntrySet(mapDataStore.loadAll(keys), mapEntrySet);
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

        Record record = getRecord(key, false);
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
        Record record = getRecord(key, false);
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
            setRecordValue(record, value, now);
            // then increase size
            updateSizeEstimator(calculateRecordHeapCost(record));
            saveIndex(record);
        }
        evictEntries(now, false);
    }

    @Override
    public Object put(Data key, Object value, long ttl) {
        checkIfLoaded();
        final long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecord(key, false);
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
            setRecordValue(record, value, now);
            // then increase size.
            updateSizeEstimator(calculateRecordHeapCost(record));
            updateTtl(record, ttl);
            saveIndex(record);
        }
        evictEntries(now, false);
        return oldValue;
    }

    @Override
    public boolean set(Data key, Object value, long ttl) {
        checkIfLoaded();
        final long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecord(key, false);
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
            setRecordValue(record, value, now);
            // then increase size.
            updateSizeEstimator(calculateRecordHeapCost(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);
        evictEntries(now, false);
        return newRecord;
    }

    @Override
    public boolean merge(Data key, EntryView mergingEntry, MapMergePolicy mergePolicy) {
        checkIfLoaded();
        final long now = getNow();
        Record record = getRecord(key, false);
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
            // same with the existing entry so no need to mapstore etc operations.
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
        evictEntries(now, false);
        return newValue != null;
    }

    // TODO why does not replace method load data from map store if currently not available in memory.
    @Override
    public Object replace(Data key, Object value) {
        checkIfLoaded();
        final long now = getNow();

        Record record = getRecord(key, false);
        Object oldValue;
        if (record != null && record.getValue() != null) {
            oldValue = record.getValue();
            value = mapServiceContext.interceptPut(name, oldValue, value);
            value = mapDataStore.add(key, value, now);
            onStore(record);
            updateSizeEstimator(-calculateRecordHeapCost(record));
            setRecordValue(record, value, now);
            updateSizeEstimator(calculateRecordHeapCost(record));
        } else {
            return null;
        }
        saveIndex(record);
        evictEntries(now, false);
        return oldValue;
    }

    @Override
    public boolean replace(Data key, Object testValue, Object newValue) {
        checkIfLoaded();
        final long now = getNow();

        Record record = getRecord(key, false);
        if (record == null) {
            return false;
        }
        if (mapServiceContext.compare(name, record.getValue(), testValue)) {
            newValue = mapServiceContext.interceptPut(name, record.getValue(), newValue);
            newValue = mapDataStore.add(key, newValue, now);
            onStore(record);
            updateSizeEstimator(-calculateRecordHeapCost(record));
            setRecordValue(record, newValue, now);
            updateSizeEstimator(calculateRecordHeapCost(record));
        } else {
            return false;
        }
        saveIndex(record);
        evictEntries(now, false);
        return true;
    }

    @Override
    public void putTransient(Data key, Object value, long ttl) {
        checkIfLoaded();
        final long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecord(key, false);
        if (record == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            record = createRecord(key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordHeapCost(record));
        } else {
            value = mapServiceContext.interceptPut(name, record.getValue(), value);
            updateSizeEstimator(-calculateRecordHeapCost(record));
            setRecordValue(record, value, now);
            updateSizeEstimator(calculateRecordHeapCost(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);
        evictEntries(now, false);
        mapDataStore.addTransient(key, now);
    }

    @Override
    public Object putFromLoad(Data key, Object value) {
        return putFromLoad(key, value, DEFAULT_TTL);
    }

    @Override
    public Object putFromLoad(Data key, Object value, long ttl) {
        final long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecord(key, false);
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
            setRecordValue(record, value, now);
            updateSizeEstimator(calculateRecordHeapCost(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);
        evictEntries(now, false);
        return oldValue;
    }

    @Override
    public boolean tryPut(Data key, Object value, long ttl) {
        checkIfLoaded();
        final long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecord(key, false);
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
            setRecordValue(record, value, now);
            updateSizeEstimator(calculateRecordHeapCost(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);
        evictEntries(now, false);
        return true;
    }

    @Override
    public Object putIfAbsent(Data key, Object value, long ttl) {
        checkIfLoaded();
        final long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecord(key, false);
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
            updateTtl(record, ttl);
        }
        saveIndex(record);
        evictEntries(now, false);
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

    private Record getRecord(Data key, boolean backup) {
        Record record = records.get(key);
        return nullIfExpired(record, backup);
    }

}
