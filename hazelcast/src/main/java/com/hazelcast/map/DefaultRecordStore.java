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
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.eviction.EvictionHelper;
import com.hazelcast.map.mapstore.MapDataStore;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.operation.PutAllOperation;
import com.hazelcast.map.operation.PutFromLoadAllOperation;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordFactory;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Default implementation of record-store.
 */
public class DefaultRecordStore implements RecordStore {

    private static final long DEFAULT_TTL = -1L;
    /**
     * Number of reads before clean up.
     * A nice number such as 2^n - 1.
     */
    private static final int POST_READ_CHECK_POINT = 63;
    private final String name;
    private final int partitionId;
    private final ConcurrentMap<Data, Record> records = new ConcurrentHashMap<Data, Record>(1000);
    private final MapContainer mapContainer;
    private final MapServiceContext mapServiceContext;
    private final RecordFactory recordFactory;
    private final ILogger logger;
    private final SizeEstimator sizeEstimator;
    private final AtomicBoolean loaded = new AtomicBoolean(false);
    private final LockStore lockStore;
    private long lastEvictionTime;
    /**
     * Flag for checking if this record store has at least one candidate entry
     * for expiration (idle or tll) or not.
     */
    private volatile boolean expirable;

    /**
     * Iterates over a pre-set entry count/percentage in one round.
     * Used in expiration logic for traversing entries. Initializes lazily.
     */
    private Iterator<Record> expirationIterator;

    /**
     * If there is no clean-up caused by puts after some time,
     * count a number of gets and start eviction.
     */
    private int readCountBeforeCleanUp;

    /**
     * used in LRU eviction logic.
     */
    private long lruAccessSequenceNumber;

    private boolean evictionEnabled;

    private MapDataStore<Data, Object> mapDataStore;

    public DefaultRecordStore(String name, MapServiceContext mapServiceContext, int partitionId) {
        this.name = name;
        this.partitionId = partitionId;
        this.mapServiceContext = mapServiceContext;
        this.mapContainer = mapServiceContext.getMapContainer(name);
        this.logger = mapServiceContext.getNodeEngine().getLogger(this.getName());
        this.recordFactory = mapContainer.getRecordFactory();
        this.lockStore = createLockStore();
        this.sizeEstimator = SizeEstimators.createMapSizeEstimator();
        this.expirable = isRecordStoreExpirable();
        loadFromMapStore();
        this.evictionEnabled
                = !MapConfig.EvictionPolicy.NONE.equals(mapContainer.getMapConfig().getEvictionPolicy());
        this.mapDataStore = mapContainer.getMapStoreManager().getMapDataStore(partitionId);
    }

    public boolean isLoaded() {
        return loaded.get();
    }

    public void setLoaded(boolean isLoaded) {
        loaded.set(isLoaded);
    }

    public void checkIfLoaded() {
        if (mapContainer.getStore() != null && !loaded.get()) {
            throw ExceptionUtil.rethrow(new RetryableHazelcastException("Map is not ready!!!"));
        }
    }

    public String getName() {
        return name;
    }

    public void flush() {
        checkIfLoaded();
        final Collection<Data> processedKeys = mapDataStore.flush();
        for (Data key : processedKeys) {
            final Record record = records.get(key);
            if (record != null) {
                record.onStore();
            }
        }
    }

    public MapContainer getMapContainer() {
        return mapContainer;
    }

    public Record getRecord(Data key) {
        return records.get(key);
    }

    @Override
    public void putRecord(Data key, Record record) {
        final Record existingRecord = records.put(key, record);
        updateSizeEstimator(-calculateRecordSize(existingRecord));
        updateSizeEstimator(calculateRecordSize(record));
    }

    public Record putBackup(Data key, Object value) {
        return putBackup(key, value, DEFAULT_TTL);
    }

    /**
     * @param key   the key to be processed.
     * @param value the value to be processed.
     * @param ttl   milliseconds. Check out {@link com.hazelcast.map.proxy.MapProxySupport#putInternal}
     * @return previous record if exists otherwise null.
     */
    public Record putBackup(Data key, Object value, long ttl) {
        final long now = getNow();
        earlyWriteCleanup(now);
        markRecordStoreExpirable(ttl);

        Record record = records.get(key);
        if (record == null) {
            record = createRecord(key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value, now);
            updateSizeEstimator(calculateRecordSize(record));
        }
        mapDataStore.addBackup(key, value, now);
        return record;
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

    public Map<Data, Record> getReadonlyRecordMapByWaitingMapStoreLoad() {
        checkIfLoaded();
        return getReadonlyRecordMap();
    }

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

    /**
     * Size may not give precise size at a specific moment
     * due to the expiration logic. But eventually, it should be correct.
     *
     * @return record store size.
     */
    public int size() {
        // do not add checkIfLoaded(), size() is also used internally
        return records.size();
    }

    public boolean isEmpty() {
        checkIfLoaded();
        return records.isEmpty();
    }

    @Override
    public void evictExpiredEntries(int percentage, boolean ownerPartition) {
        final long now = getNow();
        final int size = size();
        final int maxIterationCount = getMaxIterationCount(size, percentage);
        final int maxRetry = 3;
        int loop = 0;
        int evictedEntryCount = 0;
        while (true) {
            evictedEntryCount += evictExpiredEntries0(maxIterationCount, now, ownerPartition);
            if (evictedEntryCount >= maxIterationCount) {
                break;
            }
            loop++;
            if (loop > maxRetry) {
                break;
            }
        }
    }

    /**
     * Intended to put an upper bound to iterations. Used in evictions.
     *
     * @param size       of iterate-able.
     * @param percentage percentage of size.
     * @return 100 If calculated iteration count is less than 100, otherwise returns calculated iteration count.
     */
    private int getMaxIterationCount(int size, int percentage) {
        final int defaultMaxIterationCount = 100;
        final float oneHundred = 100F;
        float maxIterationCount = size * (percentage / oneHundred);
        if (maxIterationCount <= defaultMaxIterationCount) {
            return defaultMaxIterationCount;
        }
        return Math.round(maxIterationCount);
    }

    private int evictExpiredEntries0(int maxIterationCount, long now, boolean ownerPartition) {
        int evictedCount = 0;
        int checkedEntryCount = 0;
        initExpirationIterator();
        while (expirationIterator.hasNext()) {
            if (checkedEntryCount >= maxIterationCount) {
                break;
            }
            checkedEntryCount++;
            final Record record = expirationIterator.next();
            final Data key = record.getKey();
            if (isLocked(key)) {
                continue;
            }
            if (isReachable(record, now)) {
                continue;
            }
            //!!! get entry value here because evict0(key) nulls the record value.
            final Object value = record.getValue();
            evict0(key);
            evictedCount++;
            initExpirationIterator();

            // do post eviction operations if this partition is an owner partition.
            if (ownerPartition) {
                doPostEvictionOperations(key, value);
            }
            if (!expirationIterator.hasNext()) {
                break;
            }
        }
        return evictedCount;
    }

    private void initExpirationIterator() {
        if (expirationIterator == null || !expirationIterator.hasNext()) {
            expirationIterator = records.values().iterator();
        }
    }

    @Override
    public boolean containsValue(Object value) {
        checkIfLoaded();
        final long now = getNow();
        for (Record record : records.values()) {
            if (nullIfExpired(record) == null) {
                continue;
            }
            if (mapServiceContext.compare(name, value, record.getValue())) {
                return true;
            }
        }
        postReadCleanUp(now);
        return false;
    }

    public boolean lock(Data key, String caller, long threadId, long ttl) {
        checkIfLoaded();
        return lockStore != null && lockStore.lock(key, caller, threadId, ttl);
    }

    public boolean txnLock(Data key, String caller, long threadId, long ttl) {
        checkIfLoaded();
        return lockStore != null && lockStore.txnLock(key, caller, threadId, ttl);
    }

    public boolean extendLock(Data key, String caller, long threadId, long ttl) {
        checkIfLoaded();
        return lockStore != null && lockStore.extendLeaseTime(key, caller, threadId, ttl);
    }

    public boolean unlock(Data key, String caller, long threadId) {
        checkIfLoaded();
        return lockStore != null && lockStore.unlock(key, caller, threadId);
    }

    public boolean forceUnlock(Data dataKey) {
        return lockStore != null && lockStore.forceUnlock(dataKey);
    }

    @Override
    public long getHeapCost() {
        return sizeEstimator.getSize();
    }

    public boolean isLocked(Data dataKey) {
        return lockStore != null && lockStore.isLocked(dataKey);
    }

    public boolean canAcquireLock(Data key, String caller, long threadId) {
        return lockStore == null || lockStore.canAcquireLock(key, caller, threadId);
    }

    public String getLockOwnerInfo(Data key) {
        return lockStore != null ? lockStore.getOwnerInfo(key) : null;
    }

    public Set<Map.Entry<Data, Data>> entrySetData() {
        checkIfLoaded();
        Map<Data, Data> temp = new HashMap<Data, Data>(records.size());
        for (Data key : records.keySet()) {
            temp.put(key, mapServiceContext.toData(records.get(key).getValue()));
        }
        return temp.entrySet();
    }

    public Map.Entry<Data, Object> getMapEntry(Data dataKey) {
        checkIfLoaded();
        Record record = records.get(dataKey);
        if (record == null) {
            record = getRecordInternal(dataKey, true);
        } else {
            accessRecord(record);
        }
        final Object data = record != null ? record.getValue() : null;
        return new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, data);
    }


    // TODO Does it need to load from store on backup?
    public Map.Entry<Data, Object> getMapEntryForBackup(Data dataKey) {
        checkIfLoaded();
        Record record = records.get(dataKey);
        if (record == null) {
            record = getRecordInternal(dataKey, false);
        } else {
            accessRecord(record);
        }
        final Object data = record != null ? record.getValue() : null;
        return new AbstractMap.SimpleImmutableEntry<Data, Object>(dataKey, data);
    }

    private Record getRecordInternal(Data key, boolean enableIndex) {
        Record record = null;
        final Object value = mapDataStore.load(key);
        if (value != null) {
            record = createRecord(key, value, getNow());
            records.put(key, record);
            if (enableIndex) {
                saveIndex(record);
            }
            updateSizeEstimator(calculateRecordSize(record));
        }
        return record;
    }

    public Set<Data> keySet() {
        checkIfLoaded();
        Set<Data> keySet = new HashSet<Data>(records.size());
        for (Data data : records.keySet()) {
            keySet.add(data);
        }
        return keySet;
    }

    public Collection<Data> valuesData() {
        checkIfLoaded();
        Collection<Data> values = new ArrayList<Data>(records.size());
        for (Record record : records.values()) {
            values.add(mapServiceContext.toData(record.getValue()));
        }
        return values;
    }

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
                updateSizeEstimator(calculateRecordSize(record));
            }
        }
        Set<Data> keysToDelete = records.keySet();
        keysToDelete.removeAll(lockedRecords.keySet());

        final MapStoreWrapper store = mapContainer.getStore();
        if (store != null) {
            // Use an ArrayList so that we don't trigger calls to equals or hashCode on the key objects
            Collection<Object> keysObject = new ArrayList<Object>(keysToDelete.size());
            for (Data key : keysToDelete) {
                keysObject.add(mapServiceContext.toObject(key));
            }

            store.deleteAll(keysObject);
        }

        int numOfClearedEntries = keysToDelete.size();
        removeIndex(keysToDelete);

        clearRecordsMap(lockedRecords);
        resetAccessSequenceNumber();
        mapDataStore.reset();
        return numOfClearedEntries;
    }

    public void reset() {
        checkIfLoaded();

        clearRecordsMap(Collections.<Data, Record>emptyMap());
        resetSizeEstimator();
        resetAccessSequenceNumber();
        mapDataStore.reset();
    }

    private void resetAccessSequenceNumber() {
        lruAccessSequenceNumber = 0L;
    }

    @Override
    public Object evict(Data key) {
        checkIfLoaded();
        return evict0(key);
    }

    private Object evict0(Data key) {
        Record record = records.get(key);
        Object value = null;
        if (record != null) {
            value = record.getValue();
            final long lastUpdateTime = record.getLastUpdateTime();
            mapDataStore.addStagingArea(key, value, lastUpdateTime);
            mapServiceContext.interceptRemove(name, value);
            updateSizeEstimator(-calculateRecordSize(record));
            deleteRecord(key);
            removeIndex(key);
        }
        return value;
    }

    @Override
    public int evictAll() {
        checkIfLoaded();
        final int size = size();
        final Set<Data> keysToPreserve = evictAll0();
        removeIndexByPreserving(keysToPreserve);
        return size - keysToPreserve.size();
    }

    @Override
    public void evictAllBackup() {
        evictAll0();
    }

    /**
     * Internal evict all provides common functionality to all {@link #evictAll()}
     *
     * @return preserved keys.
     */
    private Set<Data> evictAll0() {
        resetSizeEstimator();
        resetAccessSequenceNumber();

        Set<Data> keysToPreserve = Collections.emptySet();
        final Map<Data, Record> recordsToPreserve = getLockedRecords();
        if (!recordsToPreserve.isEmpty()) {
            keysToPreserve = recordsToPreserve.keySet();
            updateSizeEstimator(calculateRecordSize(recordsToPreserve.values()));
        }
        clearRecordsMap(recordsToPreserve);
        return keysToPreserve;
    }

    /**
     * Removes indexes by excluding keysToPreserve.
     *
     * @param keysToPreserve should not be removed from index.
     */
    private void removeIndexByPreserving(Set<Data> keysToPreserve) {
        final Set<Data> currentKeySet = records.keySet();
        currentKeySet.removeAll(keysToPreserve);

        removeIndex(currentKeySet);
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
            Record record = records.get(key);
            if (record != null) {
                lockedRecords.put(key, record);
            }
        }
        return lockedRecords;
    }

    @Override
    public void removeBackup(Data key) {
        final long now = getNow();
        earlyWriteCleanup(now);

        final Record record = records.get(key);
        if (record == null) {
            return;
        }
        // reduce size
        updateSizeEstimator(-calculateRecordSize(record));
        deleteRecord(key);
        mapDataStore.removeBackup(key, now);
    }

    @Override
    public boolean remove(Data key, Object testValue) {
        checkIfLoaded();
        final long now = getNow();
        earlyWriteCleanup(now);

        Record record = records.get(key);
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
            updateSizeEstimator(-calculateRecordSize(record));
            deleteRecord(key);
            removed = true;
        }
        return removed;
    }

    @Override
    public Object remove(Data key) {
        checkIfLoaded();
        final long now = getNow();
        earlyWriteCleanup(now);

        Record record = records.get(key);
        Object oldValue;
        if (record == null) {
            oldValue = mapDataStore.load(key);
            if (oldValue != null) {
                removeIndex(key);
                mapDataStore.remove(key, now);
            }
        } else {
            oldValue = record.getValue();
            oldValue = mapServiceContext.interceptRemove(name, oldValue);
            if (oldValue != null) {
                removeIndex(key);
                mapDataStore.remove(key, now);
                onStore(record);
            }
            // reduce size
            updateSizeEstimator(-calculateRecordSize(record));
            deleteRecord(key);
        }
        return oldValue;
    }

    @Override
    public Object get(Data key) {
        checkIfLoaded();
        long now = getNow();
        final Object value = get0(key, now);
        postReadCleanUp(now);
        return value;
    }

    private Object get0(Data key, long now) {
        Record record = records.get(key);
        record = nullIfExpired(record);
        Object value;
        if (record == null) {
            value = mapDataStore.load(key);
            if (value != null) {
                record = createRecord(key, value, now);
                records.put(key, record);
                saveIndex(record);
                updateSizeEstimator(calculateRecordSize(record));
            }
        } else {
            accessRecord(record, now);
            value = record.getValue();
        }
        value = mapServiceContext.interceptGet(name, value);
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
            final Record record = records.get(key);
            if (record != null) {
                addMapEntrySet(record.getKey(), record.getValue(), mapEntrySet);
                iterator.remove();
            }
        }
        addMapEntrySet(mapDataStore.loadAll(keys), mapEntrySet);

        postReadCleanUp(now);
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
        Record record = records.get(key);
        record = nullIfExpired(record);

        if (record == null) {
            Object value = mapDataStore.load(key);
            if (value != null) {
                record = createRecord(key, value, now);
                records.put(key, record);
                updateSizeEstimator(calculateRecordSize(record));
            }
        }
        boolean contains = record != null;
        if (contains) {
            accessRecord(record, now);
        }
        postReadCleanUp(now);
        return contains;
    }

    @Override
    public void put(Map.Entry<Data, Object> entry) {
        checkIfLoaded();
        final long now = getNow();
        earlyWriteCleanup(now);

        Data key = entry.getKey();
        Object value = entry.getValue();
        Record record = records.get(key);
        if (record == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            value = mapDataStore.add(key, value, now);
            record = createRecord(key, value, now);
            records.put(key, record);
            // increase size.
            updateSizeEstimator(calculateRecordSize(record));
            saveIndex(record);
        } else {
            final Object oldValue = record.getValue();
            value = mapServiceContext.interceptPut(name, oldValue, value);
            value = mapDataStore.add(key, value, now);
            onStore(record);
            // if key exists before, first reduce size
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value, now);
            // then increase size
            updateSizeEstimator(calculateRecordSize(record));
            saveIndex(record);
        }
    }

    public Object put(Data key, Object value, long ttl) {
        checkIfLoaded();
        final long now = getNow();
        earlyWriteCleanup(now);
        markRecordStoreExpirable(ttl);

        Record record = records.get(key);
        Object oldValue;
        if (record == null) {
            oldValue = mapDataStore.load(key);
            value = mapServiceContext.interceptPut(name, null, value);
            value = mapDataStore.add(key, value, now);
            record = createRecord(key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
            saveIndex(record);
        } else {
            oldValue = record.getValue();
            value = mapServiceContext.interceptPut(name, oldValue, value);
            value = mapDataStore.add(key, value, now);
            onStore(record);
            // if key exists before, first reduce size
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value, now);
            // then increase size.
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
            saveIndex(record);
        }

        return oldValue;
    }


    public boolean set(Data key, Object value, long ttl) {
        checkIfLoaded();
        final long now = getNow();
        earlyWriteCleanup(now);
        markRecordStoreExpirable(ttl);

        Record record = records.get(key);
        boolean newRecord = false;
        if (record == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            value = mapDataStore.add(key, value, now);
            record = createRecord(key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
            newRecord = true;
        } else {
            value = mapServiceContext.interceptPut(name, record.getValue(), value);
            value = mapDataStore.add(key, value, now);
            onStore(record);
            // if key exists before, first reduce size
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value, now);
            // then increase size.
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);

        return newRecord;
    }

    public boolean merge(Data key, EntryView mergingEntry, MapMergePolicy mergePolicy) {
        checkIfLoaded();
        final long now = getNow();
        earlyWriteCleanup(now);

        Record record = records.get(key);
        Object newValue;
        if (record == null) {
            final Object notExistingKey = mapServiceContext.toObject(key);
            final EntryView<Object, Object> nullEntryView = EntryViews.createNullEntryView(notExistingKey);
            newValue = mergePolicy.merge(name, mergingEntry, nullEntryView);
            if (newValue == null) {
                return false;
            }
            newValue = mapDataStore.add(key, newValue, now);
            record = createRecord(key, newValue, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            Object oldValue = record.getValue();
            EntryView existingEntry = EntryViews.createSimpleEntryView(mapServiceContext.toObject(record.getKey()),
                    mapServiceContext.toObject(record.getValue()), record);
            newValue = mergePolicy.merge(name, mergingEntry, existingEntry);
            // existing entry will be removed
            if (newValue == null) {
                removeIndex(key);
                mapDataStore.remove(key, now);
                onStore(record);
                // reduce size.
                updateSizeEstimator(-calculateRecordSize(record));
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
            updateSizeEstimator(-calculateRecordSize(record));
            recordFactory.setValue(record, newValue);
            updateSizeEstimator(calculateRecordSize(record));
        }
        saveIndex(record);
        return newValue != null;
    }

    // TODO why does not replace method load data from map store if currently not available in memory.
    public Object replace(Data key, Object value) {
        checkIfLoaded();
        final long now = getNow();
        earlyWriteCleanup(now);

        Record record = records.get(key);
        Object oldValue;
        if (record != null && record.getValue() != null) {
            oldValue = record.getValue();
            value = mapServiceContext.interceptPut(name, oldValue, value);
            value = mapDataStore.add(key, value, now);
            onStore(record);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value, now);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            return null;
        }
        saveIndex(record);

        return oldValue;
    }

    public boolean replace(Data key, Object testValue, Object newValue) {
        checkIfLoaded();
        final long now = getNow();
        earlyWriteCleanup(now);

        Record record = records.get(key);
        if (record == null) {
            return false;
        }
        if (mapServiceContext.compare(name, record.getValue(), testValue)) {
            newValue = mapServiceContext.interceptPut(name, record.getValue(), newValue);
            newValue = mapDataStore.add(key, newValue, now);
            onStore(record);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, newValue, now);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            return false;
        }
        saveIndex(record);

        return true;
    }

    public void putTransient(Data key, Object value, long ttl) {
        checkIfLoaded();
        final long now = getNow();
        earlyWriteCleanup(now);
        markRecordStoreExpirable(ttl);

        Record record = records.get(key);
        if (record == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            record = createRecord(key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            value = mapServiceContext.interceptPut(name, record.getValue(), value);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value, now);
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
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
        earlyWriteCleanup(now);
        markRecordStoreExpirable(ttl);

        Record record = records.get(key);
        Object oldValue = null;
        if (record == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            record = createRecord(key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            oldValue = record.getValue();
            value = mapServiceContext.interceptPut(name, record.getValue(), value);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value, now);
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);
        return oldValue;
    }


    public boolean tryPut(Data key, Object value, long ttl) {
        checkIfLoaded();

        final long now = getNow();
        earlyWriteCleanup(now);
        markRecordStoreExpirable(ttl);

        Record record = records.get(key);
        if (record == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            value = mapDataStore.add(key, value, now);
            record = createRecord(key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            value = mapServiceContext.interceptPut(name, record.getValue(), value);
            value = mapDataStore.add(key, value, now);
            onStore(record);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value, now);
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);

        return true;
    }

    public Object putIfAbsent(Data key, Object value, long ttl) {
        checkIfLoaded();

        final long now = getNow();
        earlyWriteCleanup(now);
        markRecordStoreExpirable(ttl);

        Record record = records.get(key);
        Object oldValue;
        if (record == null) {
            oldValue = mapDataStore.load(key);
            if (oldValue != null) {
                record = createRecord(key, oldValue, now);
                records.put(key, record);
                updateSizeEstimator(calculateRecordSize(record));
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
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);

        return oldValue;
    }

    private void onStore(Record record) {
        if (record != null) {
            record.onStore();
        }
    }

    private void loadFromMapStore() {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final AtomicBoolean loadOccurred = loaded;
        if (!mapContainer.isMapStoreEnabled() || loadOccurred.get()) {
            return;
        }
        final Address partitionOwner = nodeEngine.getPartitionService().getPartitionOwner(partitionId);
        final boolean isOwner = nodeEngine.getThisAddress().equals(partitionOwner);
        if (!isOwner) {
            loadOccurred.set(true);
            return;
        }
        final Map<Data, Object> loadedKeys = mapContainer.getInitialKeys();
        if (loadedKeys == null || loadedKeys.isEmpty()) {
            loadOccurred.set(true);
            return;
        }
        doChunkedLoad(loadedKeys, nodeEngine);
    }

    private void doChunkedLoad(Map<Data, Object> loadedKeys, NodeEngine nodeEngine) {
        final int mapLoadChunkSize = nodeEngine.getGroupProperties().MAP_LOAD_CHUNK_SIZE.getInteger();
        final Queue<Map> chunks = new LinkedList<Map>();
        Map<Data, Object> partitionKeys = new HashMap<Data, Object>();
        Iterator<Map.Entry<Data, Object>> iterator = loadedKeys.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<Data, Object> entry = iterator.next();
            final Data data = entry.getKey();
            if (partitionId == nodeEngine.getPartitionService().getPartitionId(data)) {
                partitionKeys.put(data, entry.getValue());
                //split into chunks
                if (partitionKeys.size() >= mapLoadChunkSize) {
                    chunks.add(partitionKeys);
                    partitionKeys = new HashMap<Data, Object>();
                }
                iterator.remove();
            }
        }
        if (!partitionKeys.isEmpty()) {
            chunks.add(partitionKeys);
        }
        if (chunks.isEmpty()) {
            loaded.set(true);
            return;
        }
        try {
            final AtomicInteger checkIfMapLoaded = new AtomicInteger(chunks.size());
            ExecutionService executionService = nodeEngine.getExecutionService();
            Map<Data, Object> chunkedKeys;
            while ((chunkedKeys = chunks.poll()) != null) {
                executionService.submit("hz:map-load", new MapLoadAllTask(chunkedKeys, checkIfMapLoaded));
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    /**
     * TODO make checkEvictable fast by carrying threshold logic to partition.
     * This cleanup adds some latency to write operations.
     * But it sweeps records much better under high write loads.
     * <p/>
     *
     * @param now now in time.
     */
    private void earlyWriteCleanup(long now) {
        if (evictionEnabled) {
            cleanUp(now);
        }
    }

    /**
     * If there is no clean-up caused by puts after some time,
     * try to clean-up from gets.
     *
     * @param now now.
     */
    private void postReadCleanUp(long now) {
        if (evictionEnabled) {
            readCountBeforeCleanUp++;
            if ((readCountBeforeCleanUp & POST_READ_CHECK_POINT) == 0) {
                cleanUp(now);
            }
        }

    }

    private void cleanUp(long now) {
        if (size() == 0) {
            return;
        }
        if (inEvictableTimeWindow(now) && isEvictable()) {
            removeEvictables();
            lastEvictionTime = now;
            readCountBeforeCleanUp = 0;
        }
    }

    private void removeEvictables() {
        EvictionHelper.removeEvictableRecords(DefaultRecordStore.this,
                mapContainer.getMapConfig(), mapServiceContext.getService());
    }


    /**
     * Eviction waits at least 1000 milliseconds to run.
     *
     * @return <code>true</code> if in that time window,
     * otherwise <code>false</code>
     */
    private boolean inEvictableTimeWindow(long now) {
        final int evictAfterMs = 1000;
        return (now - lastEvictionTime) > evictAfterMs;
    }

    private boolean isEvictable() {
        return EvictionHelper.checkEvictable(mapContainer);
    }

    private Record nullIfExpired(Record record) {
        return evictIfNotReachable(record);
    }

    @Override
    public boolean isExpirable() {
        return expirable;
    }

    @Override
    public void loadAllFromStore(Collection<Data> keys, boolean replaceExistingValues) {
        if (keys.isEmpty()) {
            return;
        }
        loaded.set(false);
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.submit("hz:map-loadAllKeys", new LoadAllKeysTask(keys, replaceExistingValues));
    }

    @Override
    public MapDataStore<Data, Object> getMapDataStore() {
        return mapDataStore;
    }


    private Record createRecord(Data key, Object value, long ttl, long now) {
        return mapContainer.createRecord(key, value, ttl, now);
    }

    private Record createRecord(Data key, Object value, long now) {
        return mapContainer.createRecord(key, value, DEFAULT_TTL, now);
    }

    /**
     * Check if record is reachable according to ttl or idle times.
     * If not reachable return null.
     *
     * @param record {@link com.hazelcast.map.record.Record}
     * @return null if evictable.
     */
    private Record evictIfNotReachable(Record record) {
        if (record == null) {
            return null;
        }
        if (isLocked(record.getKey())) {
            return record;
        }
        if (isReachable(record)) {
            return record;
        }
        final Data key = record.getKey();
        final Object value = record.getValue();
        evict(key);
        doPostEvictionOperations(key, value);
        return null;
    }

    private boolean isReachable(Record record) {
        final long now = getNow();
        return isReachable(record, now);
    }

    private boolean isReachable(Record record, long time) {
        final Record idleExpired = isIdleExpired(record, time);
        if (idleExpired == null) {
            return false;
        }
        final Record ttlExpired = isTTLExpired(record, time);

        return ttlExpired != null;
    }

    private Record isIdleExpired(Record record, long time) {
        if (record == null) {
            return null;
        }
        boolean result;
        // lastAccessTime : updates on every touch (put/get).
        final long lastAccessTime = record.getLastAccessTime();

        assert lastAccessTime > 0L;
        assert time > 0L;
        assert time >= lastAccessTime;

        final long idleTime = getIdleTime();
        result = time - lastAccessTime >= idleTime;

        return result ? null : record;
    }

    private long getIdleTime() {
        final int maxIdleSeconds = mapContainer.getMapConfig().getMaxIdleSeconds();
        return maxIdleSeconds == 0 ? Long.MAX_VALUE : mapServiceContext.convertTime(maxIdleSeconds, TimeUnit.SECONDS);
    }

    private Record isTTLExpired(Record record, long time) {
        if (record == null) {
            return null;
        }
        boolean result;
        final long ttl = record.getTtl();
        // when ttl is zero or negative, it should remain eternally.
        if (ttl < 1L) {
            return record;
        }
        final long creationTime = record.getCreationTime();

        assert ttl > 0L : String.format("wrong ttl %d", ttl);
        assert creationTime > 0L : String.format("wrong creationTime %d", creationTime);
        assert time > 0L : String.format("wrong time %d", time);
        assert time >= creationTime : String.format("time >= lastUpdateTime (%d >= %d)",
                time, creationTime);

        result = time - creationTime >= ttl;
        return result ? null : record;
    }

    /**
     * - Sends eviction event.
     * - Invalidates near cache.
     *
     * @param key   the key to be processed.
     * @param value the value to be processed.
     */
    private void doPostEvictionOperations(Data key, Object value) {
        final NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
        if (nearCacheProvider.isNearCacheAndInvalidationEnabled(name)) {
            nearCacheProvider.invalidateAllNearCaches(name, key);
        }
        EvictionHelper.fireEvent(key, value, name, mapServiceContext.getService());
    }

    private void accessRecord(Record record, long now) {
        increaseRecordEvictionCriteriaNumber(record, mapContainer.getMapConfig().getEvictionPolicy());
        record.setLastAccessTime(now);
        record.onAccess();
    }

    private void accessRecord(Record record) {
        final long now = getNow();
        accessRecord(record, now);
    }

    private long getNow() {
        return Clock.currentTimeMillis();
    }

    private void increaseRecordEvictionCriteriaNumber(Record record, MapConfig.EvictionPolicy evictionPolicy) {
        switch (evictionPolicy) {
            case LRU:
                ++lruAccessSequenceNumber;
                record.setEvictionCriteriaNumber(lruAccessSequenceNumber);
                break;
            case LFU:
                record.setEvictionCriteriaNumber(record.getEvictionCriteriaNumber() + 1L);
                break;
            case NONE:
                break;
            default:
                throw new IllegalArgumentException("Not an appropriate eviction policy [" + evictionPolicy + ']');
        }
    }

    private void saveIndex(Record record) {
        Data dataKey = record.getKey();
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            SerializationService ss = mapServiceContext.getNodeEngine().getSerializationService();
            QueryableEntry queryableEntry = new QueryEntry(ss, dataKey, dataKey, record.getValue());
            indexService.saveEntryIndex(queryableEntry);
        }
    }

    private void updateTtl(Record record, long ttl) {
        if (ttl < 0L) {
            return;
        }
        record.setTtl(ttl);
        if (record.getStatistics() != null) {
            // when ttl is zero, entry should remain eternally.
            final long expirationTime = ttl == 0 ? Long.MAX_VALUE : (getNow() + ttl);
            record.getStatistics().setExpirationTime(expirationTime);
        }
    }


    private void markRecordStoreExpirable(long ttl) {
        if (ttl > 0L) {
            expirable = true;
        }
    }

    private void updateSizeEstimator(long recordSize) {
        sizeEstimator.add(recordSize);
    }

    private long calculateRecordSize(Record record) {
        return sizeEstimator.getCost(record);
    }

    /**
     * Returns total size of collection.
     *
     * @param collection which's size to be calculated.
     * @return total size of collection.
     */
    private long calculateRecordSize(Collection<Record> collection) {
        long totalSize = 0L;
        for (Record record : collection) {
            totalSize += calculateRecordSize(record);
        }
        return totalSize;
    }

    private void resetSizeEstimator() {
        sizeEstimator.reset();
    }

    private void setRecordValue(Record record, Object value, long now) {
        accessRecord(record, now);
        record.setLastUpdateTime(now);
        record.onUpdate();
        recordFactory.setValue(record, value);
    }

    private void removeIndex(Data key) {
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            indexService.removeEntryIndex(key);
        }
    }

    private void removeIndex(Set<Data> keys) {
        final IndexService indexService = mapContainer.getIndexService();
        if (indexService.hasIndex()) {
            for (Data key : keys) {
                indexService.removeEntryIndex(key);
            }
        }
    }

    private boolean isRecordStoreExpirable() {
        return mapContainer.getMapConfig().getMaxIdleSeconds() > 0
                || mapContainer.getMapConfig().getTimeToLiveSeconds() > 0;
    }

    private LockStore createLockStore() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService == null) {
            return null;
        }
        return lockService.createLockStore(partitionId, new DefaultObjectNamespace(MapService.SERVICE_NAME, name));
    }

    private int getLoadBatchSize() {
        return mapServiceContext.getNodeEngine().getGroupProperties().MAP_LOAD_CHUNK_SIZE.getInteger();
    }


    private final class LoadAllKeysTask implements Runnable {

        private final Collection<Data> keys;
        private final boolean replaceExistingValues;

        private LoadAllKeysTask(Collection<Data> keys, boolean replaceExistingValues) {
            this.keys = keys;
            this.replaceExistingValues = replaceExistingValues;
        }

        @Override
        public void run() {
            loadKeys(keys, replaceExistingValues);
        }
    }

    private void loadKeys(Collection<Data> keys, boolean replaceExistingValues) {
        if (!replaceExistingValues) {
            removeExistingKeys(keys);
        }
        removeUnloadableKeys(keys);

        if (keys.isEmpty()) {
            loaded.set(true);
            return;
        }
        final List<Object> objectKeys = convertDataKeysToObject(keys);
        doBatchLoad(objectKeys);
    }

    private void doBatchLoad(List<Object> keys) {
        final Queue<List<Object>> batchChunks = createBatchChunks(keys);
        final int size = batchChunks.size();
        final AtomicInteger finishedBatchCounter = new AtomicInteger(size);
        while (!batchChunks.isEmpty()) {
            final List<Object> chunk = batchChunks.poll();
            final List<Data> keyValueSequence = loadAndGet(chunk);
            if (keyValueSequence.isEmpty()) {
                if (finishedBatchCounter.decrementAndGet() == 0) {
                    loaded.set(true);
                }
                continue;
            }
            sendOperation(keyValueSequence, finishedBatchCounter);
        }
    }

    private Queue<List<Object>> createBatchChunks(List<Object> keys) {
        final Queue<List<Object>> chunks = new LinkedList<List<Object>>();
        final int loadBatchSize = getLoadBatchSize();
        int page = 0;
        List<Object> tmpKeys;
        while ((tmpKeys = getBatchChunk(keys, loadBatchSize, page++)) != null) {
            chunks.add(tmpKeys);
        }
        return chunks;
    }

    private List<Data> loadAndGet(List<Object> keys) {
        Map<Object, Object> entries = Collections.emptyMap();
        try {
            entries = mapContainer.getStore().loadAll(keys);
        } catch (Throwable t) {
            logger.warning("Could not load keys from map store", t);
        }
        return getKeyValueSequence(entries);
    }

    private List<Data> getKeyValueSequence(Map<Object, Object> entries) {
        if (entries == null || entries.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Data> keyValueSequence = new ArrayList<Data>();
        for (final Map.Entry<Object, Object> entry : entries.entrySet()) {
            final Object key = entry.getKey();
            final Object value = entry.getValue();
            final Data dataKey = mapServiceContext.toData(key);
            final Data dataValue = mapServiceContext.toData(value);
            keyValueSequence.add(dataKey);
            keyValueSequence.add(dataValue);
        }
        return keyValueSequence;
    }

    /**
     * Used to partition the list to chunks.
     *
     * @param list        to be paged.
     * @param batchSize   batch operation size.
     * @param chunkNumber batch chunk number.
     * @return sub-list of list if any or null.
     */
    private List<Object> getBatchChunk(List<Object> list, int batchSize, int chunkNumber) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        final int start = chunkNumber * batchSize;
        final int end = Math.min(start + batchSize, list.size());
        if (start >= end) {
            return null;
        }
        return list.subList(start, end);
    }

    private void sendOperation(List<Data> keyValueSequence, AtomicInteger finishedBatchCounter) {
        OperationService operationService = mapServiceContext.getNodeEngine().getOperationService();
        final Operation operation = createOperation(keyValueSequence, finishedBatchCounter);
        operationService.executeOperation(operation);
    }

    private Operation createOperation(List<Data> keyValueSequence, final AtomicInteger finishedBatchCounter) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final Operation operation = new PutFromLoadAllOperation(name, keyValueSequence);
        operation.setNodeEngine(nodeEngine);
        operation.setResponseHandler(new ResponseHandler() {
            @Override
            public void sendResponse(Object obj) {
                if (finishedBatchCounter.decrementAndGet() == 0) {
                    loaded.set(true);
                }
            }

            public boolean isLocal() {
                return true;
            }
        });
        operation.setPartitionId(partitionId);
        OperationAccessor.setCallerAddress(operation, nodeEngine.getThisAddress());
        operation.setCallerUuid(nodeEngine.getLocalMember().getUuid());
        operation.setServiceName(MapService.SERVICE_NAME);
        return operation;
    }

    private List<Object> convertDataKeysToObject(Collection<Data> keys) {
        if (keys.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Object> objectKeys = new ArrayList<Object>(keys.size());
        for (Data key : keys) {
            final Object objectKey = mapServiceContext.toObject(key);
            objectKeys.add(objectKey);
        }
        return objectKeys;
    }

    private void removeExistingKeys(Collection<Data> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        final ConcurrentMap<Data, Record> records = DefaultRecordStore.this.records;
        final Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            final Data nextKey = iterator.next();
            if (records.containsKey(nextKey)) {
                iterator.remove();
            }
        }
    }

    private void removeUnloadableKeys(Collection<Data> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        final long now = getNow();
        final Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            final Data key = iterator.next();
            final Record record = records.get(key);
            final long lastUpdateTime = record == null ? 0L : record.getLastUpdateTime();
            if (!mapDataStore.loadable(key, lastUpdateTime, now)) {
                iterator.remove();
            }
        }
    }

    private final class MapLoadAllTask implements Runnable {
        private final Map<Data, Object> keys;
        private final AtomicInteger checkIfMapLoaded;

        private MapLoadAllTask(Map<Data, Object> keys, AtomicInteger checkIfMapLoaded) {
            this.keys = keys;
            this.checkIfMapLoaded = checkIfMapLoaded;
        }

        public void run() {
            final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
            try {
                Map values = mapContainer.getStore().loadAll(keys.values());
                if (values == null || values.isEmpty()) {
                    if (checkIfMapLoaded.decrementAndGet() == 0) {
                        loaded.set(true);
                    }
                    return;
                }

                MapEntrySet entrySet = new MapEntrySet();
                for (Data dataKey : keys.keySet()) {
                    Object key = keys.get(dataKey);
                    Object value = values.get(key);
                    if (value != null) {
                        Data dataValue = mapServiceContext.toData(value);
                        entrySet.add(dataKey, dataValue);
                    }
                }
                PutAllOperation operation = new PutAllOperation(name, entrySet, true);
                operation.setNodeEngine(nodeEngine);
                operation.setResponseHandler(new ResponseHandler() {
                    @Override
                    public void sendResponse(Object obj) {
                        if (checkIfMapLoaded.decrementAndGet() == 0) {
                            loaded.set(true);
                        }
                    }

                    public boolean isLocal() {
                        return true;
                    }
                });
                operation.setPartitionId(partitionId);
                OperationAccessor.setCallerAddress(operation, nodeEngine.getThisAddress());
                operation.setCallerUuid(nodeEngine.getLocalMember().getUuid());
                operation.setServiceName(MapService.SERVICE_NAME);
                nodeEngine.getOperationService().executeOperation(operation);
            } catch (Exception e) {
                logger.warning("Exception while load all task:" + e.toString());
            }
        }
    }
}
