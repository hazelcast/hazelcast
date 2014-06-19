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
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.operation.PutAllOperation;
import com.hazelcast.map.operation.PutFromLoadAllOperation;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordFactory;
import com.hazelcast.map.writebehind.DelayedEntry;
import com.hazelcast.map.writebehind.WriteBehindQueue;
import com.hazelcast.map.writebehind.WriteBehindQueues;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author enesakar 1/17/13
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
    private final MapService mapService;
    private final RecordFactory recordFactory;
    private final ILogger logger;
    private final SizeEstimator sizeEstimator;
    private final AtomicBoolean loaded = new AtomicBoolean(false);
    private final WriteBehindQueue<DelayedEntry> writeBehindQueue;
    private LockStore lockStore;
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
     * Iterates over a pre-set entry count/percentage in one round.
     * Used in expiration logic for traversing entries. Initializes lazily.
     */
    private Iterator<DelayedEntry> evictionStagingAreaIterator;

    /**
     * If there is no clean-up caused by puts after some time,
     * count a number of gets and start eviction.
     */
    private int readCountBeforeCleanUp;

    /**
     * To check if a key has a delayed delete operation or not.
     */
    private final Set<Data> writeBehindWaitingDeletions;

    /**
     * A temporary living space for evicted data if we are using a write-behind map store. Because every eviction triggers a
     * map store flush and in write-behind mode this flush operation should not cause any inconsistencies like reading a stale value
     * from map store. To prevent this kind of inconsistencies, first we are searching an evicted entry in this space and if it is not there,
     * we are asking map store to load it. All read operations will use this staging area to return last set value on a specific key,
     * since there is a possibility that WBQ {@link com.hazelcast.map.writebehind.WriteBehindQueue} may contain more than one waiting operations
     * on a specific key.
     * <p/>
     * Method {@link com.hazelcast.map.DefaultRecordStore#cleanupEvictionStagingArea} will try to evict this staging area.
     */
    private final Map<Data, DelayedEntry> evictionStagingArea;

    /**
     * used in LRU eviction logic.
     */
    private long lruAccessSequenceNumber;

    public DefaultRecordStore(String name, MapService mapService, int partitionId) {
        this.name = name;
        this.partitionId = partitionId;
        this.mapService = mapService;
        this.mapContainer = mapService.getMapContainer(name);
        this.logger = mapService.getNodeEngine().getLogger(this.getName());
        this.recordFactory = mapContainer.getRecordFactory();
        this.lockStore = createLockStore();
        this.sizeEstimator = SizeEstimators.createMapSizeEstimator();
        this.writeBehindQueue = createWriteBehindQueue();
        this.writeBehindWaitingDeletions = createWriteBehindWaitingDeletionsSet();
        this.expirable = isRecordStoreExpirable();
        this.evictionStagingArea = createEvictionStagingArea();
        loadFromMapStore();
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
        final Collection<Data> processedKeys
                = mapContainer.getWriteBehindManager().flush(writeBehindQueue);
        for (Data pkey : processedKeys) {
            final Record record = records.get(pkey);
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
        removeFromWriteBehindWaitingDeletions(key);
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
            record = mapService.createRecord(name, key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value, now);
            updateSizeEstimator(calculateRecordSize(record));
        }
        removeFromWriteBehindWaitingDeletions(key);
        addToDelayedStore(key, record.getValue(), now);
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
        final NodeEngine nodeEngine = mapService.getNodeEngine();
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
        writeBehindQueue.clear();
        writeBehindWaitingDeletions.clear();
        evictionStagingArea.clear();
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
    public WriteBehindQueue<DelayedEntry> getWriteBehindQueue() {
        return writeBehindQueue;
    }

    @Override
    public void evictExpiredEntries(int percentage, boolean ownerPartition) {
        final long now = getNow();
        final int size = size();
        final int maxIterationCount = getMaxIterationCount(size, percentage);
        final int maxRetry = 3;
        int loop = 0;
        int evicteds = 0;
        while (true) {
            evicteds += evictExpiredEntries0(maxIterationCount, now, ownerPartition);
            if (evicteds >= maxIterationCount) {
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
            if (mapService.compare(name, value, record.getValue())) {
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
            temp.put(key, mapService.toData(records.get(key).getValue()));
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
        if (mapContainer.getStore() != null) {
            final Object value = loadFromStoreOrStagingArea(key);
            if (value != null) {
                record = mapService.createRecord(name, key, value, DEFAULT_TTL, getNow());
                records.put(key, record);
                if (enableIndex) {
                    saveIndex(record);
                }
                updateSizeEstimator(calculateRecordSize(record));
            }
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
        if (store != null) {
            // Use an ArrayList so that we don't trigger calls to equals or hashCode on the key objects
            Collection<Object> keysObject = new ArrayList<Object>(keysToDelete.size());
            for (Data key : keysToDelete) {
                keysObject.add(mapService.toObject(key));
            }

            store.deleteAll(keysObject);
        }

        removeIndex(keysToDelete);

        clearRecordsMap(lockedRecords);
        resetAccessSequenceNumber();
        writeBehindQueue.clear();
    }

    public void reset() {
        checkIfLoaded();

        clearRecordsMap(Collections.<Data, Record>emptyMap());
        resetSizeEstimator();
        resetAccessSequenceNumber();
        writeBehindQueue.clear();
        writeBehindWaitingDeletions.clear();
        evictionStagingArea.clear();
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
            putEvictionStagingArea(key, value, lastUpdateTime);
            mapService.interceptRemove(name, value);
            updateSizeEstimator(-calculateRecordSize(record));
            deleteRecord(key);
            removeIndex(key);
        }
        removeFromWriteBehindWaitingDeletions(key);
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
        addToDelayedStore(key, null, now);
    }

    @Override
    public boolean remove(Data key, Object testValue) {
        checkIfLoaded();
        final long now = getNow();
        earlyWriteCleanup(now);

        Record record = records.get(key);
        Object oldValue = null;
        boolean removed = false;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                oldValue = loadFromStoreOrStagingArea(key);
            }
            if (oldValue == null) {
                return false;
            }
        } else {
            oldValue = record.getValue();
        }
        if (mapService.compare(name, testValue, oldValue)) {
            mapService.interceptRemove(name, oldValue);
            removeIndex(key);
            mapStoreDelete(record, key, now);
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
        Object oldValue = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                oldValue = loadFromStoreOrStagingArea(key);
                if (oldValue != null) {
                    removeIndex(key);
                    mapStoreDelete(null, key, now);
                }
            }
        } else {
            oldValue = record.getValue();
            oldValue = mapService.interceptRemove(name, oldValue);
            if (oldValue != null) {
                removeIndex(key);
                mapStoreDelete(record, key, now);
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
        Record record = records.get(key);
        record = nullIfExpired(record);
        Object value = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                value = loadFromStoreOrStagingArea(key);
                if (value != null) {
                    record = mapService.createRecord(name, key, value, DEFAULT_TTL, now);
                    records.put(key, record);
                    saveIndex(record);
                    updateSizeEstimator(calculateRecordSize(record));
                }
            }
        } else {
            accessRecord(record, now);
            value = record.getValue();
        }
        value = mapService.interceptGet(name, value);
        postReadCleanUp(now);
        return value;
    }

    public Object loadFromStoreOrStagingArea(Data key) {
        if (hasWaitingWriteBehindDeleteOperation(key)) {
            return null;
        }
        final Object fromStagingArea = getFromEvictionStagingArea(key);
        return fromStagingArea == null ? mapContainer.getStore().load(mapService.toObject(key)) :
                fromStagingArea;
    }

    @Override
    public MapEntrySet getAll(Set<Data> keySet) {
        checkIfLoaded();
        final long now = getNow();
        final MapEntrySet mapEntrySet = new MapEntrySet();
        Map<Object, Data> keyMapForLoader = Collections.emptyMap();
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
        final Map<Object, Object> fromEvictionStagingArea = getFromEvictionStagingArea(keySet);
        for (Object o : fromEvictionStagingArea.keySet()) {
            keyMapForLoader.remove(o);
        }
        final Map<Object, Object> loadedKeys = mapContainer.getStore().loadAll(keyMapForLoader.keySet());
        for (Map.Entry entry : loadedKeys.entrySet()) {
            final Object objectKey = entry.getKey();
            Object value = entry.getValue();
            Data dataKey = keyMapForLoader.get(objectKey);
            if (hasWaitingWriteBehindDeleteOperation(dataKey)) {
                continue;
            }
            if (value != null) {
                Record record = mapService.createRecord(name, dataKey, value, DEFAULT_TTL, now);
                records.put(dataKey, record);
                saveIndex(record);
                updateSizeEstimator(calculateRecordSize(record));
            }
            value = mapService.interceptGet(name, value);
            if (value != null) {
                mapEntrySet.add(new AbstractMap.SimpleImmutableEntry(dataKey, mapService.toData(value)));
            }
        }
        postReadCleanUp(now);
        return mapEntrySet;
    }

    @Override
    public boolean containsKey(Data key) {
        checkIfLoaded();

        final long now = getNow();
        Record record = records.get(key);
        record = nullIfExpired(record);

        if (record == null) {
            if (mapContainer.getStore() != null) {
                Object value = loadFromStoreOrStagingArea(key);
                if (value != null) {
                    record = mapService.createRecord(name, key, value, DEFAULT_TTL, now);
                    records.put(key, record);
                    updateSizeEstimator(calculateRecordSize(record));
                }
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
            value = mapService.interceptPut(name, null, value);
            value = mapStoreWrite(key, value, null, now);
            record = mapService.createRecord(name, key, value, DEFAULT_TTL, now);
            records.put(key, record);
            // increase size.
            updateSizeEstimator(calculateRecordSize(record));
            saveIndex(record);
        } else {
            final Object oldValue = record.getValue();
            value = mapService.interceptPut(name, oldValue, value);
            value = mapStoreWrite(key, value, record, now);
            // if key exists before, first reduce size
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value, now);
            // then increase size
            updateSizeEstimator(calculateRecordSize(record));
            saveIndex(record);
        }
        removeFromWriteBehindWaitingDeletions(key);
    }

    public Object put(Data key, Object value, long ttl) {
        checkIfLoaded();
        final long now = getNow();
        earlyWriteCleanup(now);
        markRecordStoreExpirable(ttl);

        Record record = records.get(key);
        Object oldValue = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                oldValue = loadFromStoreOrStagingArea(key);
            }
            value = mapService.interceptPut(name, null, value);
            value = mapStoreWrite(key, value, null, now);
            record = mapService.createRecord(name, key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
            saveIndex(record);
        } else {
            oldValue = record.getValue();
            value = mapService.interceptPut(name, oldValue, value);
            value = mapStoreWrite(key, value, record, now);
            // if key exists before, first reduce size
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value, now);
            // then increase size.
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
            saveIndex(record);
        }
        removeFromWriteBehindWaitingDeletions(key);

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
            value = mapService.interceptPut(name, null, value);
            value = mapStoreWrite(key, value, null, now);
            record = mapService.createRecord(name, key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
            newRecord = true;
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            value = mapStoreWrite(key, value, record, now);
            // if key exists before, first reduce size
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value, now);
            // then increase size.
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);
        removeFromWriteBehindWaitingDeletions(key);

        return newRecord;
    }

    public boolean merge(Data key, EntryView mergingEntry, MapMergePolicy mergePolicy) {
        checkIfLoaded();
        final long now = getNow();
        earlyWriteCleanup(now);

        Record record = records.get(key);
        Object newValue;
        if (record == null) {
            final Object notExistingKey = mapService.toObject(key);
            final EntryView<Object, Object> nullEntryView = EntryViews.createNullEntryView(notExistingKey);
            newValue = mergePolicy.merge(name, mergingEntry, nullEntryView);
            if (newValue == null) {
                return false;
            }
            newValue = mapStoreWrite(key, newValue, null, now);
            record = mapService.createRecord(name, key, newValue, DEFAULT_TTL, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            Object oldValue = record.getValue();
            EntryView existingEntry = EntryViews.createSimpleEntryView(mapService.toObject(record.getKey()),
                    mapService.toObject(record.getValue()), record);
            newValue = mergePolicy.merge(name, mergingEntry, existingEntry);
            // existing entry will be removed
            if (newValue == null) {
                removeIndex(key);
                mapStoreDelete(record, key, now);
                // reduce size.
                updateSizeEstimator(-calculateRecordSize(record));
                //remove from map & invalidate.
                deleteRecord(key);
                return true;
            }
            // same with the existing entry so no need to mapstore etc operations.
            if (mapService.compare(name, newValue, oldValue)) {
                return true;
            }
            newValue = mapStoreWrite(key, newValue, record, now);
            updateSizeEstimator(-calculateRecordSize(record));
            recordFactory.setValue(record, newValue);
            updateSizeEstimator(calculateRecordSize(record));
        }
        saveIndex(record);
        return newValue != null;
    }

    // TODO why does not replace method load data from map store if currently not available in memory.
    public Object replace(Data dataKey, Object value) {
        checkIfLoaded();
        final long now = getNow();
        earlyWriteCleanup(now);

        Record record = records.get(dataKey);
        Object oldValue;
        if (record != null && record.getValue() != null) {
            oldValue = record.getValue();
            value = mapService.interceptPut(name, oldValue, value);
            value = mapStoreWrite(dataKey, value, record, now);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value, now);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            return null;
        }
        saveIndex(record);

        return oldValue;
    }

    public boolean replace(Data dataKey, Object testValue, Object newValue) {
        checkIfLoaded();
        final long now = getNow();
        earlyWriteCleanup(now);

        Record record = records.get(dataKey);
        if (record == null) {
            return false;
        }
        if (mapService.compare(name, record.getValue(), testValue)) {
            newValue = mapService.interceptPut(name, record.getValue(), newValue);
            newValue = mapStoreWrite(dataKey, newValue, record, now);
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
            value = mapService.interceptPut(name, null, value);
            record = mapService.createRecord(name, key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value, now);
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);
        removeFromWriteBehindWaitingDeletions(key);
    }

    @Override
    public Object putFromLoad(Data key, Object value, long ttl) {
        final long now = getNow();
        Record record = records.get(key);
        earlyWriteCleanup(now);
        markRecordStoreExpirable(ttl);

        Object oldValue = null;
        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            record = mapService.createRecord(name, key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            oldValue = record.getValue();
            value = mapService.interceptPut(name, record.getValue(), value);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value, now);
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);
        removeFromWriteBehindWaitingDeletions(key);
        return oldValue;
    }

    @Override
    public Object putFromLoad(Data key, Object value) {
        return putFromLoad(key, value, DEFAULT_TTL);
    }

    public boolean tryPut(Data key, Object value, long ttl) {
        checkIfLoaded();

        final long now = getNow();
        earlyWriteCleanup(now);
        markRecordStoreExpirable(ttl);

        Record record = records.get(key);
        if (record == null) {
            value = mapService.interceptPut(name, null, value);
            value = mapStoreWrite(key, value, null, now);
            record = mapService.createRecord(name, key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
        } else {
            value = mapService.interceptPut(name, record.getValue(), value);
            value = mapStoreWrite(key, value, record, now);
            updateSizeEstimator(-calculateRecordSize(record));
            setRecordValue(record, value, now);
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);
        removeFromWriteBehindWaitingDeletions(key);

        return true;
    }

    public Object putIfAbsent(Data key, Object value, long ttl) {
        checkIfLoaded();

        final long now = getNow();
        earlyWriteCleanup(now);
        markRecordStoreExpirable(ttl);

        Record record = records.get(key);
        Object oldValue = null;
        if (record == null) {
            if (mapContainer.getStore() != null) {
                oldValue = loadFromStoreOrStagingArea(key);
                if (oldValue != null) {
                    record = mapService.createRecord(name, key, oldValue, DEFAULT_TTL, now);
                    records.put(key, record);
                    updateSizeEstimator(calculateRecordSize(record));
                }
            }
        } else {
            accessRecord(record, now);
            oldValue = record.getValue();
        }
        if (oldValue == null) {
            value = mapService.interceptPut(name, null, value);
            value = mapStoreWrite(key, value, record, now);
            record = mapService.createRecord(name, key, value, ttl, now);
            records.put(key, record);
            updateSizeEstimator(calculateRecordSize(record));
            updateTtl(record, ttl);
        }
        saveIndex(record);
        removeFromWriteBehindWaitingDeletions(key);

        return oldValue;
    }

    private void loadFromMapStore() {
        final NodeEngine nodeEngine = mapService.getNodeEngine();
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
        cleanupEvictionStagingArea(now);

        if (mapContainer.isEvictionEnabled()) {
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
        cleanupEvictionStagingArea(now);

        if (mapContainer.isEvictionEnabled()) {
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
                mapContainer.getMapConfig(), mapService);
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

    private void addToWriteBehindWaitingDeletions(Data key) {
        if (!mapContainer.isWriteBehindMapStoreEnabled()) {
            return;
        }
        writeBehindWaitingDeletions.add(key);
    }

    @Override
    public void removeFromWriteBehindWaitingDeletions(Data key) {
        if (!mapContainer.isWriteBehindMapStoreEnabled()) {
            return;
        }
        writeBehindWaitingDeletions.remove(key);
    }


    private void initStagingAreaIterator() {
        if (evictionStagingAreaIterator == null || !evictionStagingAreaIterator.hasNext()) {
            evictionStagingAreaIterator = evictionStagingArea.values().iterator();
        }
    }

    private void cleanupEvictionStagingArea(long now) {
        if (!mapContainer.isWriteBehindMapStoreEnabled()) {
            return;
        }
        if (evictionStagingArea.isEmpty() || !inEvictableTimeWindow(now)) {
            return;
        }
        final long nextItemsStoreTimeInWriteBehindQueue = getNextItemsStoreTimeInWriteBehindQueue();
        final int size = evictionStagingArea.size();
        final int evictionPercentage = 20;
        int maxAllowedIterationCount = getMaxIterationCount(size, evictionPercentage);
        initStagingAreaIterator();
        while (evictionStagingAreaIterator.hasNext()) {
            if (maxAllowedIterationCount <= 0) {
                break;
            }
            --maxAllowedIterationCount;
            final DelayedEntry entry = evictionStagingAreaIterator.next();
            if (entry.getStoreTime() < nextItemsStoreTimeInWriteBehindQueue) {
                evictionStagingAreaIterator.remove();
            }
            initStagingAreaIterator();
            if (!evictionStagingAreaIterator.hasNext()) {
                break;
            }
        }
    }

    private long getNextItemsStoreTimeInWriteBehindQueue() {
        final DelayedEntry firstEntryInQueue = writeBehindQueue.get(0);
        if (firstEntryInQueue == null) {
            return 0L;
        }
        return firstEntryInQueue.getStoreTime();
    }

    private void putEvictionStagingArea(Data key, Object value, long lastUpdateTime) {
        assert value != null : String.format("value is null");
        assert lastUpdateTime > 0 : String.format("lastUpdateTime should be greater than 0, but found %d", lastUpdateTime);

        if (!mapContainer.isWriteBehindMapStoreEnabled()) {
            return;
        }
        final long storeTime = lastUpdateTime + getWriteDelayTime();
        final DelayedEntry<Void, Object> delayedEntry = DelayedEntry.createWithNullKey(value, storeTime);
        evictionStagingArea.put(key, delayedEntry);
    }

    private Object getFromEvictionStagingArea(Data key) {
        if (!mapContainer.isWriteBehindMapStoreEnabled()) {
            return null;
        }
        final DelayedEntry entryWithNullKey = evictionStagingArea.get(key);
        if (entryWithNullKey == null) {
            return null;
        }
        return mapService.toObject(entryWithNullKey.getValue());
    }

    private void removeFromEvictionStagingArea(Data key) {
        if (!mapContainer.isWriteBehindMapStoreEnabled()) {
            return;
        }
        final Object value = evictionStagingArea.remove(key);
        if (value == null) {
            return;
        }
    }

    private Map<Object, Object> getFromEvictionStagingArea(Set<Data> keys) {
        if (!mapContainer.isWriteBehindMapStoreEnabled()) {
            return Collections.emptyMap();
        }
        final Map<Object, Object> map = new HashMap<Object, Object>();
        for (Data key : keys) {
            final Object valueFromEvictionStagingArea = getFromEvictionStagingArea(key);
            if (valueFromEvictionStagingArea == null) {
                continue;
            }
            map.put(mapService.toObject(key), valueFromEvictionStagingArea);
        }
        return map;
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
        final NodeEngine nodeEngine = mapService.getNodeEngine();
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.submit("hz:map-loadAllKeys", new LoadAllKeysTask(keys, replaceExistingValues, loaded));
    }


    private boolean hasWaitingWriteBehindDeleteOperation(Data key) {
        return mapContainer.isWriteBehindMapStoreEnabled() && writeBehindWaitingDeletions.contains(key);
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
        final Record result = mapContainer.getReachabilityHandlerChain().isReachable(record,
                -1L, time);
        return result != null;
    }

    /**
     * - Sends eviction event.
     * - Invalidates near cache.
     *
     * @param key   the key to be processed.
     * @param value the value to be processed.
     */
    private void doPostEvictionOperations(Data key, Object value) {
        if (mapService.isNearCacheAndInvalidationEnabled(name)) {
            mapService.invalidateAllNearCaches(name, key);
        }
        EvictionHelper.fireEvent(key, value, name, mapService);
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
            SerializationService ss = mapService.getSerializationService();
            QueryableEntry queryableEntry = new QueryEntry(ss, dataKey, dataKey, record.getValue());
            indexService.saveEntryIndex(queryableEntry);
        }
    }

    private Object mapStoreWrite(Data key, Object value, Record record, long now) {
        final MapStoreWrapper store = mapContainer.getStore();
        if (store == null) {
            return value;
        }
        if (getWriteDelayTime() < 1L) {
            Object objectValue = mapService.toObject(value);
            store.store(mapService.toObject(key), objectValue);
            if (record != null) {
                record.onStore();
            }
            // if store is not a post-processing map-store, then avoid extra de-serialization phase.
            return store.isPostProcessingMapStore() ? objectValue : value;
        }
        addToDelayedStore(key, value, now);
        return value;
    }

    private void mapStoreDelete(Record record, Data key, long now) {
        final MapStoreWrapper store = mapContainer.getStore();
        if (store == null) {
            return;
        }
        final long writeDelay = getWriteDelayTime();
        if (writeDelay < 1L) {
            store.delete(mapService.toObject(key));
            // todo ea record will be deleted then why calling onStore
            if (record != null) {
                record.onStore();
            }
            return;
        }
        addToDelayedStore(key, null, now);
    }

    /**
     * Constructs and adds a {@link com.hazelcast.map.writebehind.DelayedEntry}
     * instance to write behind queue.
     *
     * @param key   to be stored.
     * @param value to be stored.
     */
    private void addToDelayedStore(Data key, Object value, long now) {
        if (!mapContainer.isWriteBehindMapStoreEnabled()) {
            return;
        }
        final long writeDelay = getWriteDelayTime();
        final long storeTime = now + writeDelay;
        final DelayedEntry<Data, Object> delayedEntry =
                DelayedEntry.create(key, value, storeTime, partitionId);
        // if value is null this is a delete operation.
        if (value == null) {
            addToWriteBehindWaitingDeletions(key);
            removeFromEvictionStagingArea(key);
        }
        writeBehindQueue.offer(delayedEntry);
    }

    private long getWriteDelayTime() {
        return mapContainer.getWriteDelayMillis();
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

    private WriteBehindQueue<DelayedEntry> createWriteBehindQueue() {
        final boolean writeBehindMapStoreEnabled = mapContainer.isWriteBehindMapStoreEnabled();
        if (!writeBehindMapStoreEnabled) {
            return WriteBehindQueues.emptyWriteBehindQueue();
        }
        final AtomicInteger counter = mapService.getWriteBehindQueueItemCounter();
        final int maxPerNodeWriteBehindQueueSize = mapService.getMaxPerNodeSizeOfWriteBehindQueue();
        return WriteBehindQueues.createDefaultWriteBehindQueue(maxPerNodeWriteBehindQueueSize, counter);
    }

    private Map<Data, DelayedEntry> createEvictionStagingArea() {
        final boolean writeBehindMapStoreEnabled = mapContainer.isWriteBehindMapStoreEnabled();
        if (!writeBehindMapStoreEnabled) {
            return Collections.emptyMap();
        }
        return new ConcurrentHashMap<Data, DelayedEntry>();
    }

    private Set<Data> createWriteBehindWaitingDeletionsSet() {
        final boolean writeBehindMapStoreEnabled = mapContainer.isWriteBehindMapStoreEnabled();
        if (!writeBehindMapStoreEnabled) {
            return Collections.emptySet();
        }
        return Collections.newSetFromMap(new ConcurrentHashMap());
    }

    private boolean isRecordStoreExpirable() {
        return mapContainer.getMapConfig().getMaxIdleSeconds() > 0
                || mapContainer.getMapConfig().getTimeToLiveSeconds() > 0;
    }

    private LockStore createLockStore() {
        NodeEngine nodeEngine = mapService.getNodeEngine();
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService == null) {
            return null;
        }
        return lockService.createLockStore(partitionId, new DefaultObjectNamespace(MapService.SERVICE_NAME, name));
    }

    private int getLoadBatchSize() {
        return mapService.getNodeEngine().getGroupProperties().MAP_LOAD_CHUNK_SIZE.getInteger();
    }


    private final class LoadAllKeysTask implements Runnable {

        private Collection<Data> keys;
        private boolean replaceExistingValues;
        private AtomicBoolean loaded;

        private LoadAllKeysTask(Collection<Data> keys, boolean replaceExistingValues, AtomicBoolean loaded) {
            this.keys = keys;
            this.replaceExistingValues = replaceExistingValues;
            this.loaded = loaded;
        }

        @Override
        public void run() {
            try {
                loadKeys(keys, replaceExistingValues);
            } catch (Throwable t) {
                logger.warning("Can not load data from store.", t);
            }
        }
    }

    private void loadKeys(Collection<Data> keys, boolean replaceExistingValues) {
        if (!replaceExistingValues) {
            removeExistingKeys(keys);
        }
        if (keys.isEmpty()) {
            loaded.set(true);
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
        if (entries.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Data> keyValueSequence = new ArrayList<Data>();
        for (final Map.Entry<Object, Object> entry : entries.entrySet()) {
            final Object key = entry.getKey();
            final Object value = entry.getValue();
            final Data dataKey = mapService.toData(key);
            final Data dataValue = mapService.toData(value);
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
        OperationService operationService = mapService.getNodeEngine().getOperationService();
        final Operation operation = createOperation(keyValueSequence, finishedBatchCounter);
        operationService.executeOperation(operation);
    }

    private Operation createOperation(List<Data> keyValueSequence, final AtomicInteger finishedBatchCounter) {
        final NodeEngine nodeEngine = mapService.getNodeEngine();
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
            final Object objectKey = mapService.toObject(key);
            objectKeys.add(objectKey);
        }
        return objectKeys;
    }

    private void removeExistingKeys(Collection<Data> keys) {
        final ConcurrentMap<Data, Record> records = DefaultRecordStore.this.records;
        final Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            final Data nextKey = iterator.next();
            if (records.containsKey(nextKey)) {
                iterator.remove();
            }
        }
    }

    private final class MapLoadAllTask implements Runnable {
        private Map<Data, Object> keys;
        private AtomicInteger checkIfMapLoaded;

        private MapLoadAllTask(Map<Data, Object> keys, AtomicInteger checkIfMapLoaded) {
            this.keys = keys;
            this.checkIfMapLoaded = checkIfMapLoaded;
        }

        public void run() {
            final NodeEngine nodeEngine = mapService.getNodeEngine();
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
                        Data dataValue = mapService.toData(value);
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
