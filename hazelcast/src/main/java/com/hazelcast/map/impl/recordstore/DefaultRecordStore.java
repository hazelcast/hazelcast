/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryView;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.EntryViews;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapKeyLoader;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.event.EntryEventData;
import com.hazelcast.map.impl.iterator.MapEntriesWithCursor;
import com.hazelcast.map.impl.iterator.MapKeysWithCursor;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindQueue;
import com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.wan.impl.CallerProvenance;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.map.impl.EntryViews.toLazyEntryView;
import static com.hazelcast.map.impl.ExpirationTimeSetter.setExpirationTimes;
import static com.hazelcast.map.impl.mapstore.MapDataStores.EMPTY_MAP_DATA_STORE;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static com.hazelcast.util.MapUtil.createHashMap;
import static java.util.Collections.emptyList;

/**
 * Default implementation of record-store.
 */
public class DefaultRecordStore extends AbstractEvictableRecordStore {

    protected final ILogger logger;
    protected final RecordStoreLoader recordStoreLoader;
    protected final MapKeyLoader keyLoader;

    /**
     * A collection of futures representing pending completion of the key and
     * value loading tasks.
     * The loadingFutures are modified by partition threads and can be accessed
     * by query threads.
     *
     * @see #loadAll(boolean)
     * @see #loadAllFromStore(List, boolean)
     */
    protected final Collection<Future> loadingFutures = new ConcurrentLinkedQueue<Future>();
    /**
     * The record store may be created with or without triggering the load.
     * This flag guards that the loading on create is invoked not more than
     * once should the record store be migrated.
     */
    private boolean loadedOnCreate;
    /**
     * Records if the record store on the migration source has been loaded.
     * If the record store has already been loaded, the migration target should
     * NOT trigger loading again on migration commit, otherwise it may trigger
     * key loading.
     */
    private boolean loadedOnPreMigration;

    private final IPartitionService partitionService;

    public DefaultRecordStore(MapContainer mapContainer, int partitionId,
                              MapKeyLoader keyLoader, ILogger logger) {
        super(mapContainer, partitionId);

        this.logger = logger;
        this.keyLoader = keyLoader;
        this.recordStoreLoader = createRecordStoreLoader(mapStoreContext);
        this.partitionService = mapServiceContext.getNodeEngine().getPartitionService();
    }

    @Override
    public MapDataStore<Data, Object> getMapDataStore() {
        return mapDataStore;
    }

    @Override
    public long softFlush() {
        updateStoreStats();
        return mapDataStore.softFlush();
    }

    /**
     * Flushes evicted records to map store.
     *
     * @param recordsToBeFlushed records to be flushed to map-store.
     * @param backup             <code>true</code> if backup, false otherwise.
     */
    private void flush(Collection<Record> recordsToBeFlushed, boolean backup) {
        Iterator<Record> iterator = recordsToBeFlushed.iterator();
        while (iterator.hasNext()) {
            Record record = iterator.next();
            mapDataStore.flush(record.getKey(), record.getValue(), backup);
        }
    }

    @Override
    public Record getRecord(Data key) {
        return storage.get(key);
    }

    @Override
    public void putRecord(Data key, Record record) {
        markRecordStoreExpirable(record.getTtl(), record.getMaxIdle());
        storage.put(key, record);
        mutationObserver.onReplicationPutRecord(key, record);
        updateStatsOnPut(record.getHits());
    }

    @Override
    public Record putBackup(Data key, Object value, CallerProvenance provenance) {
        return putBackup(key, value, DEFAULT_TTL, DEFAULT_MAX_IDLE, false, provenance);
    }

    @Override
    public Record putBackup(Data key, Object value, long ttl, long maxIdle, boolean putTransient, CallerProvenance provenance) {
        long now = getNow();
        markRecordStoreExpirable(ttl, maxIdle);

        Record record = getRecordOrNull(key, now, true);
        if (record == null) {
            record = createRecord(value, ttl, maxIdle, now);
            storage.put(key, record);
            mutationObserver.onPutRecord(key, record);
        } else {
            updateRecord(key, record, value, now, true);
        }

        if (persistenceEnabledFor(provenance)) {
            if (putTransient) {
                mapDataStore.addTransient(key, now);
            } else {
                mapDataStore.addBackup(key, value, now);
            }
        }
        return record;
    }

    @Override
    public Iterator<Record> iterator() {
        return new ReadOnlyRecordIterator(storage.values());
    }

    @Override
    public Iterator<Record> iterator(long now, boolean backup) {
        return new ReadOnlyRecordIterator(storage.values(), now, backup);
    }

    @Override
    public MapKeysWithCursor fetchKeys(int tableIndex, int size) {
        return storage.fetchKeys(tableIndex, size);
    }

    @Override
    public MapEntriesWithCursor fetchEntries(int tableIndex, int size) {
        return storage.fetchEntries(tableIndex, size, serializationService);
    }

    @Override
    public Iterator<Record> loadAwareIterator(long now, boolean backup) {
        checkIfLoaded();
        return iterator(now, backup);
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
        return storage.size();
    }

    @Override
    public boolean isEmpty() {
        checkIfLoaded();
        return storage.isEmpty();
    }

    @Override
    public boolean containsValue(Object value) {
        checkIfLoaded();
        long now = getNow();
        Collection<Record> records = storage.values();

        if (!records.isEmpty()) {
            // optimisation to skip serialisation/deserialisation
            // in each call to RecordComparator.isEqual()
            value = inMemoryFormat == InMemoryFormat.OBJECT
                    ? serializationService.toObject(value)
                    : serializationService.toData(value);
        }

        for (Record record : records) {
            if (getOrNullIfExpired(record, now, false) == null) {
                continue;
            }
            if (recordComparator.isEqual(value, record.getValue())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean txnLock(Data key, String caller, long threadId, long referenceId, long ttl, boolean blockReads) {
        checkIfLoaded();
        return lockStore != null && lockStore.txnLock(key, caller, threadId, referenceId, ttl, blockReads);
    }

    @Override
    public boolean extendLock(Data key, String caller, long threadId, long ttl) {
        checkIfLoaded();
        return lockStore != null && lockStore.extendLeaseTime(key, caller, threadId, ttl);
    }

    @Override
    public boolean localLock(Data key, String caller, long threadId, long referenceId, long ttl) {
        checkIfLoaded();
        return lockStore != null && lockStore.localLock(key, caller, threadId, referenceId, ttl);
    }

    @Override
    public boolean unlock(Data key, String caller, long threadId, long referenceId) {
        checkIfLoaded();
        return lockStore != null && lockStore.unlock(key, caller, threadId, referenceId);
    }

    @Override
    public boolean lock(Data key, String caller, long threadId, long referenceId, long ttl) {
        checkIfLoaded();
        return lockStore != null && lockStore.lock(key, caller, threadId, referenceId, ttl);
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
        return lockStore != null && lockStore.shouldBlockReads(key);
    }

    @Override
    public boolean canAcquireLock(Data key, String caller, long threadId) {
        return lockStore == null || lockStore.canAcquireLock(key, caller, threadId);
    }

    @Override
    public boolean isLockedBy(Data key, String caller, long threadId) {
        return lockStore != null && lockStore.isLockedBy(key, caller, threadId);
    }

    @Override
    public String getLockOwnerInfo(Data key) {
        return lockStore != null ? lockStore.getOwnerInfo(key) : null;
    }

    @Override
    public Record loadRecordOrNull(Data key, boolean backup, Address callerAddress) {
        Record record = null;
        Object value = mapDataStore.load(key);
        if (value != null) {
            record = createRecord(value, DEFAULT_TTL, DEFAULT_MAX_IDLE, getNow());
            storage.put(key, record);
            mutationObserver.onLoadRecord(key, record);
            if (!backup) {
                saveIndex(record, null);
                mapEventPublisher.publishEvent(callerAddress, name, EntryEventType.LOADED,
                        key, null, value, null);
            }
            evictEntries(key);
        }
        // here, we are only publishing events for loaded entries. This is required for notifying query-caches
        // otherwise query-caches cannot see loaded entries
        if (!backup && record != null && hasQueryCache()) {
            addEventToQueryCache(record);
        }
        return record;
    }

    protected List<Data> getKeysFromRecords(Collection<Record> clearableRecords) {
        List<Data> keys = new ArrayList<Data>(clearableRecords.size());
        for (Record clearableRecord : clearableRecords) {
            keys.add(clearableRecord.getKey());
        }
        return keys;
    }

    protected int removeRecords(Collection<Record> recordsToRemove) {
        return removeOrEvictRecords(recordsToRemove, false);
    }

    protected int evictRecords(Collection<Record> recordsToEvict) {
        return removeOrEvictRecords(recordsToEvict, true);
    }

    private int removeOrEvictRecords(Collection<Record> recordsToRemove, boolean eviction) {
        if (CollectionUtil.isEmpty(recordsToRemove)) {
            return 0;
        }
        int removalSize = recordsToRemove.size();
        Iterator<Record> iterator = recordsToRemove.iterator();
        while (iterator.hasNext()) {
            Record record = iterator.next();
            if (eviction) {
                mutationObserver.onEvictRecord(record.getKey(), record);
            } else {
                mutationObserver.onRemoveRecord(record.getKey(), record);
            }
            storage.removeRecord(record);
            iterator.remove();
        }
        return removalSize;
    }

    protected Collection<Record> getNotLockedRecords() {
        Set<Data> lockedKeySet = lockStore == null ? null : lockStore.getLockedKeys();
        if (CollectionUtil.isEmpty(lockedKeySet)) {
            return storage.values();
        }

        int notLockedKeyCount = storage.size() - lockedKeySet.size();
        if (notLockedKeyCount <= 0) {
            return emptyList();
        }

        List<Record> notLockedRecords = new ArrayList<Record>(notLockedKeyCount);
        Collection<Record> records = storage.values();
        for (Record record : records) {
            if (!lockedKeySet.contains(record.getKey())) {
                notLockedRecords.add(record);
            }
        }
        return notLockedRecords;
    }

    @Override
    public Object evict(Data key, boolean backup) {
        Record record = storage.get(key);
        Object value = null;
        if (record != null) {
            value = record.getValue();
            mapDataStore.flush(key, value, backup);
            removeIndex(record);
            mutationObserver.onEvictRecord(key, record);
            storage.removeRecord(record);
            if (!backup) {
                mapServiceContext.interceptRemove(name, value);
            }
        }
        return value;
    }

    @Override
    public int evictAll(boolean backup) {
        checkIfLoaded();

        Collection<Record> evictableRecords = getNotLockedRecords();
        flush(evictableRecords, backup);
        removeIndex(evictableRecords);
        return evictRecords(evictableRecords);
    }

    @Override
    public void removeBackup(Data key, CallerProvenance provenance) {
        long now = getNow();

        Record record = getRecordOrNull(key, now, true);
        if (record == null) {
            return;
        }
        mutationObserver.onRemoveRecord(key, record);
        storage.removeRecord(record);
        if (persistenceEnabledFor(provenance)) {
            mapDataStore.removeBackup(key, now);
        }
    }

    @Override
    public boolean delete(Data key, CallerProvenance provenance) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, false);
        if (record == null) {
            if (persistenceEnabledFor(provenance)) {
                mapDataStore.remove(key, now);
            }
        } else {
            return removeRecord(key, record, now, provenance) != null;
        }
        return false;
    }

    @Override
    public Object remove(Data key, CallerProvenance provenance) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, false);
        Object oldValue;
        if (record == null) {
            oldValue = mapDataStore.load(key);
            if (oldValue != null && persistenceEnabledFor(provenance)) {
                mapDataStore.remove(key, now);
            }
        } else {
            oldValue = removeRecord(key, record, now, provenance);
        }
        return oldValue;
    }

    @Override
    public boolean remove(Data key, Object testValue) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, false);
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
        if (recordComparator.isEqual(testValue, oldValue)) {
            mapServiceContext.interceptRemove(name, oldValue);
            removeIndex(record);
            mapDataStore.remove(key, now);
            onStore(record);
            mutationObserver.onRemoveRecord(record.getKey(), record);
            storage.removeRecord(record);
            removed = true;
        }
        return removed;
    }

    @Override
    public Object get(Data key, boolean backup, Address
            callerAddress) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, backup);
        if (record == null) {
            record = loadRecordOrNull(key, backup, callerAddress);
        } else {
            accessRecord(record, now);
        }
        Object value = record == null ? null : record.getValue();
        value = mapServiceContext.interceptGet(name, value);

        return value;
    }

    @Override
    public Data readBackupData(Data key) {
        Record record = getRecord(key);

        if (record == null) {
            return null;
        }

        Object value = record.getValue();
        mapServiceContext.interceptAfterGet(name, value);
        // this serialization step is needed not to expose the object, see issue 1292
        return mapServiceContext.toData(value);
    }

    @Override
    public MapEntries getAll(Set<Data> keys, Address
            callerAddress) {
        checkIfLoaded();
        long now = getNow();

        MapEntries mapEntries = new MapEntries(keys.size());

        Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            Data key = iterator.next();
            Record record = getRecordOrNull(key, now, false);
            if (record != null) {
                addMapEntrySet(key, record.getValue(), mapEntries);
                accessRecord(record, now);
                iterator.remove();
            }
        }

        Map loadedEntries = loadEntries(keys, callerAddress);
        addMapEntrySet(loadedEntries, mapEntries);

        return mapEntries;
    }

    protected Map<Data, Object> loadEntries(Set<Data> keys, Address callerAddress) {
        Map loadedEntries = mapDataStore.loadAll(keys);
        if (loadedEntries == null || loadedEntries.isEmpty()) {
            return Collections.emptyMap();
        }

        // holds serialized keys and if values are serialized, also holds them in serialized format.
        Map<Data, Object> resultMap = createHashMap(loadedEntries.size());

        // add loaded key-value pairs to this record-store.
        Set entrySet = loadedEntries.entrySet();
        for (Object object : entrySet) {
            Map.Entry entry = (Map.Entry) object;

            Data key = toData(entry.getKey());
            Object value = entry.getValue();

            resultMap.put(key, value);

            putFromLoad(key, value, callerAddress);

        }

        if (hasQueryCache()) {
            for (Data key : resultMap.keySet()) {
                Record record = storage.get(key);
                // here we are only publishing events for loaded entries. This is required for notifying query-caches
                // otherwise query-caches cannot see loaded entries
                addEventToQueryCache(record);
            }
        }
        return resultMap;
    }

    protected void addMapEntrySet(Object key, Object value, MapEntries mapEntries) {
        if (key == null || value == null) {
            return;
        }
        value = mapServiceContext.interceptGet(name, value);
        Data dataKey = mapServiceContext.toData(key);
        Data dataValue = mapServiceContext.toData(value);
        mapEntries.add(dataKey, dataValue);
    }

    protected void addMapEntrySet(Map<Object, Object> entries, MapEntries mapEntries) {
        for (Map.Entry<Object, Object> entry : entries.entrySet()) {
            addMapEntrySet(entry.getKey(), entry.getValue(), mapEntries);
        }
    }

    @Override
    public boolean existInMemory(Data key) {
        return storage.containsKey(key);
    }

    @Override
    public boolean containsKey(Data key, Address callerAddress) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, false);
        if (record == null) {
            record = loadRecordOrNull(key, false, callerAddress);
        }
        boolean contains = record != null;
        if (contains) {
            accessRecord(record, now);
        }

        return contains;
    }

    /**
     * @return {@code true} if this IMap has any query-cache, otherwise return {@code false}
     */
    public boolean hasQueryCache() {
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        PublisherContext publisherContext = queryCacheContext.getPublisherContext();
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        PublisherRegistry publisherRegistry = mapPublisherRegistry.getOrNull(name);
        return publisherRegistry != null;
    }

    private void addEventToQueryCache(Record record) {
        EntryEventData eventData = new EntryEventData(thisAddress.toString(), name, thisAddress,
                record.getKey(), mapServiceContext.toData(record.getValue()),
                null, null, ADDED.getType());

        mapEventPublisher.addEventToQueryCache(eventData);
    }

    @Override
    public void setTTL(Data key, long ttl) {
        if (mapServiceContext.getNodeEngine().getClusterService().getClusterVersion().isLessThan(Versions.V3_11)) {
            throw new UnsupportedOperationException("Modifying TTL is available when cluster version is 3.11 or higher");
        }
        long now = getNow();
        markRecordStoreExpirable(ttl, DEFAULT_MAX_IDLE);
        Record record = getRecordOrNull(key, now, false);
        if (record != null) {
            setExpirationTimes(ttl, DEFAULT_MAX_IDLE, record, mapContainer.getMapConfig(), true);
        }
    }

    public Object set(Data dataKey, Object value, long ttl, long maxIdle) {
        return putInternal(dataKey, value, ttl, maxIdle, false, true);
    }

    @Override
    public Object put(Data key, Object value, long ttl, long maxIdle) {
        return putInternal(key, value, ttl, maxIdle, true, true);
    }

    protected Object putInternal(Data key, Object value, long ttl, long maxIdle, boolean loadFromStore, boolean countAsAccess) {
        checkIfLoaded();

        long now = getNow();
        markRecordStoreExpirable(ttl, maxIdle);

        Record record = getRecordOrNull(key, now, false);
        Object oldValue = record == null ? (loadFromStore ? mapDataStore.load(key) : null) : record.getValue();
        value = mapServiceContext.interceptPut(name, oldValue, value);
        value = mapDataStore.add(key, value, now);
        onStore(record);

        if (record == null) {
            record = createRecord(value, ttl, maxIdle, now);
            storage.put(key, record);
            mutationObserver.onPutRecord(key, record);
        } else {
            updateRecord(key, record, value, now, countAsAccess);
            setExpirationTimes(ttl, maxIdle, record, mapContainer.getMapConfig(), false);
        }

        saveIndex(record, oldValue);
        return oldValue;
    }

    @Override
    public boolean merge(MapMergeTypes mergingEntry, SplitBrainMergePolicy<Data, MapMergeTypes> mergePolicy) {
        return merge(mergingEntry, mergePolicy, CallerProvenance.NOT_WAN);
    }

    @Override
    public boolean merge(MapMergeTypes mergingEntry,
                         SplitBrainMergePolicy<Data, MapMergeTypes> mergePolicy,
                         CallerProvenance provenance) {
        checkIfLoaded();
        long now = getNow();

        serializationService.getManagedContext().initialize(mergingEntry);
        serializationService.getManagedContext().initialize(mergePolicy);

        Data key = mergingEntry.getKey();
        Record record = getRecordOrNull(key, now, false);
        Object newValue;
        Object oldValue = null;
        if (record == null) {
            newValue = mergePolicy.merge(mergingEntry, null);
            if (newValue == null) {
                return false;
            }

            newValue = persistenceEnabledFor(provenance) ? mapDataStore.add(key, newValue, now) : newValue;
            record = createRecord(newValue, DEFAULT_TTL, DEFAULT_MAX_IDLE, now);
            mergeRecordExpiration(record, mergingEntry);
            storage.put(key, record);
            mutationObserver.onPutRecord(key, record);
        } else {
            oldValue = record.getValue();
            MapMergeTypes existingEntry = createMergingEntry(serializationService, record);
            newValue = mergePolicy.merge(mergingEntry, existingEntry);
            // existing entry will be removed
            if (newValue == null) {
                removeIndex(record);
                if (persistenceEnabledFor(provenance)) {
                    mapDataStore.remove(key, now);
                }
                onStore(record);
                mutationObserver.onRemoveRecord(key, record);
                storage.removeRecord(record);
                return true;
            }
            if (newValue == mergingEntry.getValue()) {
                mergeRecordExpiration(record, mergingEntry);
            }
            // same with the existing entry so no need to map-store etc operations.
            if (recordComparator.isEqual(newValue, oldValue)) {
                return true;
            }
            newValue = persistenceEnabledFor(provenance) ? mapDataStore.add(key, newValue, now) : newValue;
            onStore(record);
            mutationObserver.onUpdateRecord(key, record, newValue);
            storage.updateRecordValue(key, record, newValue);
        }
        saveIndex(record, oldValue);
        return newValue != null;
    }

    @Override
    public boolean merge(Data key, EntryView mergingEntry, MapMergePolicy mergePolicy) {
        return merge(key, mergingEntry, mergePolicy, CallerProvenance.NOT_WAN);
    }

    @Override
    public boolean merge(Data key, EntryView mergingEntry,
                         MapMergePolicy mergePolicy, CallerProvenance provenance) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, false);
        mergingEntry = toLazyEntryView(mergingEntry, serializationService, mergePolicy);
        Object newValue;
        Object oldValue = null;
        if (record == null) {
            Object notExistingKey = mapServiceContext.toObject(key);
            EntryView nullEntryView = EntryViews.createNullEntryView(notExistingKey);
            newValue = mergePolicy.merge(name, mergingEntry, nullEntryView);
            if (newValue == null) {
                return false;
            }

            newValue = persistenceEnabledFor(provenance) ? mapDataStore.add(key, newValue, now) : newValue;
            record = createRecord(newValue, DEFAULT_TTL, DEFAULT_MAX_IDLE, now);
            mergeRecordExpiration(record, mergingEntry);
            storage.put(key, record);
            mutationObserver.onPutRecord(key, record);
        } else {
            oldValue = record.getValue();
            EntryView existingEntry = EntryViews.createLazyEntryView(record.getKey(), record.getValue(),
                    record, serializationService, mergePolicy);
            newValue = mergePolicy.merge(name, mergingEntry, existingEntry);
            // existing entry will be removed
            if (newValue == null) {
                removeIndex(record);
                if (persistenceEnabledFor(provenance)) {
                    mapDataStore.remove(key, now);
                }
                onStore(record);
                mutationObserver.onRemoveRecord(key, record);
                storage.removeRecord(record);
                return true;
            }
            if (newValue == mergingEntry.getValue()) {
                mergeRecordExpiration(record, mergingEntry);
            }
            // same with the existing entry so no need to map-store etc operations.
            if (recordComparator.isEqual(newValue, oldValue)) {
                return true;
            }
            newValue = persistenceEnabledFor(provenance) ? mapDataStore.add(key, newValue, now) : newValue;
            onStore(record);
            mutationObserver.onUpdateRecord(key, record, newValue);
            storage.updateRecordValue(key, record, newValue);
        }
        saveIndex(record, oldValue);
        return newValue != null;
    }

    // TODO why does not replace method load data from map store if currently not available in memory.
    @Override
    public Object replace(Data key, Object update) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, false);
        if (record == null || record.getValue() == null) {
            return null;
        }
        Object oldValue = record.getValue();
        update = mapServiceContext.interceptPut(name, oldValue, update);
        update = mapDataStore.add(key, update, now);
        onStore(record);
        updateRecord(key, record, update, now, true);
        setExpirationTimes(record.getTtl(), record.getMaxIdle(), record, mapContainer.getMapConfig(), false);
        saveIndex(record, oldValue);
        return oldValue;
    }
    @Override
    public boolean replace(Data key, Object expect, Object update) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, false);
        if (record == null) {
            return false;
        }
        MapServiceContext mapServiceContext = this.mapServiceContext;
        Object current = record.getValue();
        if (!recordComparator.isEqual(expect, current)) {
            return false;
        }
        update = mapServiceContext.interceptPut(name, current, update);
        update = mapDataStore.add(key, update, now);
        onStore(record);
        updateRecord(key, record, update, now, true);
        setExpirationTimes(record.getTtl(), record.getMaxIdle(), record, mapContainer.getMapConfig(), false);
        saveIndex(record, current);
        return true;
    }

    @Override
    public Object putTransient(Data key, Object value, long ttl, long maxIdle) {
        checkIfLoaded();
        long now = getNow();
        markRecordStoreExpirable(ttl, maxIdle);

        Record record = getRecordOrNull(key, now, false);
        Object oldValue = null;
        if (record == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            record = createRecord(value, ttl, maxIdle, now);
            storage.put(key, record);
            mutationObserver.onPutRecord(key, record);
        } else {
            oldValue = record.getValue();
            value = mapServiceContext.interceptPut(name, oldValue, value);
            updateRecord(key, record, value, now, true);
            setExpirationTimes(ttl, maxIdle, record, mapContainer.getMapConfig(), false);
        }
        saveIndex(record, oldValue);
        mapDataStore.addTransient(key, now);
        return oldValue;
    }

    @Override
    public Object putFromLoad(Data key, Object value, Address callerAddress) {
        return putFromLoadInternal(key, value, DEFAULT_TTL, DEFAULT_MAX_IDLE, false, callerAddress);
    }

    @Override
    public Object putFromLoadBackup(Data key, Object value) {
        return putFromLoadInternal(key, value, DEFAULT_TTL, DEFAULT_MAX_IDLE, true, null);
    }

    private Object putFromLoadInternal(Data key, Object value, long ttl, long maxIdle, boolean backup, Address callerAddress) {
        if (!isKeyAndValueLoadable(key, value)) {
            return null;
        }

        long now = getNow();

        if (shouldEvict()) {
            return null;
        }

        markRecordStoreExpirable(ttl, maxIdle);

        Record record = getRecordOrNull(key, now, false);
        Object oldValue = null;
        EntryEventType entryEventType = null;
        if (record == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            record = createRecord(value, ttl, maxIdle, now);
            storage.put(key, record);
            if (canPublishLoadEvent()) {
                mutationObserver.onLoadRecord(key, record);
            } else {
                mutationObserver.onPutRecord(key, record);
            }
        } else {
            oldValue = record.getValue();
            value = mapServiceContext.interceptPut(name, oldValue, value);
            updateRecord(key, record, value, now, true);
            setExpirationTimes(ttl, maxIdle, record, mapContainer.getMapConfig(), false);

            entryEventType = UPDATED;

        }
        if (!backup) {
            saveIndex(record, oldValue);

            if (entryEventType == UPDATED) {
                mapEventPublisher.publishEvent(callerAddress, name, EntryEventType.UPDATED, key, oldValue, value);
            } else {
                if (canPublishLoadEvent()) {
                    mapEventPublisher.publishEvent(callerAddress, name, EntryEventType.LOADED, key, null, value);
                } else {
                    mapEventPublisher.publishEvent(callerAddress, name, EntryEventType.ADDED, key, null, value);
                }
            }
        }
        return oldValue;
    }

    private boolean canPublishLoadEvent() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        ClusterService clusterService = nodeEngine.getClusterService();
        boolean version311OrLater = clusterService.getClusterVersion().isGreaterOrEqual(Versions.V3_11);
        boolean addEventPublishingEnabled = mapContainer.isAddEventPublishingEnabled();
        return version311OrLater && !addEventPublishingEnabled;
    }

    protected boolean isKeyAndValueLoadable(Data key, Object value) {
        if (key == null) {
            logger.warning("Found an attempt to load a null key from map-store, ignoring it.");
            return false;
        }

        if (value == null) {
            logger.warning("Found an attempt to load a null value from map-store, ignoring it.");
            return false;
        }

        if (partitionService.getPartitionId(key) != partitionId) {
            throw new IllegalStateException("MapLoader loaded an item belongs to a different partition");
        }

        return true;
    }

    @Override
    public boolean setWithUncountedAccess(Data dataKey, Object value, long ttl, long maxIdle) {
        Object oldValue = putInternal(dataKey, value, ttl, maxIdle, false, false);
        return oldValue == null;
    }

    @Override
    public Object putIfAbsent(Data key, Object value, long ttl, long maxIdle, Address callerAddress) {
        checkIfLoaded();
        long now = getNow();
        markRecordStoreExpirable(ttl, maxIdle);

        Record record = getRecordOrNull(key, now, false);
        Object oldValue;
        if (record == null) {
            oldValue = mapDataStore.load(key);
            if (oldValue != null) {
                record = createRecord(oldValue, DEFAULT_TTL, DEFAULT_MAX_IDLE, now);
                storage.put(key, record);

                mutationObserver.onPutRecord(key, record);
                mapEventPublisher.publishEvent(callerAddress, name, EntryEventType.LOADED, key, null, oldValue);
            }
        } else {
            accessRecord(record, now);
            oldValue = record.getValue();
        }
        if (oldValue == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            value = mapDataStore.add(key, value, now);
            onStore(record);
            record = createRecord(value, ttl, maxIdle, now);
            storage.put(key, record);
            mutationObserver.onPutRecord(key, record);

            setExpirationTimes(ttl, maxIdle, record, mapContainer.getMapConfig(), false);
        }
        saveIndex(record, oldValue);
        return oldValue;
    }

    protected Object removeRecord(Data key, @Nonnull Record record,
                                  long now, CallerProvenance provenance) {
        Object oldValue = record.getValue();
        oldValue = mapServiceContext.interceptRemove(name, oldValue);
        if (oldValue != null) {
            removeIndex(record);
            if (persistenceEnabledFor(provenance)) {
                mapDataStore.remove(key, now);
            }
            onStore(record);
        }
        mutationObserver.onRemoveRecord(key, record);
        storage.removeRecord(record);
        return oldValue;
    }

    @Override
    public Record getRecordOrNull(Data key) {
        long now = getNow();
        return getRecordOrNull(key, now, false);
    }

    protected Record getRecordOrNull(Data key, long now,
                                     boolean backup) {
        Record record = storage.get(key);
        if (record == null) {
            return null;
        }
        return getOrNullIfExpired(record, now, backup);
    }

    protected void onStore(Record record) {
        if (record == null || mapDataStore == EMPTY_MAP_DATA_STORE) {
            return;
        }

        record.onStore();
    }

    private void updateStoreStats() {
        if (!(mapDataStore instanceof WriteBehindStore)
                || !mapContainer.getMapConfig().isStatisticsEnabled()) {
            return;
        }

        long now = Clock.currentTimeMillis();
        WriteBehindQueue<DelayedEntry> writeBehindQueue = ((WriteBehindStore) mapDataStore).getWriteBehindQueue();
        List<DelayedEntry> delayedEntries = writeBehindQueue.asList();
        for (DelayedEntry delayedEntry : delayedEntries) {
            Record record = getRecordOrNull(toData(delayedEntry.getKey()), now, false);
            onStore(record);
        }
    }

    @Override
    public boolean isKeyLoadFinished() {
        return keyLoader.isKeyLoadFinished();
    }

    @Override
    public void checkIfLoaded() {
        if (loadingFutures.isEmpty()) {
            return;
        }

        if (isLoaded()) {
            List<Future> doneFutures = null;
            try {
                doneFutures = FutureUtil.getAllDone(loadingFutures);
                // check all finished loading futures for exceptions
                FutureUtil.checkAllDone(doneFutures);
            } catch (Exception e) {
                logger.severe("Exception while loading map " + name, e);
                throw ExceptionUtil.rethrow(e);
            } finally {
                loadingFutures.removeAll(doneFutures);
            }
        } else {
            keyLoader.triggerLoadingWithDelay();
            throw new RetryableHazelcastException("Map " + getName()
                    + " is still loading data from external store");
        }
    }

    @Override
    public void startLoading() {
        if (logger.isFinestEnabled()) {
            logger.finest("StartLoading invoked " + getStateMessage());
        }
        if (mapStoreContext.isMapLoader() && !loadedOnCreate) {
            if (!loadedOnPreMigration) {
                if (logger.isFinestEnabled()) {
                    logger.finest("Triggering load " + getStateMessage());
                }
                loadedOnCreate = true;
                loadingFutures.add(keyLoader.startInitialLoad(mapStoreContext, partitionId));
            } else {
                if (logger.isFinestEnabled()) {
                    logger.finest("Promoting to loaded on migration " + getStateMessage());
                }
                keyLoader.promoteToLoadedOnMigration();
            }
        }
    }

    @Override
    public void setPreMigrationLoadedStatus(boolean loaded) {
        loadedOnPreMigration = loaded;
    }

    @Override
    public boolean isLoaded() {
        return FutureUtil.allDone(loadingFutures);
    }

    @Override
    public void loadAll(boolean replaceExistingValues) {
        if (logger.isFinestEnabled()) {
            logger.finest("loadAll invoked " + getStateMessage());
        }

        logger.info("Starting to load all keys for map " + name + " on partitionId=" + partitionId);
        Future<?> loadingKeysFuture = keyLoader.startLoading(mapStoreContext, replaceExistingValues);
        loadingFutures.add(loadingKeysFuture);
    }

    @Override
    public void loadAllFromStore(List<Data> keys,
                                 boolean replaceExistingValues) {
        if (!keys.isEmpty()) {
            Future f = recordStoreLoader.loadValues(keys, replaceExistingValues);
            loadingFutures.add(f);
        }

        // We should not track key loading here. IT's not key loading but values loading.
        // Apart from that it's irrelevant for RECEIVER nodes. SENDER and SENDER_BACKUP will track the key-loading anyway.
        // Fixes https://github.com/hazelcast/hazelcast/issues/9255
    }

    @Override
    public void updateLoadStatus(boolean lastBatch, Throwable
            exception) {
        keyLoader.trackLoading(lastBatch, exception);

        if (lastBatch) {
            logger.finest("Completed loading map " + name + " on partitionId=" + partitionId);
        }
    }

    @Override
    public void maybeDoInitialLoad() {
        if (keyLoader.shouldDoInitialLoad()) {
            loadAll(false);
        }
    }

    private String getStateMessage() {
        return "on partitionId=" + partitionId + " on " + mapServiceContext.getNodeEngine().getThisAddress()
                + " loadedOnCreate=" + loadedOnCreate + " loadedOnPreMigration=" + loadedOnPreMigration
                + " isLoaded=" + isLoaded();
    }

    @Override
    public int clear() {
        checkIfLoaded();
        // we don't remove locked keys. These are clearable records.
        Collection<Record> clearableRecords = getNotLockedRecords();
        // This conversion is required by mapDataStore#removeAll call.
        List<Data> keys = getKeysFromRecords(clearableRecords);
        mapDataStore.removeAll(keys);
        clearMapStore();
        removeIndex(clearableRecords);
        return removeRecords(clearableRecords);
    }

    @Override
    public void reset() {
        clearMapStore();
        storage.clear(false);
        stats.reset();
        mutationObserver.onReset();
    }

    @Override
    public void destroy() {
        clearPartition(false, true);
        storage.destroy(false);
        mutationObserver.onDestroy(false);
    }

    @Override
    public void destroyInternals() {
        clearMapStore();
        clearStorage(false);
        storage.destroy(false);
        mutationObserver.onDestroy(true);
    }

    @Override
    public void clearPartition(boolean onShutdown, boolean onRecordStoreDestroy) {
        clearOtherResourcesThanStorage(onRecordStoreDestroy);
        clearStorage(onShutdown || onRecordStoreDestroy);
    }

    public void clearOtherResourcesThanStorage(boolean onRecordStoreDestroy) {
        clearMapStore();
        clearLockStore();
        clearIndexedData(onRecordStoreDestroy);
    }

    private void clearLockStore() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            ObjectNamespace namespace = MapService.getObjectNamespace(name);
            lockService.clearLockStore(partitionId, namespace);
        }
    }

    public void clearStorage(boolean onShutdown) {
        if (onShutdown) {
            NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
            NativeMemoryConfig nativeMemoryConfig = nodeEngine.getConfig().getNativeMemoryConfig();
            boolean shouldClear = (nativeMemoryConfig != null && nativeMemoryConfig.getAllocatorType() != POOLED);
            if (shouldClear) {
                storage.clear(true);
                mutationObserver.onClear();
            }
            storage.destroy(true);
            mutationObserver.onDestroy(true);
        } else {
            storage.clear(false);
            mutationObserver.onClear();
        }
    }

    private void clearMapStore() {
        mapDataStore.reset();
    }

    /**
     * Only indexed data will be removed, index info will stay.
     */
    public void clearIndexedData(boolean onRecordStoreDestroy) {
        clearGlobalIndexes();
        clearPartitionedIndexes(onRecordStoreDestroy);
    }

    private void clearGlobalIndexes() {
        Indexes indexes = mapContainer.getIndexes(partitionId);
        if (indexes.isGlobal()) {
            if (indexes.hasIndex()) {
                // clears indexed data of this partition
                // from shared global index.
                fullScanLocalDataToClear(indexes);
            }
        }
    }

    private void clearPartitionedIndexes(boolean onRecordStoreDestroy) {
        Indexes indexes = mapContainer.getIndexes(partitionId);
        if (indexes.isGlobal()) {
            return;
        }

        if (onRecordStoreDestroy) {
            indexes.destroyIndexes();
        } else {
            indexes.clearAll();
        }
    }

    /**
     * Clears local data of this partition from global index by doing
     * partition full-scan.
     */
    private void fullScanLocalDataToClear(Indexes indexes) {
        for (Record record : storage.values()) {
            Data key = record.getKey();
            Object value = Records.getValueOrCachedValue(record, serializationService);
            indexes.removeEntryIndex(key, value, Index.OperationSource.SYSTEM);
        }
    }
}
