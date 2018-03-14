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
import com.hazelcast.core.EntryView;
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.merge.MergingEntry;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.CollectionUtil;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.FutureUtil;

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
import static com.hazelcast.map.impl.ExpirationTimeSetter.setTTLAndUpdateExpiryTime;
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
    public void destroy() {
        clearPartition(false);
        storage.destroy(false);
        eventJournal.destroy(mapContainer.getObjectNamespace(), partitionId);
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
    protected void flush(Collection<Record> recordsToBeFlushed, boolean backup) {
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
        markRecordStoreExpirable(record.getTtl());
        storage.put(key, record);
        updateStatsOnPut(record.getHits());
    }

    @Override
    public Record putBackup(Data key, Object value) {
        return putBackup(key, value, DEFAULT_TTL, false);
    }

    @Override
    public Record putBackup(Data key, Object value, long ttl, boolean putTransient) {
        long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecordOrNull(key, now, true);
        if (record == null) {
            record = createRecord(value, ttl, now);
            storage.put(key, record);
            eventJournal.writeAddEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                    key, record.getValue());
        } else {
            updateRecord(key, record, value, now, true);
        }
        if (putTransient) {
            mapDataStore.addTransient(key, now);
        } else {
            mapDataStore.addBackup(key, value, now);
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

    @Override
    public void clearPartition(boolean onShutdown) {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            ObjectNamespace namespace = MapService.getObjectNamespace(name);
            lockService.clearLockStore(partitionId, namespace);
        }

        Indexes indexes = mapContainer.getIndexes(partitionId);
        if (indexes.isGlobal()) {
            if (indexes.hasIndex()) {
                for (Record record : storage.values()) {
                    Data key = record.getKey();
                    Object value = Records.getValueOrCachedValue(record, serializationService);
                    indexes.removeEntryIndex(key, value);
                }
            }
        } else {
            indexes.clearIndexes();
        }

        mapDataStore.reset();

        if (onShutdown) {
            NativeMemoryConfig nativeMemoryConfig = nodeEngine.getConfig().getNativeMemoryConfig();
            boolean shouldClear = (nativeMemoryConfig != null && nativeMemoryConfig.getAllocatorType() != POOLED);
            if (shouldClear) {
                storage.clear(true);
            }
            storage.destroy(true);
        } else {
            storage.clear(false);
        }
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
    public Record loadRecordOrNull(Data key, boolean backup) {
        Record record = null;
        Object value = mapDataStore.load(key);
        if (value != null) {
            record = createRecord(value, DEFAULT_TTL, getNow());
            storage.put(key, record);
            eventJournal.writeAddEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                    key, record.getValue());
            if (!backup) {
                saveIndex(record, null);
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

    @Override
    public int clear() {
        checkIfLoaded();

        // we don't remove locked keys. These are clearable records.
        Collection<Record> clearableRecords = getNotLockedRecords();
        // This conversion is required by mapDataStore#removeAll call.
        List<Data> keys = getKeysFromRecords(clearableRecords);
        mapDataStore.removeAll(keys);
        mapDataStore.reset();
        removeIndex(clearableRecords);
        return removeRecords(clearableRecords);
    }

    protected List<Data> getKeysFromRecords(Collection<Record> clearableRecords) {
        List<Data> keys = new ArrayList<Data>(clearableRecords.size());
        for (Record clearableRecord : clearableRecords) {
            keys.add(clearableRecord.getKey());
        }
        return keys;
    }

    protected int removeRecords(Collection<Record> recordsToRemove) {
        if (CollectionUtil.isEmpty(recordsToRemove)) {
            return 0;
        }
        int removalSize = recordsToRemove.size();
        Iterator<Record> iterator = recordsToRemove.iterator();
        while (iterator.hasNext()) {
            Record record = iterator.next();
            eventJournal.writeRemoveEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                    record.getKey(), record.getValue());
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

    /**
     * Resets the record store to it's initial state.
     */
    @Override
    public void reset() {
        mapDataStore.reset();
        storage.clear(false);
        stats.reset();
    }

    @Override
    public Object evict(Data key, boolean backup) {
        Record record = storage.get(key);
        Object value = null;
        if (record != null) {
            value = record.getValue();
            mapDataStore.flush(key, value, backup);
            removeIndex(record);
            eventJournal.writeEvictEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                    key, value);
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
        return removeRecords(evictableRecords);
    }

    @Override
    public void removeBackup(Data key) {
        long now = getNow();

        Record record = getRecordOrNull(key, now, true);
        if (record == null) {
            return;
        }
        eventJournal.writeRemoveEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                record.getKey(), record.getValue());
        storage.removeRecord(record);
        mapDataStore.removeBackup(key, now);
    }

    @Override
    public Object remove(Data key) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, false);
        Object oldValue;
        if (record == null) {
            oldValue = mapDataStore.load(key);
            if (oldValue != null) {
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
            eventJournal.writeRemoveEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                    key, oldValue);
            storage.removeRecord(record);
            removed = true;
        }
        return removed;
    }

    @Override
    public boolean delete(Data key) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, false);
        if (record == null) {
            mapDataStore.remove(key, now);
        } else {
            return removeRecord(key, record, now) != null;
        }
        return false;
    }

    @Override
    public Object get(Data key, boolean backup) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, backup);
        if (record == null) {
            record = loadRecordOrNull(key, backup);
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
    public MapEntries getAll(Set<Data> keys) {
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

        Map loadedEntries = loadEntries(keys);
        addMapEntrySet(loadedEntries, mapEntries);

        return mapEntries;
    }

    protected Map<Data, Object> loadEntries(Set<Data> keys) {
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

            putFromLoad(key, value);

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
    public boolean containsKey(Data key) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, false);
        if (record == null) {
            record = loadRecordOrNull(key, false);
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
                record.getKey(), mapServiceContext.toData(record.getValue()), null, null, ADDED.getType());

        mapEventPublisher.addEventToQueryCache(eventData);
    }

    @Override
    public Object set(Data dataKey, Object value, long ttl) {
        return putInternal(dataKey, value, ttl, false, true);
    }

    @Override
    public Object put(Data key, Object value, long ttl) {
        return putInternal(key, value, ttl, true, true);
    }

    protected Object putInternal(Data key, Object value, long ttl, boolean loadFromStore, boolean countAsAccess) {
        checkIfLoaded();

        long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecordOrNull(key, now, false);
        Object oldValue = record == null ? (loadFromStore ? mapDataStore.load(key) : null) : record.getValue();
        value = mapServiceContext.interceptPut(name, oldValue, value);
        value = mapDataStore.add(key, value, now);
        onStore(record);

        if (record == null) {
            record = createRecord(value, ttl, now);
            storage.put(key, record);
            eventJournal.writeAddEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                    record.getKey(), record.getValue());
        } else {
            updateRecord(key, record, value, now, countAsAccess);
            setTTLAndUpdateExpiryTime(ttl, record, mapContainer.getMapConfig(), false);
        }

        saveIndex(record, oldValue);
        return oldValue;
    }

    @Override
    public boolean merge(MergingEntry<Data, Object> mergingEntry, SplitBrainMergePolicy mergePolicy) {
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
            newValue = mapDataStore.add(key, newValue, now);
            record = createRecord(newValue, DEFAULT_TTL, now);
            mergeRecordExpiration(record, mergingEntry);
            storage.put(key, record);
            eventJournal.writeUpdateEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                    key, null, record.getValue());
        } else {
            oldValue = record.getValue();
            MergingEntry<Data, Object> existingEntry = createMergingEntry(serializationService, record);
            newValue = mergePolicy.merge(mergingEntry, existingEntry);
            // existing entry will be removed
            if (newValue == null) {
                removeIndex(record);
                mapDataStore.remove(key, now);
                onStore(record);
                eventJournal.writeUpdateEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(),
                        partitionId, key, oldValue, null);
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
            newValue = mapDataStore.add(key, newValue, now);
            onStore(record);
            eventJournal.writeUpdateEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                    key, oldValue, newValue);
            storage.updateRecordValue(key, record, newValue);
        }
        saveIndex(record, oldValue);
        return newValue != null;
    }

    @Override
    public boolean merge(Data key, EntryView mergingEntry, MapMergePolicy mergePolicy) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, false);
        mergingEntry = EntryViews.convertToLazyEntryView(mergingEntry, serializationService, mergePolicy);
        Object newValue;
        Object oldValue = null;
        if (record == null) {
            Object notExistingKey = mapServiceContext.toObject(key);
            EntryView nullEntryView = EntryViews.createNullEntryView(notExistingKey);
            newValue = mergePolicy.merge(name, mergingEntry, nullEntryView);
            if (newValue == null) {
                return false;
            }
            newValue = mapDataStore.add(key, newValue, now);
            record = createRecord(newValue, DEFAULT_TTL, now);
            mergeRecordExpiration(record, mergingEntry);
            storage.put(key, record);
            eventJournal.writeUpdateEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                    key, null, record.getValue());
        } else {
            oldValue = record.getValue();
            EntryView existingEntry = EntryViews.createLazyEntryView(record.getKey(), record.getValue(),
                    record, serializationService, mergePolicy);
            newValue = mergePolicy.merge(name, mergingEntry, existingEntry);
            // existing entry will be removed
            if (newValue == null) {
                removeIndex(record);
                mapDataStore.remove(key, now);
                onStore(record);
                eventJournal.writeUpdateEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(),
                        partitionId, key, oldValue, null);
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
            newValue = mapDataStore.add(key, newValue, now);
            onStore(record);
            eventJournal.writeUpdateEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                    key, oldValue, newValue);
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
        setTTLAndUpdateExpiryTime(record.getTtl(), record, mapContainer.getMapConfig(), false);
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
        setTTLAndUpdateExpiryTime(record.getTtl(), record, mapContainer.getMapConfig(), false);
        saveIndex(record, current);
        return true;
    }

    @Override
    public Object putTransient(Data key, Object value, long ttl) {
        checkIfLoaded();
        long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecordOrNull(key, now, false);
        Object oldValue = null;
        if (record == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            record = createRecord(value, ttl, now);
            storage.put(key, record);
            eventJournal.writeAddEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                    record.getKey(), record.getValue());
        } else {
            oldValue = record.getValue();
            value = mapServiceContext.interceptPut(name, oldValue, value);
            updateRecord(key, record, value, now, true);
            setTTLAndUpdateExpiryTime(ttl, record, mapContainer.getMapConfig(), false);
        }
        saveIndex(record, oldValue);
        mapDataStore.addTransient(key, now);
        return oldValue;
    }

    @Override
    public Object putFromLoad(Data key, Object value) {
        return putFromLoadInternal(key, value, DEFAULT_TTL, false);
    }

    @Override
    public Object putFromLoadBackup(Data key, Object value) {
        return putFromLoadInternal(key, value, DEFAULT_TTL, true);
    }

    private Object putFromLoadInternal(Data key, Object value, long ttl, boolean backup) {
        if (!isKeyAndValueLoadable(key, value)) {
            return null;
        }

        long now = getNow();

        if (shouldEvict()) {
            return null;
        }

        markRecordStoreExpirable(ttl);

        Record record = getRecordOrNull(key, now, false);
        Object oldValue = null;
        if (record == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            record = createRecord(value, ttl, now);
            storage.put(key, record);
            eventJournal.writeAddEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                    record.getKey(), record.getValue());
        } else {
            oldValue = record.getValue();
            value = mapServiceContext.interceptPut(name, oldValue, value);
            updateRecord(key, record, value, now, true);
            setTTLAndUpdateExpiryTime(ttl, record, mapContainer.getMapConfig(), false);
        }
        if (!backup) {
            saveIndex(record, oldValue);
        }
        return oldValue;
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
    public boolean setWithUncountedAccess(Data dataKey, Object value, long ttl) {
        Object oldValue = putInternal(dataKey, value, ttl, false, false);
        return oldValue == null;
    }

    @Override
    public Object putIfAbsent(Data key, Object value, long ttl) {
        checkIfLoaded();
        long now = getNow();
        markRecordStoreExpirable(ttl);

        Record record = getRecordOrNull(key, now, false);
        Object oldValue;
        if (record == null) {
            oldValue = mapDataStore.load(key);
            if (oldValue != null) {
                record = createRecord(oldValue, DEFAULT_TTL, now);
                storage.put(key, record);
                eventJournal.writeAddEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                        record.getKey(), record.getValue());
            }
        } else {
            accessRecord(record, now);
            oldValue = record.getValue();
        }
        if (oldValue == null) {
            value = mapServiceContext.interceptPut(name, null, value);
            value = mapDataStore.add(key, value, now);
            onStore(record);
            record = createRecord(value, ttl, now);
            storage.put(key, record);
            eventJournal.writeAddEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                    record.getKey(), record.getValue());
            setTTLAndUpdateExpiryTime(ttl, record, mapContainer.getMapConfig(), false);
        }
        saveIndex(record, oldValue);
        return oldValue;
    }

    protected Object removeRecord(Data key, Record record, long now) {
        Object oldValue = record.getValue();
        oldValue = mapServiceContext.interceptRemove(name, oldValue);
        if (oldValue != null) {
            removeIndex(record);
            mapDataStore.remove(key, now);
            onStore(record);
        }
        eventJournal.writeRemoveEvent(mapContainer.getEventJournalConfig(), mapContainer.getObjectNamespace(), partitionId,
                record.getKey(), record.getValue());
        storage.removeRecord(record);
        return oldValue;
    }

    @Override
    public Record getRecordOrNull(Data key) {
        long now = getNow();
        return getRecordOrNull(key, now, false);
    }

    protected Record getRecordOrNull(Data key, long now, boolean backup) {
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
                ExceptionUtil.rethrow(e);
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
    public void loadAllFromStore(List<Data> keys, boolean replaceExistingValues) {
        if (!keys.isEmpty()) {
            Future f = recordStoreLoader.loadValues(keys, replaceExistingValues);
            loadingFutures.add(f);
        }

        // We should not track key loading here. IT's not key loading but values loading.
        // Apart from that it's irrelevant for RECEIVER nodes. SENDER and SENDER_BACKUP will track the key-loading anyway.
        // Fixes https://github.com/hazelcast/hazelcast/issues/9255
    }

    @Override
    public void updateLoadStatus(boolean lastBatch, Throwable exception) {
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
}
