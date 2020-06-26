/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.locksupport.LockSupportService;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.EntryLoader.MetadataAwareValue;
import com.hazelcast.map.impl.InterceptorRegistry;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapKeyLoader;
import com.hazelcast.map.impl.MapService;
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
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.wan.impl.CallerProvenance;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.LOADED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.MapUtil.isNullOrEmpty;
import static com.hazelcast.map.impl.ExpirationTimeSetter.setExpirationTimes;
import static com.hazelcast.map.impl.mapstore.MapDataStores.EMPTY_MAP_DATA_STORE;
import static com.hazelcast.map.impl.record.Record.UNSET;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;

/**
 * Default implementation of record-store.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity"})
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
    protected final Collection<Future> loadingFutures = new ConcurrentLinkedQueue<>();
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
    private final InterceptorRegistry interceptorRegistry;

    public DefaultRecordStore(MapContainer mapContainer, int partitionId,
                              MapKeyLoader keyLoader, ILogger logger) {
        super(mapContainer, partitionId);

        this.logger = logger;
        this.keyLoader = keyLoader;
        this.recordStoreLoader = createRecordStoreLoader(mapStoreContext);
        this.partitionService = mapServiceContext.getNodeEngine().getPartitionService();
        this.interceptorRegistry = mapContainer.getInterceptorRegistry();
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
     */
    private void flush(ArrayList<Data> dataKeys,
                       ArrayList<Record> records, boolean backup) {
        if (mapDataStore == EMPTY_MAP_DATA_STORE) {
            return;
        }

        for (int i = 0; i < dataKeys.size(); i++) {
            mapDataStore.flush(dataKeys.get(i), records.get(i).getValue(), backup);
        }
    }

    @Override
    public Record getRecord(Data key) {
        return storage.get(key);
    }

    @Override
    public Record putReplicatedRecord(Data dataKey, Record replicatedRecord, long nowInMillis,
                                      boolean populateIndexes) {
        Record newRecord = createRecord(dataKey, replicatedRecord, nowInMillis);
        markRecordStoreExpirable(replicatedRecord.getTtl(), replicatedRecord.getMaxIdle());
        storage.put(dataKey, newRecord);
        mutationObserver.onReplicationPutRecord(dataKey, newRecord, populateIndexes);
        updateStatsOnPut(replicatedRecord.getHits(), nowInMillis);
        return newRecord;
    }

    @Override
    public Record putBackup(Data dataKey, Record newRecord,
                            boolean putTransient, CallerProvenance provenance) {
        return putBackupInternal(dataKey, newRecord.getValue(),
                newRecord.getTtl(), newRecord.getMaxIdle(), putTransient, provenance, null);
    }

    @Override
    public Record putBackupTxn(Data dataKey, Record newRecord, boolean putTransient,
                               CallerProvenance provenance, UUID transactionId) {
        return putBackupInternal(dataKey, newRecord.getValue(),
                newRecord.getTtl(), newRecord.getMaxIdle(), putTransient, provenance, transactionId);
    }

    @Override
    public Record putBackup(Data key, Object value, long ttl, long maxIdle, CallerProvenance provenance) {
        return putBackupInternal(key, value, ttl, maxIdle,
                false, provenance, null);
    }

    private Record putBackupInternal(Data key, Object value, long ttl, long maxIdle,
                                     boolean putTransient, CallerProvenance provenance,
                                     UUID transactionId) {
        long now = getNow();
        markRecordStoreExpirable(ttl, maxIdle);

        Record record = getRecordOrNull(key, now, true);
        if (record == null) {
            record = createRecord(key, value, ttl, maxIdle, now);
            storage.put(key, record);
            mutationObserver.onPutRecord(key, record, null, true);
        } else {
            updateRecord(key, record, record.getValue(), value, now, true,
                    ttl, maxIdle, false, transactionId, true);
        }

        if (persistenceEnabledFor(provenance)) {
            if (putTransient) {
                mapDataStore.addTransient(key, now);
            } else {
                mapDataStore.addBackup(key, value, record.getExpirationTime(), now, transactionId);
            }
        }
        return record;
    }

    @Override
    public void forEach(BiConsumer<Data, Record> consumer, boolean backup) {
        forEach(consumer, backup, false);
    }

    @Override
    public void forEach(BiConsumer<Data, Record> consumer,
                        boolean backup, boolean includeExpiredRecords) {

        long now = Clock.currentTimeMillis();
        Iterator<Map.Entry<Data, Record>> entries = storage.mutationTolerantIterator();
        while (entries.hasNext()) {
            Map.Entry<Data, Record> entry = entries.next();

            Data key = entry.getKey();
            Record record = entry.getValue();

            if (includeExpiredRecords || !isExpired(record, now, backup)) {
                consumer.accept(key, record);
            }
        }
    }

    @Override
    public Iterator<Map.Entry<Data, Record>> iterator() {
        return storage.mutationTolerantIterator();
    }

    @Override
    public void forEachAfterLoad(BiConsumer<Data, Record> consumer, boolean backup) {
        checkIfLoaded();
        forEach(consumer, backup);
    }

    @Override
    public MapKeysWithCursor fetchKeys(IterationPointer[] pointers, int size) {
        return storage.fetchKeys(pointers, size);
    }

    @Override
    public MapEntriesWithCursor fetchEntries(IterationPointer[] pointers, int size) {
        return storage.fetchEntries(pointers, size);
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

        if (storage.isEmpty()) {
            return false;
        }

        // optimisation to skip serialisation/de-serialisation
        // in each call to RecordComparator.isEqual()
        value = inMemoryFormat == InMemoryFormat.OBJECT
                ? serializationService.toObject(value)
                : serializationService.toData(value);

        Iterator<Map.Entry<Data, Record>> entryIterator = storage.mutationTolerantIterator();
        while (entryIterator.hasNext()) {
            Map.Entry<Data, Record> entry = entryIterator.next();

            Data key = entry.getKey();
            Record record = entry.getValue();

            if (getOrNullIfExpired(key, record, now, false) == null) {
                continue;
            }
            if (valueComparator.isEqual(value, record.getValue(), serializationService)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean txnLock(Data key, UUID caller, long threadId, long referenceId, long ttl, boolean blockReads) {
        checkIfLoaded();
        return lockStore != null && lockStore.txnLock(key, caller, threadId, referenceId, ttl, blockReads);
    }

    @Override
    public boolean extendLock(Data key, UUID caller, long threadId, long ttl) {
        checkIfLoaded();
        return lockStore != null && lockStore.extendLeaseTime(key, caller, threadId, ttl);
    }

    @Override
    public boolean localLock(Data key, UUID caller, long threadId, long referenceId, long ttl) {
        checkIfLoaded();
        return lockStore != null && lockStore.localLock(key, caller, threadId, referenceId, ttl);
    }

    @Override
    public boolean unlock(Data key, UUID caller, long threadId, long referenceId) {
        checkIfLoaded();
        return lockStore != null && lockStore.unlock(key, caller, threadId, referenceId);
    }

    @Override
    public boolean lock(Data key, UUID caller, long threadId, long referenceId, long ttl) {
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
    public boolean canAcquireLock(Data key, UUID caller, long threadId) {
        return lockStore == null || lockStore.canAcquireLock(key, caller, threadId);
    }

    @Override
    public boolean isLockedBy(Data key, UUID caller, long threadId) {
        return lockStore != null && lockStore.isLockedBy(key, caller, threadId);
    }

    @Override
    public String getLockOwnerInfo(Data key) {
        return lockStore != null ? lockStore.getOwnerInfo(key) : null;
    }

    @Override
    public Record loadRecordOrNull(Data key, boolean backup, Address callerAddress) {
        Object value = mapDataStore.load(key);
        if (value == null) {
            return null;
        }

        long ttl = UNSET;
        if (mapDataStore.isWithExpirationTime()) {
            MetadataAwareValue loaderEntry = (MetadataAwareValue) value;
            long proposedTtl = expirationTimeToTtl(loaderEntry.getExpirationTime());
            if (proposedTtl <= 0) {
                return null;
            }
            value = loaderEntry.getValue();
            ttl = proposedTtl;
        }
        Record record = createRecord(key, value, ttl, UNSET, getNow());
        markRecordStoreExpirable(ttl, UNSET);
        storage.put(key, record);
        mutationObserver.onLoadRecord(key, record, backup);
        if (!backup) {
            mapEventPublisher.publishEvent(callerAddress, name, EntryEventType.LOADED,
                    key, null, value, null);
        }
        evictEntries(key);
        // here, we are only publishing events for loaded
        // entries. This is required for notifying query-caches
        // otherwise query-caches cannot see loaded entries
        if (!backup && hasQueryCache()) {
            addEventToQueryCache(key, record);
        }
        return record;
    }

    protected long expirationTimeToTtl(long definedExpirationTime) {
        return definedExpirationTime - System.currentTimeMillis();
    }

    protected int removeBulk(ArrayList<Data> dataKeys, ArrayList<Record> records) {
        return removeOrEvictEntries(dataKeys, records, false);
    }

    protected int evictBulk(ArrayList<Data> dataKeys, ArrayList<Record> records) {
        return removeOrEvictEntries(dataKeys, records, true);
    }

    private int removeOrEvictEntries(ArrayList<Data> dataKeys, ArrayList<Record> records, boolean eviction) {
        for (int i = 0; i < dataKeys.size(); i++) {
            Data dataKey = dataKeys.get(i);
            Record record = records.get(i);
            removeOrEvictEntry(dataKey, record, eviction);
        }

        return dataKeys.size();
    }

    private void removeOrEvictEntry(Data dataKey, Record record, boolean eviction) {
        if (eviction) {
            mutationObserver.onEvictRecord(dataKey, record);
        } else {
            mutationObserver.onRemoveRecord(dataKey, record);
        }
        storage.removeRecord(dataKey, record);
    }

    @Override
    public Object evict(Data key, boolean backup) {
        Record record = storage.get(key);
        Object value = null;
        if (record != null) {
            value = record.getValue();
            mapDataStore.flush(key, value, backup);
            mutationObserver.onEvictRecord(key, record);
            storage.removeRecord(key, record);
            if (!backup) {
                mapServiceContext.interceptRemove(interceptorRegistry, value);
            }
        }
        return value;
    }

    @Override
    public void removeBackup(Data key, CallerProvenance provenance) {
        removeBackupInternal(key, provenance, null);
    }

    @Override
    public void removeBackupTxn(Data key, CallerProvenance provenance, UUID transactionId) {
        removeBackupInternal(key, provenance, transactionId);
    }

    private void removeBackupInternal(Data key, CallerProvenance provenance, UUID transactionId) {
        long now = getNow();

        Record record = getRecordOrNull(key, now, true);
        if (record == null) {
            return;
        }
        mutationObserver.onRemoveRecord(key, record);
        storage.removeRecord(key, record);
        if (persistenceEnabledFor(provenance)) {
            mapDataStore.removeBackup(key, now, transactionId);
        }
    }

    @Override
    public boolean delete(Data key, CallerProvenance provenance) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, false);
        if (record == null) {
            if (persistenceEnabledFor(provenance)) {
                mapDataStore.remove(key, now, null);
            }
        } else {
            return removeRecord(key, record, now, provenance, null) != null;
        }
        return false;
    }

    @Override
    public Object removeTxn(Data dataKey, CallerProvenance callerProvenance, UUID transactionId) {
        return removeInternal(dataKey, callerProvenance, transactionId);
    }

    @Override
    public Object remove(Data key, CallerProvenance callerProvenance) {
        return removeInternal(key, callerProvenance, null);
    }

    private Object removeInternal(Data key, CallerProvenance provenance,
                                  UUID transactionId) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, false);
        Object oldValue;
        if (record == null) {
            oldValue = mapDataStore.load(key);
            if (oldValue != null && persistenceEnabledFor(provenance)) {
                mapDataStore.remove(key, now, transactionId);
            }
        } else {
            oldValue = removeRecord(key, record, now, provenance, transactionId);
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

        if (valueComparator.isEqual(testValue, oldValue, serializationService)) {
            mapServiceContext.interceptRemove(interceptorRegistry, oldValue);
            mapDataStore.remove(key, now, null);
            if (record != null) {
                onStore(record);
                mutationObserver.onRemoveRecord(key, record);
                storage.removeRecord(key, record);
            }
            removed = true;
        }
        return removed;
    }

    @Override
    public Object get(Data key, boolean backup, Address callerAddress, boolean touch) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, backup);
        if (record == null) {
            record = loadRecordOrNull(key, backup, callerAddress);
            record = getOrNullIfExpired(key, record, now, backup);
        } else if (touch) {
            accessRecord(record, now);
        }
        Object value = record == null ? null : record.getValue();
        value = mapServiceContext.interceptGet(interceptorRegistry, value);

        return value;
    }

    /**
     * This method is called directly by user threads, in other words
     * it is called outside of the partition threads.
     */
    @Override
    public Data readBackupData(Data key) {
        Record record = getRecord(key);

        if (record == null) {
            return null;
        } else {
            if (partitionService.isPartitionOwner(partitionId)) {
                // set last access time to prevent
                // premature removal of the entry because
                // of idleness based expiry
                record.setLastAccessTime(Clock.currentTimeMillis());
            }
        }

        Object value = record.getValue();
        mapServiceContext.interceptAfterGet(interceptorRegistry, value);
        // this serialization step is needed not to expose the object, see issue 1292
        return mapServiceContext.toData(value);
    }

    @Override
    public MapEntries getAll(Set<Data> keys, Address callerAddress) {
        checkIfLoaded();
        long now = getNow();

        MapEntries mapEntries = new MapEntries(keys.size());

        // first search in memory
        Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            Data key = iterator.next();
            Record record = getRecordOrNull(key, now, false);
            if (record != null) {
                addToMapEntrySet(key, record.getValue(), mapEntries);
                accessRecord(record, now);
                iterator.remove();
            }
        }

        // then try to load missing keys from map-store
        if (mapDataStore != EMPTY_MAP_DATA_STORE && !keys.isEmpty()) {
            Map loadedEntries = loadEntries(keys, callerAddress);
            addToMapEntrySet(loadedEntries, mapEntries);
        }

        return mapEntries;
    }

    private Map<Data, Object> loadEntries(Set<Data> keys, Address callerAddress) {
        Map loadedEntries = mapDataStore.loadAll(keys);

        if (isNullOrEmpty(loadedEntries)) {
            return Collections.emptyMap();
        }

        // holds serialized keys and if values are
        // serialized, also holds them in serialized format.
        Map<Data, Object> resultMap = createHashMap(loadedEntries.size());

        // add loaded key-value pairs to this record-store.
        Set entrySet = loadedEntries.entrySet();
        for (Object object : entrySet) {
            Map.Entry entry = (Map.Entry) object;

            Data key = toData(entry.getKey());
            Object value = entry.getValue();
            if (mapDataStore.isWithExpirationTime()) {
                MetadataAwareValue loaderEntry = (MetadataAwareValue) value;

                if (expirationTimeToTtl(loaderEntry.getExpirationTime()) > 0) {
                    resultMap.put(key, loaderEntry.getValue());
                }
                putFromLoad(key, loaderEntry.getValue(), loaderEntry.getExpirationTime(), callerAddress);

            } else {
                resultMap.put(key, value);
                putFromLoad(key, value, callerAddress);
            }
        }

        if (hasQueryCache()) {
            for (Data key : resultMap.keySet()) {
                Record record = storage.get(key);
                // here we are only publishing events for loaded entries. This is required for notifying query-caches
                // otherwise query-caches cannot see loaded entries
                addEventToQueryCache(key, record);
            }
        }
        return resultMap;
    }

    protected void addToMapEntrySet(Object key, Object value, MapEntries mapEntries) {
        if (key == null || value == null) {
            return;
        }
        value = mapServiceContext.interceptGet(interceptorRegistry, value);
        Data dataKey = mapServiceContext.toData(key);
        Data dataValue = mapServiceContext.toData(value);
        mapEntries.add(dataKey, dataValue);
    }

    protected void addToMapEntrySet(Map<Object, Object> entries, MapEntries mapEntries) {
        for (Map.Entry<Object, Object> entry : entries.entrySet()) {
            addToMapEntrySet(entry.getKey(), entry.getValue(), mapEntries);
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

    private void addEventToQueryCache(Data dataKey, Record record) {
        EntryEventData eventData = new EntryEventData(thisAddress.toString(), name, thisAddress,
                dataKey, mapServiceContext.toData(record.getValue()),
                null, null, ADDED.getType());

        mapEventPublisher.addEventToQueryCache(eventData);
    }

    @Override
    public boolean setTtl(Data key, long ttl, boolean backup) {
        long now = getNow();
        Record record = getRecordOrNull(key, now, false);
        Object existingValue = record == null ? mapDataStore.load(key) : record.getValue();
        if (existingValue == null) {
            return false;
        }
        if (record == null) {
            createRecord(key, existingValue, ttl, UNSET, now);
            mutationObserver.onPutRecord(key, null, existingValue, false);
        } else {
            updateRecord(key, record, existingValue, existingValue, now, true, ttl,
                    UNSET, true, null, backup);
        }
        markRecordStoreExpirable(ttl, UNSET);
        return true;
    }

    @Override
    public Object set(Data dataKey, Object value, long ttl, long maxIdle) {
        return putInternal(dataKey, value, ttl, maxIdle, null, false, true);
    }

    @Override
    public Object setTxn(Data dataKey, Object value, long ttl, long maxIdle, UUID transactionId) {
        return putInternal(dataKey, value, ttl, maxIdle, transactionId, false, true);
    }

    @Override
    public Object put(Data key, Object value, long ttl, long maxIdle) {
        return putInternal(key, value, ttl, maxIdle, null, true, true);
    }

    protected Object putInternal(Data key, Object newValue, long ttl,
                                 long maxIdle, @Nullable UUID transactionId,
                                 boolean loadFromStore, boolean countAsAccess) {
        checkIfLoaded();

        long now = getNow();
        markRecordStoreExpirable(ttl, maxIdle);

        Record record = getRecordOrNull(key, now, false);
        Object oldValue = record == null
                ? (loadFromStore ? mapDataStore.load(key) : null) : record.getValue();
        newValue = mapServiceContext.interceptPut(interceptorRegistry, oldValue, newValue);
        onStore(record);

        if (record == null) {
            putNewRecord(key, oldValue, newValue, ttl, maxIdle, now, transactionId);
        } else {
            updateRecord(key, record, oldValue, newValue, now, countAsAccess, ttl,
                    maxIdle, true, transactionId, false);
        }

        return oldValue;
    }

    @Override
    public boolean merge(MapMergeTypes<Object, Object> mergingEntry,
                         SplitBrainMergePolicy<Object, MapMergeTypes<Object, Object>, Object> mergePolicy) {
        return merge(mergingEntry, mergePolicy, CallerProvenance.NOT_WAN);
    }

    @Override
    public boolean merge(MapMergeTypes<Object, Object> mergingEntry,
                         SplitBrainMergePolicy<Object, MapMergeTypes<Object, Object>, Object> mergePolicy,
                         CallerProvenance provenance) {
        checkIfLoaded();
        long now = getNow();

        mergingEntry = (MapMergeTypes<Object, Object>) serializationService.getManagedContext().initialize(mergingEntry);
        mergePolicy = (SplitBrainMergePolicy<Object, MapMergeTypes<Object, Object>, Object>)
            serializationService.getManagedContext().initialize(mergePolicy);

        Data key = (Data) mergingEntry.getRawKey();
        Record record = getRecordOrNull(key, now, false);
        Object newValue;
        Object oldValue;
        if (record == null) {
            newValue = mergePolicy.merge(mergingEntry, null);
            if (newValue == null) {
                return false;
            }

            record = createRecord(key, newValue, UNSET, UNSET, now);
            mergeRecordExpiration(record, mergingEntry);
            if (persistenceEnabledFor(provenance)) {
                putIntoMapStore(record, key, newValue, now, null);
            }
            storage.put(key, record);
            mutationObserver.onPutRecord(key, record, null, false);
        } else {
            oldValue = record.getValue();
            MapMergeTypes<Object, Object> existingEntry = createMergingEntry(serializationService, key, record);
            newValue = mergePolicy.merge(mergingEntry, existingEntry);
            // existing entry will be removed
            if (newValue == null) {
                if (persistenceEnabledFor(provenance)) {
                    mapDataStore.remove(key, now, null);
                }
                onStore(record);
                mutationObserver.onRemoveRecord(key, record);
                storage.removeRecord(key, record);
                return true;
            }

            if (valueComparator.isEqual(newValue, oldValue, serializationService)) {
                mergeRecordExpiration(record, mergingEntry);
                return true;
            }

            newValue = persistenceEnabledFor(provenance)
                    ? mapDataStore.add(key, newValue, record.getExpirationTime(), now, null) : newValue;
            onStore(record);
            mutationObserver.onUpdateRecord(key, record, oldValue, newValue, false);
            storage.updateRecordValue(key, record, newValue);
        }

        return newValue != null;
    }

    @Override
    public Object replace(Data key, Object update) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, false);
        Object oldValue;
        if (record == null) {
            oldValue = mapDataStore.load(key);
        } else {
            oldValue = record.getValue();
        }
        if (oldValue == null) {
            return null;
        }
        update = mapServiceContext.interceptPut(interceptorRegistry, oldValue, update);
        if (record == null) {
            record = putNewRecord(key, oldValue, update, UNSET, UNSET, now, null);
        } else {
            updateRecord(key, record, oldValue, update, now, true,
                    UNSET, UNSET, true, null, false);
        }
        onStore(record);
        return oldValue;
    }

    @Override
    public boolean replace(Data key, Object expect, Object update) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, false);
        Object current;
        if (record == null) {
            current = mapDataStore.load(key);
        } else {
            current = record.getValue();
        }
        if (current == null) {
            return false;
        }
        if (!valueComparator.isEqual(expect, current, serializationService)) {
            return false;
        }
        update = mapServiceContext.interceptPut(interceptorRegistry, current, update);
        if (record == null) {
            record = putNewRecord(key, current, update, UNSET, UNSET, now, null);
        } else {
            updateRecord(key, record, current, update, now, true,
                    UNSET, UNSET, true, null, false);
        }
        onStore(record);
        setExpirationTimes(record.getTtl(), record.getMaxIdle(), record,
                mapContainer.getMapConfig(), false);
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
            value = mapServiceContext.interceptPut(interceptorRegistry, null, value);
            record = createRecord(key, value, ttl, maxIdle, now);
            storage.put(key, record);
            mutationObserver.onPutRecord(key, record, null, false);
        } else {
            oldValue = record.getValue();
            value = mapServiceContext.interceptPut(interceptorRegistry, oldValue, value);
            updateRecord(key, record, oldValue, value, now, true, UNSET,
                    UNSET, false, null, false);
            setExpirationTimes(ttl, maxIdle, record, mapContainer.getMapConfig(), false);
        }
        mapDataStore.addTransient(key, now);
        return oldValue;
    }

    @Override
    public Object putFromLoad(Data key, Object value, Address callerAddress) {
        return putFromLoadInternal(key, value, UNSET, UNSET, false, callerAddress);
    }

    @Override
    public Object putFromLoad(Data key, Object value, long expirationTime, Address callerAddress) {
        if (expirationTime == MetadataAwareValue.NO_TIME_SET) {
            return putFromLoad(key, value, callerAddress);
        }
        long ttl = expirationTimeToTtl(expirationTime);
        if (ttl <= 0) {
            return null;
        }
        return putFromLoadInternal(key, value, ttl, UNSET, false, callerAddress);
    }

    @Override
    public Object putFromLoadBackup(Data key, Object value) {
        return putFromLoadInternal(key, value, UNSET, UNSET, true, null);
    }

    @Override
    public Object putFromLoadBackup(Data key, Object value, long expirationTime) {
        if (expirationTime == MetadataAwareValue.NO_TIME_SET) {
            return putFromLoadBackup(key, value);
        }
        long ttl = expirationTimeToTtl(expirationTime);
        if (ttl <= 0) {
            return null;
        }
        return putFromLoadInternal(key, value, ttl, UNSET, true, null);
    }

    private Object putFromLoadInternal(Data key, Object value, long ttl,
                                       long maxIdle, boolean backup, Address callerAddress) {
        checkKeyAndValue(key, value);

        long now = getNow();

        if (shouldEvict()) {
            return null;
        }

        markRecordStoreExpirable(ttl, maxIdle);

        Record record = getRecordOrNull(key, now, backup);
        Object oldValue = null;
        EntryEventType entryEventType;
        if (record == null) {
            value = mapServiceContext.interceptPut(interceptorRegistry, null, value);
            record = createRecord(key, value, ttl, maxIdle, now);
            storage.put(key, record);
            mutationObserver.onLoadRecord(key, record, backup);
            entryEventType = LOADED;
        } else {
            oldValue = record.getValue();
            value = mapServiceContext.interceptPut(interceptorRegistry, oldValue, value);
            updateRecord(key, record, oldValue, value, now, true,
                    ttl, maxIdle, false, null, backup);
            entryEventType = UPDATED;
        }

        if (!backup) {
            mapEventPublisher.publishEvent(callerAddress, name, entryEventType, key, oldValue, value);
        }

        return oldValue;
    }

    private void checkKeyAndValue(Data key, Object value) {
        if (key == null || value == null) {
            String msg = String.format("Neither key nor value can be loaded as null.[mapName: %s, key: %s, value: %s]",
                    name, serializationService.toObject(key), serializationService.toObject(value));
            throw new NullPointerException(msg);
        }

        if (partitionService.getPartitionId(key) != partitionId) {
            throw new IllegalStateException("MapLoader loaded an item belongs to a different partition");
        }
    }

    @Override
    public boolean setWithUncountedAccess(Data dataKey, Object value, long ttl, long maxIdle) {
        Object oldValue = putInternal(dataKey, value, ttl, maxIdle,
                null, false, false);
        return oldValue == null;
    }

    @Override
    public Object putIfAbsent(Data key, Object value, long ttl,
                              long maxIdle, Address callerAddress) {
        checkIfLoaded();
        long now = getNow();
        markRecordStoreExpirable(ttl, maxIdle);

        Record record = getRecordOrNull(key, now, false);
        Object oldValue;
        if (record == null) {
            oldValue = mapDataStore.load(key);
            if (oldValue != null) {
                record = createRecord(key, oldValue, UNSET, UNSET, now);
                storage.put(key, record);
                mutationObserver.onPutRecord(key, record, null, false);
                mapEventPublisher.publishEvent(callerAddress, name, EntryEventType.LOADED, key, null, oldValue);
            }
        } else {
            accessRecord(record, now);
            oldValue = record.getValue();
        }
        if (oldValue == null) {
            value = mapServiceContext.interceptPut(interceptorRegistry, null, value);
            onStore(record);
            putNewRecord(key, null, value, ttl, maxIdle, now, null);
        }
        return oldValue;
    }

    protected Object removeRecord(Data key, @Nonnull Record record,
                                  long now, CallerProvenance provenance,
                                  UUID transactionId) {
        Object oldValue = record.getValue();
        oldValue = mapServiceContext.interceptRemove(interceptorRegistry, oldValue);
        if (oldValue != null) {
            if (persistenceEnabledFor(provenance)) {
                mapDataStore.remove(key, now, transactionId);
            }
            onStore(record);
        }
        mutationObserver.onRemoveRecord(key, record);
        storage.removeRecord(key, record);
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
        return getOrNullIfExpired(key, record, now, backup);
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
        WriteBehindQueue<DelayedEntry> writeBehindQueue
                = ((WriteBehindStore) mapDataStore).getWriteBehindQueue();
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

        if (FutureUtil.allDone(loadingFutures)) {
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
    public boolean isLoaded() {
        boolean result = FutureUtil.allDone(loadingFutures);
        if (result) {
            loadingFutures.removeAll(FutureUtil.getAllDone(loadingFutures));
        }

        return result;
    }

    // only used for testing purposes
    public Collection<Future> getLoadingFutures() {
        return loadingFutures;
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
    public int evictAll(boolean backup) {
        checkIfLoaded();

        ArrayList<Data> keys = new ArrayList<>();
        ArrayList<Record> records = new ArrayList<>();
        // we don't remove locked keys. These are clearable records.
        forEach(new BiConsumer<Data, Record>() {
            Set<Data> lockedKeySet = lockStore.getLockedKeys();

            @Override
            public void accept(Data dataKey, Record record) {
                if (lockedKeySet != null && !lockedKeySet.contains(dataKey)) {
                    keys.add(dataKey);
                    records.add(record);
                }

            }
        }, true);

        flush(keys, records, backup);
        return evictBulk(keys, records);
    }

    // TODO optimize when no mapdatastore
    @Override
    public int clear() {
        checkIfLoaded();

        ArrayList<Data> keys = new ArrayList<>();
        ArrayList<Record> records = new ArrayList<>();
        // we don't remove locked keys. These are clearable records.
        forEach(new BiConsumer<Data, Record>() {
            Set<Data> lockedKeySet = lockStore.getLockedKeys();

            @Override
            public void accept(Data dataKey, Record record) {
                if (lockedKeySet != null && !lockedKeySet.contains(dataKey)) {
                    keys.add(dataKey);
                    records.add(record);
                }

            }
        }, isBackup(this));
        // This conversion is required by mapDataStore#removeAll call.
        mapDataStore.removeAll(keys);
        mapDataStore.reset();
        return removeBulk(keys, records);
    }

    private boolean isBackup(RecordStore recordStore) {
        int partitionId = recordStore.getPartitionId();
        IPartition partition = partitionService.getPartition(partitionId, false);
        return !partition.isLocal();
    }

    @Override
    public void reset() {
        mapDataStore.reset();
        storage.clear(false);
        stats.reset();
        mutationObserver.onReset();
    }

    @Override
    public void destroy() {
        clearPartition(false, true);
    }

    @Override
    public void clearPartition(boolean onShutdown, boolean onStorageDestroy) {
        clearLockStore();
        mapDataStore.reset();

        if (onShutdown) {
            if (hasPooledMemoryAllocator()) {
                destroyStorageImmediate(true, true);
            } else {
                destroyStorageAfterClear(true, true);
            }
        } else {
            if (onStorageDestroy) {
                destroyStorageAfterClear(false, false);
            } else {
                clearStorage(false);
            }
        }
    }

    private boolean hasPooledMemoryAllocator() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        NativeMemoryConfig nativeMemoryConfig = nodeEngine.getConfig().getNativeMemoryConfig();
        return nativeMemoryConfig != null && nativeMemoryConfig.getAllocatorType() == POOLED;
    }

    private void destroyStorageImmediate(boolean isDuringShutdown, boolean internal) {
        storage.destroy(isDuringShutdown);
        mutationObserver.onDestroy(isDuringShutdown, internal);
    }

    /**
     * Calls also {@link #clearStorage(boolean)} to release allocated HD memory
     * of key+value pairs because {@link #destroyStorageImmediate(boolean, boolean)}
     * only releases internal resources of backing data structure.
     *
     * @param isDuringShutdown {@link Storage#clear(boolean)}
     * @param internal         see {@link MutationObserver#onDestroy(boolean, boolean)}}
     */
    public void destroyStorageAfterClear(boolean isDuringShutdown, boolean internal) {
        clearStorage(isDuringShutdown);
        destroyStorageImmediate(isDuringShutdown, internal);
    }

    private void clearStorage(boolean isDuringShutdown) {
        storage.clear(isDuringShutdown);
        mutationObserver.onClear();
    }

    private void clearLockStore() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        LockSupportService lockService = nodeEngine.getServiceOrNull(LockSupportService.SERVICE_NAME);
        if (lockService != null) {
            ObjectNamespace namespace = MapService.getObjectNamespace(name);
            lockService.clearLockStore(partitionId, namespace);
        }
    }
}
