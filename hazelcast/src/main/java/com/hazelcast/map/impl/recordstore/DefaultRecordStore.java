/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.locksupport.LockSupportService;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.CollectionUtil;
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
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.spi.partition.IPartitionService;
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

import static com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType.POOLED;
import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.LOADED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.map.impl.ExpirationTimeSetter.setExpirationTimes;
import static com.hazelcast.map.impl.mapstore.MapDataStores.EMPTY_MAP_DATA_STORE;
import static com.hazelcast.map.impl.record.Record.UNSET;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static java.util.Collections.emptyList;

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
     *
     * @param recordsToBeFlushed records to be flushed to map-store.
     * @param backup             <code>true</code> if backup, false otherwise.
     */
    private void flush(Collection<Record> recordsToBeFlushed, boolean backup) {
        for (Record record : recordsToBeFlushed) {
            mapDataStore.flush(record.getKey(), record.getValue(), backup);
        }
    }

    @Override
    public Record getRecord(Data key) {
        return storage.get(key);
    }

    @Override
    public Record putReplicatedRecord(Record replicatedRecord, long nowInMillis) {
        Record newRecord = createRecord(replicatedRecord, nowInMillis);
        markRecordStoreExpirable(replicatedRecord.getTtl(), replicatedRecord.getMaxIdle());
        // Note that newRecord may not know its key yet, key assignment
        // is done after storage put in some cases so better to
        // use key of replicatedRecord when doing storage.put
        storage.put(replicatedRecord.getKey(), newRecord);
        mutationObserver.onReplicationPutRecord(newRecord.getKey(), newRecord);
        updateStatsOnPut(replicatedRecord.getHits(), nowInMillis);
        return newRecord;
    }

    @Override
    public Record putBackup(Record newRecord, boolean putTransient, CallerProvenance provenance) {
        return putBackupInternal(newRecord.getKey(), newRecord.getValue(),
                newRecord.getTtl(), newRecord.getMaxIdle(), putTransient, provenance, null);
    }

    @Override
    public Record putBackupTxn(Record newRecord, boolean putTransient,
                               CallerProvenance provenance, UUID transactionId) {
        return putBackupInternal(newRecord.getKey(), newRecord.getValue(),
                newRecord.getTtl(), newRecord.getMaxIdle(), putTransient, provenance, transactionId);
    }

    @Override
    public Record putBackup(Data key, Object value, CallerProvenance provenance) {
        return putBackupInternal(key, value, UNSET, UNSET,
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
            mutationObserver.onPutRecord(key, record);
        } else {
            updateRecord(key, record, value, now, true,
                    ttl, maxIdle, false, transactionId);
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
        Record record;
        long ttl = UNSET;
        Object value = mapDataStore.load(key);
        if (value == null) {
            return null;
        }
        if (mapDataStore.isWithExpirationTime()) {
            MetadataAwareValue loaderEntry = (MetadataAwareValue) value;
            long proposedTtl = expirationTimeToTtl(loaderEntry.getExpirationTime());
            if (proposedTtl < 0) {
                return null;
            }
            value = loaderEntry.getValue();
            ttl = proposedTtl;
        }
        record = createRecord(key, value, ttl, UNSET, getNow());
        markRecordStoreExpirable(ttl, UNSET);
        storage.put(key, record);
        mutationObserver.onLoadRecord(key, record);
        if (!backup) {
            saveIndex(record, null);
            mapEventPublisher.publishEvent(callerAddress, name, EntryEventType.LOADED,
                    key, null, value, null);
        }
        evictEntries(key);
        // here, we are only publishing events for loaded
        // entries. This is required for notifying query-caches
        // otherwise query-caches cannot see loaded entries
        if (!backup && hasQueryCache()) {
            addEventToQueryCache(record);
        }
        return record;
    }

    private long expirationTimeToTtl(long definedExpirationTime) {
        return definedExpirationTime - System.currentTimeMillis();
    }

    protected List<Data> getKeysFromRecords(Collection<Record> clearableRecords) {
        List<Data> keys = new ArrayList<>(clearableRecords.size());
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

        List<Record> notLockedRecords = new ArrayList<>(notLockedKeyCount);
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
                mapServiceContext.interceptRemove(interceptorRegistry, value);
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
        storage.removeRecord(record);
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
                removeIndex(record);
                onStore(record);
                mutationObserver.onRemoveRecord(key, record);
                storage.removeRecord(record);
            }
            removed = true;
        }
        return removed;
    }

    @Override
    public Object get(Data key, boolean backup, Address callerAddress) {
        checkIfLoaded();
        long now = getNow();

        Record record = getRecordOrNull(key, now, backup);
        if (record == null) {
            record = loadRecordOrNull(key, backup, callerAddress);
            record = getOrNullIfExpired(record, now, backup);
        } else {
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
            if (mapDataStore.isWithExpirationTime()) {
                MetadataAwareValue loaderEntry = (MetadataAwareValue) value;

                if (expirationTimeToTtl(loaderEntry.getExpirationTime()) >= 0) {
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
                addEventToQueryCache(record);
            }
        }
        return resultMap;
    }

    protected void addMapEntrySet(Object key, Object value, MapEntries mapEntries) {
        if (key == null || value == null) {
            return;
        }
        value = mapServiceContext.interceptGet(interceptorRegistry, value);
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
    public boolean setTtl(Data key, long ttl) {
        long now = getNow();
        Record record = getRecordOrNull(key, now, false);
        Object existingValue = record == null ? mapDataStore.load(key) : record.getValue();
        if (existingValue == null) {
            return false;
        }
        if (record == null) {
            createRecord(key, existingValue, ttl, UNSET, now);
        } else {
            updateRecord(key, record, existingValue, now, true, ttl,
                    UNSET, true, null);
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

    protected Object putInternal(Data key, Object value, long ttl,
                                 long maxIdle, @Nullable UUID transactionId,
                                 boolean loadFromStore, boolean countAsAccess) {
        checkIfLoaded();

        long now = getNow();
        markRecordStoreExpirable(ttl, maxIdle);

        Record record = getRecordOrNull(key, now, false);
        Object oldValue = record == null
                ? (loadFromStore ? mapDataStore.load(key) : null) : record.getValue();
        value = mapServiceContext.interceptPut(interceptorRegistry, oldValue, value);
        onStore(record);

        if (record == null) {
            record = putNewRecord(key, value, ttl, maxIdle, now, transactionId);
        } else {
            updateRecord(key, record, value, now, countAsAccess, ttl, maxIdle, true, transactionId);
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

            record = createRecord(key, newValue, UNSET, UNSET, now);
            mergeRecordExpiration(record, mergingEntry);
            newValue = persistenceEnabledFor(provenance)
                    ? mapDataStore.add(key, newValue, record.getExpirationTime(), now, null) : newValue;
            recordFactory.setValue(record, newValue);
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
                    mapDataStore.remove(key, now, null);
                }
                onStore(record);
                mutationObserver.onRemoveRecord(key, record);
                storage.removeRecord(record);
                return true;
            }

            if (valueComparator.isEqual(newValue, oldValue, serializationService)) {
                mergeRecordExpiration(record, mergingEntry);
                return true;
            }

            newValue = persistenceEnabledFor(provenance)
                    ? mapDataStore.add(key, newValue, record.getExpirationTime(), now, null) : newValue;
            onStore(record);
            mutationObserver.onUpdateRecord(key, record, newValue);
            storage.updateRecordValue(key, record, newValue);
        }
        saveIndex(record, oldValue);
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
            record = putNewRecord(key, update, UNSET, UNSET, now, null);
        } else {
            updateRecord(key, record, update, now, true,
                    UNSET, UNSET, true, null);
        }
        onStore(record);
        saveIndex(record, oldValue);
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
            record = putNewRecord(key, update, UNSET, UNSET, now, null);
        } else {
            updateRecord(key, record, update, now, true,
                    UNSET, UNSET, true, null);
        }
        onStore(record);
        setExpirationTimes(record.getTtl(), record.getMaxIdle(), record,
                mapContainer.getMapConfig(), false);
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
            value = mapServiceContext.interceptPut(interceptorRegistry, null, value);
            record = createRecord(key, value, ttl, maxIdle, now);
            storage.put(key, record);
            mutationObserver.onPutRecord(key, record);
        } else {
            oldValue = record.getValue();
            value = mapServiceContext.interceptPut(interceptorRegistry, oldValue, value);
            updateRecord(key, record, value, now, true, UNSET,
                    UNSET, false, null);
            setExpirationTimes(ttl, maxIdle, record, mapContainer.getMapConfig(), false);
        }
        saveIndex(record, oldValue);
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
        if (ttl < 0) {
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
        if (ttl < 0) {
            return null;
        }
        return putFromLoadInternal(key, value, ttl, UNSET, true, null);
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
        EntryEventType entryEventType;
        if (record == null) {
            value = mapServiceContext.interceptPut(interceptorRegistry, null, value);
            record = createRecord(key, value, ttl, maxIdle, now);
            storage.put(key, record);
            mutationObserver.onLoadRecord(key, record);
            entryEventType = LOADED;
        } else {
            oldValue = record.getValue();
            value = mapServiceContext.interceptPut(interceptorRegistry, oldValue, value);
            updateRecord(key, record, value, now, true,
                    ttl, maxIdle, false, null);
            entryEventType = UPDATED;
        }

        if (!backup) {
            saveIndex(record, oldValue);
            mapEventPublisher.publishEvent(callerAddress, name, entryEventType, key, oldValue, value);
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
    public boolean setWithUncountedAccess(Data dataKey, Object value, long ttl, long maxIdle) {
        Object oldValue = putInternal(dataKey, value, ttl, maxIdle,
                null, false, false);
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
                record = createRecord(key, oldValue, UNSET, UNSET, now);
                storage.put(key, record);

                mutationObserver.onPutRecord(key, record);
                mapEventPublisher.publishEvent(callerAddress, name, EntryEventType.LOADED, key, null, oldValue);
            }
        } else {
            accessRecord(record, now);
            oldValue = record.getValue();
        }
        if (oldValue == null) {
            value = mapServiceContext.interceptPut(interceptorRegistry, null, value);
            onStore(record);
            record = putNewRecord(key, value, ttl, maxIdle, now, null);
        }
        saveIndex(record, oldValue);
        return oldValue;
    }

    protected Object removeRecord(Data key, @Nonnull Record record,
                                  long now, CallerProvenance provenance,
                                  UUID transactionId) {
        Object oldValue = record.getValue();
        oldValue = mapServiceContext.interceptRemove(interceptorRegistry, oldValue);
        if (oldValue != null) {
            removeIndex(record);
            if (persistenceEnabledFor(provenance)) {
                mapDataStore.remove(key, now, transactionId);
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
    }

    @Override
    public void clearPartition(boolean onShutdown, boolean onStorageDestroy) {
        clearLockStore();
        clearOtherDataThanStorage(onShutdown, onStorageDestroy);

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

    /**
     * Only cleans the data other than storage-data that is held on this record
     * store. Other services data like lock-service-data is not cleared here.
     */
    public void clearOtherDataThanStorage(boolean onShutdown, boolean onStorageDestroy) {
        clearMapStore();
        clearIndexedData(onShutdown, onStorageDestroy);
    }

    private void destroyStorageImmediate(boolean isDuringShutdown, boolean internal) {
        storage.destroy(isDuringShutdown);
        mutationObserver.onDestroy(internal);
    }

    /**
     * Calls also {@link #clearStorage(boolean)} to release allocated HD memory
     * of key+value pairs because {@link #destroyStorageImmediate(boolean, boolean)}
     * only releases internal resources of backing data structure.
     *
     * @param isDuringShutdown {@link Storage#clear(boolean)}
     * @param internal         see {@link MutationObserver#onDestroy(boolean)}}
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

    private void clearMapStore() {
        mapDataStore.reset();
    }

    /**
     * Only indexed data will be removed, index info will stay.
     */
    private void clearIndexedData(boolean onShutdown, boolean onStorageDestroy) {
        clearGlobalIndexes(onShutdown);
        clearPartitionedIndexes(onStorageDestroy);
    }

    private void clearGlobalIndexes(boolean onShutdown) {
        Indexes indexes = mapContainer.getIndexes(partitionId);
        if (indexes.isGlobal()) {
            if (onShutdown) {
                indexes.destroyIndexes();
            } else {
                if (indexes.haveAtLeastOneIndex()) {
                    // clears indexed data of this partition
                    // from shared global index.
                    fullScanLocalDataToClear(indexes);
                }
            }
        }
    }

    private void clearPartitionedIndexes(boolean onStorageDestroy) {
        Indexes indexes = mapContainer.getIndexes(partitionId);
        if (indexes.isGlobal()) {
            return;
        }

        if (onStorageDestroy) {
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
        InternalIndex[] indexesSnapshot = indexes.getIndexes();
        for (Record record : storage.values()) {
            Data key = record.getKey();
            Object value = Records.getValueOrCachedValue(record, serializationService);
            indexes.removeEntry(key, value, Index.OperationSource.SYSTEM);
        }
        Indexes.markPartitionAsUnindexed(partitionId, indexesSnapshot);
    }
}
