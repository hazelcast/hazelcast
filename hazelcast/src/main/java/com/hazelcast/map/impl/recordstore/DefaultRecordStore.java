/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryReason;
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
import static com.hazelcast.map.impl.mapstore.MapDataStores.EMPTY_MAP_DATA_STORE;
import static com.hazelcast.map.impl.record.Record.UNSET;
import static com.hazelcast.map.impl.recordstore.StaticParams.PUT_BACKUP_PARAMS;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;

/**
 * Default implementation of a record store.
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
     * A reference to the Json Metadata store. It is initialized lazily only if the
     * store is needed.
     */
    private JsonMetadataStore metadataStore;

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
        initJsonMetadataStore();
    }

    // Overridden in EE
    protected void initJsonMetadataStore() {
        // Forcibly initialize on-heap Json Metadata Store to avoid
        // lazy initialization and potential race condition.
        getOrCreateMetadataStore();
    }

    @Override
    public MapDataStore<Data, Object> getMapDataStore() {
        return mapDataStore;
    }

    protected JsonMetadataStore createMetadataStore() {
        return new JsonMetadataStoreImpl();
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
    public JsonMetadataStore getOrCreateMetadataStore() {
        if (metadataStore == null) {
            metadataStore = createMetadataStore();
        }
        return metadataStore;
    }

    private void destroyMetadataStore() {
        if (metadataStore != null) {
            metadataStore.destroy();
        }
    }

    @Override
    public Record getRecord(Data key) {
        return storage.get(key);
    }

    @Override
    public Record putOrUpdateReplicatedRecord(Data dataKey, Record replicatedRecord,
                                              ExpiryMetadata expiryMetadata,
                                              boolean indexesMustBePopulated, long now) {
        Record newRecord = storage.get(dataKey);
        if (newRecord == null) {
            newRecord = createRecord(dataKey, replicatedRecord != null
                    ? replicatedRecord.getValue() : null, now);
            storage.put(dataKey, newRecord);
        } else {
            storage.updateRecordValue(dataKey, newRecord, replicatedRecord.getValue());
        }

        Records.copyMetadataFrom(replicatedRecord, newRecord);
        expirySystem.add(dataKey, expiryMetadata, now);
        mutationObserver.onReplicationPutRecord(dataKey, newRecord, indexesMustBePopulated);
        updateStatsOnPut(replicatedRecord.getHits(), now);

        return newRecord;
    }

    @Override
    public void removeReplicatedRecord(Data dataKey) {
        Record record = storage.get(dataKey);
        if (record != null) {
            mutationObserver.onRemoveRecord(dataKey, record);
            removeKeyFromExpirySystem(dataKey);
            storage.removeRecord(dataKey, record);
        }
    }

    @Override
    public Record putBackup(Data dataKey, Record newRecord, ExpiryMetadata expiryMetadata,
                            boolean putTransient, CallerProvenance provenance) {
        return putBackupInternal(dataKey, newRecord.getValue(),
                expiryMetadata.getTtl(), expiryMetadata.getMaxIdle(), expiryMetadata.getExpirationTime(),
                putTransient, provenance, null);
    }

    @Override
    public Record putBackup(Data dataKey, Record record, long ttl,
                            long maxIdle, long nowOrExpiryTime, CallerProvenance provenance) {
        return putBackupInternal(dataKey, record.getValue(),
                ttl, maxIdle, nowOrExpiryTime, false, provenance, null);
    }

    @Override
    public Record putBackupTxn(Data dataKey, Record newRecord, ExpiryMetadata expiryMetadata,
                               boolean putTransient, CallerProvenance provenance, UUID transactionId) {
        return putBackupInternal(dataKey, newRecord.getValue(),
                expiryMetadata.getTtl(), expiryMetadata.getMaxIdle(), expiryMetadata.getExpirationTime(),
                putTransient, provenance, transactionId);
    }

    @Override
    public Record putBackup(Data key, Object value, long ttl, long maxIdle,
                            long nowOrExpiryTime, CallerProvenance provenance) {
        return putBackupInternal(key, value, ttl, maxIdle, nowOrExpiryTime,
                false, provenance, null);
    }

    private Record putBackupInternal(Data key, Object value, long ttl, long maxIdle, long expiryTime,
                                     boolean putTransient, CallerProvenance provenance,
                                     UUID transactionId) {
        long now = getNow();
        putInternal(key, value, ttl, maxIdle, expiryTime, now,
                null, null, null, PUT_BACKUP_PARAMS);

        Record record = getRecord(key);

        if (persistenceEnabledFor(provenance)) {
            if (putTransient) {
                mapDataStore.addTransient(key, now);
            } else {
                mapDataStore.addBackup(key, value,
                        expirySystem.getExpiryMetadata(key).getExpirationTime(),
                        now, transactionId);
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

        long now = getNow();
        Iterator<Map.Entry<Data, Record>> entries = storage.mutationTolerantIterator();
        while (entries.hasNext()) {
            Map.Entry<Data, Record> entry = entries.next();

            Data key = entry.getKey();
            Record record = entry.getValue();

            if (includeExpiredRecords
                    || hasExpired(key, now, backup) == ExpiryReason.NOT_EXPIRED) {
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

            if (evictIfExpired(key, now, false)) {
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

    /**
     * Loads value of key. If necessary loads it by
     * extracting from {@link MetadataAwareValue}
     *
     * @return loaded value from map-store,
     * when no value found returns null
     */
    private Object loadValueOf(Data key) {
        Object value = mapDataStore.load(key);
        if (value == null) {
            return null;
        }

        if (mapDataStore.isWithExpirationTime()) {
            MetadataAwareValue loaderEntry = (MetadataAwareValue) value;
            long proposedTtl = expirationTimeToTtl(loaderEntry.getExpirationTime());
            if (proposedTtl <= 0) {
                return null;
            }
            value = loaderEntry.getValue();
        }

        return value;
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
        long now = getNow();
        Record record = putNewRecord(key, null, value, ttl, UNSET, UNSET, now,
                null, LOADED, false, backup);
        if (!backup && mapEventPublisher.hasEventListener(name)) {
            mapEventPublisher.publishEvent(callerAddress, name, EntryEventType.LOADED,
                    key, null, record.getValue(), null);
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
        removeKeyFromExpirySystem(dataKey);
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
            removeKeyFromExpirySystem(key);
            storage.removeRecord(key, record);
            if (!backup) {
                mapServiceContext.interceptRemove(interceptorRegistry, value);
            }
        }
        return value;
    }

    private void removeKeyFromExpirySystem(Data key) {
        expirySystem.removeKeyFromExpirySystem(key);
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
        removeKeyFromExpirySystem(key);
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
            oldValue = loadValueOf(key);
            if (oldValue != null && persistenceEnabledFor(provenance)) {
                mapDataStore.remove(key, now, transactionId);
                updateStatsOnRemove(now);
            }
        } else {
            oldValue = removeRecord(key, record, now, provenance, transactionId);
            updateStatsOnRemove(now);
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
            oldValue = loadValueOf(key);
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
                removeKeyFromExpirySystem(key);
                storage.removeRecord(key, record);
                updateStatsOnRemove(now);
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
        if (record != null && touch) {
            accessRecord(key, record, now);
        } else if (record == null && mapDataStore != EMPTY_MAP_DATA_STORE) {
            record = loadRecordOrNull(key, backup, callerAddress);
            record = evictIfExpired(key, now, backup) ? null : record;
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
                record.setLastAccessTime(getNow());
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
                accessRecord(key, record, now);
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
                putFromLoad(key, loaderEntry.getValue(),
                        loaderEntry.getExpirationTime(), callerAddress);

            } else {
                resultMap.put(key, value);
                putFromLoad(key, value, callerAddress);
            }
        }

        if (hasQueryCache()) {
            for (Data key : resultMap.keySet()) {
                Record record = storage.get(key);
                // here we are only publishing events for loaded
                // entries. This is required for notifying query-caches
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
            accessRecord(key, record, now);
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
    public boolean setTtl(Data key, long ttl) {
        long now = getNow();
        Object oldValue = putInternal(key, null, ttl, UNSET, UNSET, now,
                null, null, null, StaticParams.SET_TTL_PARAMS);
        return oldValue != null;
    }

    @Override
    public boolean setTtlBackup(Data key, long ttl) {
        long now = getNow();
        Object oldValue = putInternal(key, null, ttl, UNSET, UNSET, now,
                null, null, null, StaticParams.SET_TTL_BACKUP_PARAMS);
        return oldValue != null;
    }

    @Override
    public Object set(Data dataKey, Object value, long ttl, long maxIdle) {
        long now = getNow();
        return putInternal(dataKey, value, ttl, maxIdle, UNSET, now,
                null, null, null, StaticParams.SET_PARAMS);
    }

    @Override
    public Object setTxn(Data dataKey, Object value, long ttl, long maxIdle, UUID transactionId) {
        long now = getNow();
        return putInternal(dataKey, value, ttl, maxIdle, UNSET, now,
                null, transactionId, null, StaticParams.SET_PARAMS);
    }

    @Override
    public Object put(Data key, Object value, long ttl, long maxIdle) {
        long now = getNow();
        return putInternal(key, value, ttl, maxIdle, UNSET, now,
                null, null, null, StaticParams.PUT_PARAMS);
    }

    /**
     * Core put method for all variants of puts/updates.
     *
     * @return old value if this is an update operation, otherwise returns null
     */
    @SuppressWarnings({"checkstyle:npathcomplexity",
            "checkstyle:parameternumber", "checkstyle:cyclomaticcomplexity"})
    private Object putInternal(Data key, Object newValue, long ttl,
                               long maxIdle, long expiryTime, long now, Object expectedValue,
                               @Nullable UUID transactionId, Address callerAddress,
                               StaticParams staticParams) {
        // If this method has to wait end of map loading.
        if (staticParams.isCheckIfLoaded()) {
            checkIfLoaded();
        }

        Object oldValue = null;

        // Get record by checking expiry, if expired, evict record.
        Record record = getRecordOrNull(key, now, staticParams.isBackup());

        // Variants of loading oldValue
        if (staticParams.isPutVanilla()) {
            oldValue = record == null
                    ? (staticParams.isLoad() ? loadValueOf(key) : null) : record.getValue();
        } else if (staticParams.isPutIfAbsent()) {
            record = getOrLoadRecord(record, key, now, callerAddress, staticParams.isBackup());
            // if this is an existing record, return existing value.
            if (record != null) {
                return record.getValue();
            }
        } else if (staticParams.isPutIfExists()) {
            // For methods like setTtl and replace,
            // when no matching record, just return.
            record = getOrLoadRecord(record, key, now, callerAddress, staticParams.isBackup());
            if (record == null) {
                return null;
            }
            oldValue = record.getValue();
            newValue = staticParams.isSetTtl() ? oldValue : newValue;
        }

        // For method replace, if current value is not expected one, return.
        if (staticParams.isPutIfEqual()
                && !valueComparator.isEqual(expectedValue, oldValue, serializationService)) {
            return null;
        }

        // Intercept put on owner partition.
        if (!staticParams.isBackup()) {
            newValue = mapServiceContext.interceptPut(interceptorRegistry, oldValue, newValue);
        }

        // Put new record or update existing one.
        if (record == null) {
            putNewRecord(key, oldValue, newValue, ttl, maxIdle, expiryTime, now,
                    transactionId, staticParams.isPutFromLoad() ? LOADED : ADDED,
                    staticParams.isStore(), staticParams.isBackup());
        } else {
            oldValue = updateRecord(record, key, oldValue, newValue, ttl, maxIdle, expiryTime, now,
                    transactionId, staticParams.isStore(),
                    staticParams.isCountAsAccess(), staticParams.isBackup());
        }
        return oldValue;
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected Record putNewRecord(Data key, Object oldValue, Object newValue, long ttl,
                                  long maxIdle, long expiryTime, long now, UUID transactionId,
                                  EntryEventType entryEventType, boolean store,
                                  boolean backup) {
        Record record = createRecord(key, newValue, now);
        if (mapDataStore != EMPTY_MAP_DATA_STORE && store) {
            putIntoMapStore(record, key, newValue, ttl, maxIdle, now, transactionId);
        }
        storage.put(key, record);
        expirySystem.add(key, ttl, maxIdle, expiryTime, now, now);

        if (entryEventType == EntryEventType.LOADED) {
            mutationObserver.onLoadRecord(key, record, backup);
        } else {
            mutationObserver.onPutRecord(key, record, oldValue, backup);
        }
        return record;
    }

    @SuppressWarnings("checkstyle:parameternumber")
    protected Object updateRecord(Record record, Data key, Object oldValue, Object newValue,
                                  long ttl, long maxIdle, long expiryTime, long now, UUID transactionId,
                                  boolean store, boolean countAsAccess, boolean backup) {
        updateStatsOnPut(countAsAccess, now);
        record.onUpdate(now);

        if (countAsAccess) {
            record.onAccess(now);
        }

        if (mapDataStore != EMPTY_MAP_DATA_STORE && store) {
            newValue = putIntoMapStore(record, key, newValue,
                    ttl, maxIdle, now, transactionId);
        }

        storage.updateRecordValue(key, record, newValue);
        expirySystem.add(key, ttl, maxIdle, expiryTime, now, now);
        mutationObserver.onUpdateRecord(key, record, oldValue, newValue, backup);
        return oldValue;
    }

    private Record getOrLoadRecord(@Nullable Record record, Data key,
                                   long now, Address callerAddress, boolean backup) {
        if (record != null) {
            accessRecord(key, record, now);
            return record;
        }

        return loadRecordOrNull(key, backup, callerAddress);
    }

    protected Object putIntoMapStore(Record record, Data key, Object newValue,
                                     long ttlMillis, long maxIdleMillis,
                                     long now, UUID transactionId) {
        long expirationTime = expirySystem.calculateExpirationTime(ttlMillis, maxIdleMillis, now, now);
        newValue = mapDataStore.add(key, newValue, expirationTime, now, transactionId);
        if (mapDataStore.isPostProcessingMapStore()) {
            storage.updateRecordValue(key, record, newValue);
        }
        onStore(record);
        return newValue;
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
            boolean persist = persistenceEnabledFor(provenance);
            putNewRecord(key, null, newValue, UNSET, UNSET, UNSET, now,
                    null, ADDED, persist, false);
        } else {
            oldValue = record.getValue();
            ExpiryMetadata expiryMetadata = expirySystem.getExpiryMetadata(key);
            MapMergeTypes<Object, Object> existingEntry
                    = createMergingEntry(serializationService, key, record, expiryMetadata);
            newValue = mergePolicy.merge(mergingEntry, existingEntry);
            // existing entry will be removed
            if (newValue == null) {
                if (persistenceEnabledFor(provenance)) {
                    mapDataStore.remove(key, now, null);
                }
                onStore(record);
                mutationObserver.onRemoveRecord(key, record);
                removeKeyFromExpirySystem(key);
                storage.removeRecord(key, record);
                return true;
            }

            if (valueComparator.isEqual(newValue, oldValue, serializationService)) {
                mergeRecordExpiration(key, record, mergingEntry, now);
                return true;
            }

            boolean persist = persistenceEnabledFor(provenance);
            updateRecord(record, key, oldValue, newValue, UNSET, UNSET, UNSET, now,
                    null, persist, true, false);
        }

        return newValue != null;
    }

    @Override
    public Object replace(Data key, Object update) {
        long now = getNow();
        return putInternal(key, update, UNSET, UNSET, UNSET, now,
                null, null, null, StaticParams.REPLACE_PARAMS);
    }

    @Override
    public boolean replace(Data key, Object expect, Object update) {
        long now = getNow();
        Object oldValue = putInternal(key, update, UNSET, UNSET, UNSET, now,
                expect, null, null, StaticParams.REPLACE_IF_SAME_PARAMS);
        return oldValue != null;
    }

    @Override
    public Object putTransient(Data key, Object value, long ttl, long maxIdle) {
        long now = getNow();
        Object oldValue = putInternal(key, value, ttl, maxIdle, UNSET, now,
                null, null, null, StaticParams.PUT_TRANSIENT_PARAMS);
        mapDataStore.addTransient(key, now);
        return oldValue;
    }

    @Override
    public Object putFromLoad(Data key, Object value, Address callerAddress) {
        return putFromLoadInternal(key, value, UNSET, UNSET, callerAddress, StaticParams.PUT_FROM_LOAD_PARAMS);
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
        return putFromLoadInternal(key, value, ttl, UNSET, callerAddress, StaticParams.PUT_FROM_LOAD_PARAMS);
    }

    @Override
    public Object putFromLoadBackup(Data key, Object value) {
        return putFromLoadInternal(key, value, UNSET, UNSET,
                null, StaticParams.PUT_FROM_LOAD_BACKUP_PARAMS);
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
        return putFromLoadInternal(key, value, ttl, UNSET,
                null, StaticParams.PUT_FROM_LOAD_BACKUP_PARAMS);
    }

    private Object putFromLoadInternal(Data key, Object newValue, long ttl,
                                       long maxIdle, Address callerAddress, StaticParams staticParams) {
        checkKeyAndValue(key, newValue);

        if (shouldEvict()) {
            return null;
        }

        long now = getNow();
        Object oldValue = putInternal(key, newValue, ttl, maxIdle, UNSET, now,
                null, null, null, staticParams);

        if (!staticParams.isBackup() && mapEventPublisher.hasEventListener(name)) {
            Record record = getRecord(key);
            EntryEventType entryEventType = oldValue == null ? LOADED : UPDATED;
            mapEventPublisher.publishEvent(callerAddress, name, entryEventType, key, oldValue, record.getValue());
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
        long now = getNow();
        Object oldValue = putInternal(dataKey, value, ttl, maxIdle, UNSET, now,
                null, null, null, StaticParams.SET_WTH_NO_ACCESS_PARAMS);
        return oldValue == null;
    }

    @Override
    public Object putIfAbsent(Data key, Object value, long ttl,
                              long maxIdle, Address callerAddress) {
        long now = getNow();
        return putInternal(key, value, ttl, maxIdle, UNSET, now,
                null, null, callerAddress, StaticParams.PUT_IF_ABSENT_PARAMS);
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
        removeKeyFromExpirySystem(key);
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
        if (record != null) {
            return evictIfExpired(key, now, backup) ? null : record;
        }

        return null;
    }

    protected void onStore(Record record) {
        if (record == null || mapDataStore == EMPTY_MAP_DATA_STORE) {
            return;
        }

        record.onStore();
    }

    private void updateStoreStats() {
        if (!(mapDataStore instanceof WriteBehindStore)
                || !mapContainer.getMapConfig().isPerEntryStatsEnabled()) {
            return;
        }

        long now = getNow();
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
        if (mapDataStore == EMPTY_MAP_DATA_STORE
                || loadingFutures.isEmpty()) {
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
            final Set<Data> lockedKeySet = lockStore.getLockedKeys();

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
            final Set<Data> lockedKeySet = lockStore.getLockedKeys();

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
        int removedKeyCount = removeBulk(keys, records);
        if (removedKeyCount > 0) {
            updateStatsOnRemove(Clock.currentTimeMillis());
        }
        return removedKeyCount;
    }

    private boolean isBackup(RecordStore recordStore) {
        int partitionId = recordStore.getPartitionId();
        IPartition partition = partitionService.getPartition(partitionId, false);
        return !partition.isLocal();
    }

    @Override
    public void reset() {
        try {
            mutationObserver.onReset();
        } finally {
            mapDataStore.reset();
            expirySystem.clear();
            storage.clear(false);
            stats.reset();
        }
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
        mutationObserver.onDestroy(isDuringShutdown, internal);
        expirySystem.destroy();
        destroyMetadataStore();
        // Destroy storage in the end
        storage.destroy(isDuringShutdown);
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
        try {
            mutationObserver.onClear();
        } finally {
            expirySystem.clear();
            storage.clear(isDuringShutdown);
        }
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
