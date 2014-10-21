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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.cache.impl.record.CacheRecordFactory;
import com.hazelcast.cache.impl.record.CacheRecordHashMap;
import com.hazelcast.cache.impl.record.CacheRecordMap;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.EventServiceImpl;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.record.CacheRecordFactory.isExpiredAt;

/**
 * Implementation of the {@link ICacheRecordStore}
 * <p/>
 * Represents a named ICache on-heap data for a single partition.
 * Total data of an ICache object is the total CacheRecordStore on all partitions.
 * This data structure is the actual cache operations implementation, data access, statistics, event firing etc.
 * <p/>
 * CacheRecordStore is managed by CachePartitionSegment.
 *
 * @see com.hazelcast.cache.impl.CachePartitionSegment
 */
public class CacheRecordStore extends AbstractCacheRecordStore<CacheRecordMap<Data, CacheRecord>> {

    protected boolean hasExpiringEntry;
    protected final ScheduledFuture<?> evictionTaskFuture;

    public CacheRecordStore(String name,
                            int partitionId,
                            NodeEngine nodeEngine,
                            AbstractCacheService cacheService) {
        this(name, partitionId, nodeEngine, cacheService, null, null, null);
    }

    public CacheRecordStore(String name,
                            int partitionId,
                            NodeEngine nodeEngine,
                            AbstractCacheService cacheService,
                            SerializationService serializationService,
                            CacheRecordFactory cacheRecordFactory,
                            ExpiryPolicy expiryPolicy) {
        super(name,
              partitionId,
              nodeEngine,
              cacheService,
              serializationService,
              cacheRecordFactory,
              expiryPolicy);
        evictionTaskFuture =
                nodeEngine.getExecutionService()
                        .scheduleWithFixedDelay("hz:cache",
                                new EvictionTask(),
                                INITIAL_DELAY, PERIOD,
                                TimeUnit.SECONDS);
    }

    @Override
    protected CacheRecordMap createRecordCacheMap() {
        return new CacheRecordHashMap<Data, CacheRecord>(1000);
    }

    @Override
    protected CacheRecordFactory createCacheRecordFactory(InMemoryFormat inMemoryFormat,
                                                          SerializationService serializationService) {
        return new CacheRecordFactory(inMemoryFormat, serializationService);
    }

    @Override
    protected <T> Data valueToData(T value) {
        return cacheService.toData(value);
    }

    @Override
    public Object get(Data key, ExpiryPolicy expiryPolicy) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        long now = Clock.currentTimeMillis();
        Object value;
        CacheRecord record = records.get(key);
        final boolean isExpired = processExpiredEntry(key, record, now);
        if (record == null || isExpired) {
            if (isStatisticsEnabled()) {
                statistics.increaseCacheMisses(1);
            }
            value = readThroughCache(key);
            if (value == null) {
                return null;
            }
            createRecordWithExpiry(key, value, expiryPolicy, now, true);

            return value;
        } else {
            value = record.getValue();
            updateAccessDuration(record, expiryPolicy, now);
            if (isStatisticsEnabled()) {
                statistics.increaseCacheHits(1);
            }
            return value;
        }
    }

    @Override
    public void put(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        getAndPut(key, value, expiryPolicy, caller, false, false);
    }

    protected Object getAndPut(Data key,
                               Object value,
                               ExpiryPolicy expiryPolicy,
                               String caller,
                               boolean getValue,
                               boolean disableWriteThrough) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean isPutSucceed;
        Object oldValue = null;
        CacheRecord record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);
        // check that new entry is not already expired, in which case it should
        // not be added to the cache or listeners called or writers called.
        if (record == null || isExpired) {
            isPutSucceed = createRecordWithExpiry(key,
                                                  value,
                                                  expiryPolicy,
                                                  now,
                                                  disableWriteThrough);
        } else {
            oldValue = record.getValue();
            isPutSucceed = updateRecordWithExpiry(key,
                                                  value,
                                                  record,
                                                  expiryPolicy,
                                                  now,
                                                  disableWriteThrough);
        }
        updateGetAndPutStat(isPutSucceed, getValue, oldValue == null, start);
        return oldValue;
    }

    protected Object getAndPut(Data key,
                               Object value,
                               ExpiryPolicy expiryPolicy,
                               String caller,
                               boolean getValue) {
        return getAndPut(key, value, expiryPolicy, caller, getValue, false);
    }

    @Override
    public Object getAndPut(Data key,
                            Object value,
                            ExpiryPolicy expiryPolicy,
                            String caller) {
        return getAndPut(key, value, expiryPolicy, caller, true, false);
    }

    @Override
    public boolean putIfAbsent(Data key,
                               Object value,
                               ExpiryPolicy expiryPolicy,
                               String caller) {
        return putIfAbsent(key, value, expiryPolicy, caller, false);
    }

    protected boolean putIfAbsent(Data key,
                                  Object value,
                                  ExpiryPolicy expiryPolicy,
                                  String caller,
                                  boolean disableWriteThrough) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean result;
        CacheRecord record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);
        if (record == null || isExpired) {
            result = createRecordWithExpiry(key,
                                            value,
                                            expiryPolicy,
                                            now,
                                            disableWriteThrough);
        } else {
            result = false;
        }
        if (result && isStatisticsEnabled()) {
            statistics.increaseCachePuts(1);
            statistics.addPutTimeNano(System.nanoTime() - start);
        }
        return result;
    }

    @Override
    public Object getAndRemove(Data key, String caller) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        deleteCacheEntry(key);

        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);

        final Object result;
        if (record == null || isExpired) {
            result = null;
        } else {
            result = record.getValue();
            deleteRecord(key);
        }
        if (isStatisticsEnabled()) {
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (result != null) {
                statistics.increaseCacheHits(1);
                statistics.increaseCacheRemovals(1);
                statistics.addRemoveTimeNano(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return result;
    }

    @Override
    public boolean remove(Data key, String caller) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        deleteCacheEntry(key);

        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);

        boolean result = true;
        if (record == null || isExpired) {
            result = false;
        } else {
            deleteRecord(key);
        }
        if (result && isStatisticsEnabled()) {
            statistics.increaseCacheRemovals(1);
            statistics.addRemoveTimeNano(System.nanoTime() - start);
        }
        return result;
    }

    @Override
    public boolean remove(Data key, Object value, String caller) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        int hitCount = 0;

        boolean result = true;
        if (record == null || isExpired) {
            if (isStatisticsEnabled()) {
                statistics.increaseCacheMisses(1);
            }
            result = false;
        } else {
            hitCount++;
            if (compare(record.getValue(), value)) {
                deleteCacheEntry(key);
                deleteRecord(key);
            } else {
                long expiryTime = updateAccessDuration(record, defaultExpiryPolicy, now);
                processExpiredEntry(key, record, expiryTime, now);
                result = false;
            }
        }
        if (result && isStatisticsEnabled()) {
            statistics.increaseCacheRemovals(1);
            statistics.addRemoveTimeNano(System.nanoTime() - start);
            if (hitCount == 1) {
                statistics.increaseCacheHits(hitCount);
            } else {
                statistics.increaseCacheMisses(1);
            }

        }
        return result;
    }

    @Override
    public boolean replace(Data key,
                           Object value,
                           ExpiryPolicy expiryPolicy,
                           String caller) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean result;
        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        if (record == null || isExpired) {
            result = false;
        } else {
            result = updateRecordWithExpiry(key,
                                            value,
                                            record,
                                            expiryPolicy,
                                            now,
                                            false);
        }
        if (isStatisticsEnabled()) {
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (result) {
                statistics.increaseCachePuts(1);
                statistics.increaseCacheHits(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return result;
    }

    @Override
    public boolean replace(Data key,
                           Object oldValue,
                           Object newValue,
                           ExpiryPolicy expiryPolicy,
                           String caller) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean isHit = false;
        boolean result;
        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        if (record == null || isExpired) {
            result = false;
        } else {
            isHit = true;
            Object value = record.getValue();

            if (compare(value, oldValue)) {
                result = updateRecordWithExpiry(key,
                                                newValue,
                                                record,
                                                expiryPolicy,
                                                now,
                                                false);
            } else {
                updateAccessDuration(record, expiryPolicy, now);
                result = false;
            }
        }
        updateReplaceStat(result, isHit, start);
        return result;
    }

    @Override
    public Object getAndReplace(Data key,
                                Object value,
                                ExpiryPolicy expiryPolicy,
                                String caller) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        Object result = null;
        CacheRecord record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        if (record == null || isExpired) {
            result = null;
        } else {
            result = record.getValue();
            updateRecordWithExpiry(key, value, record, expiryPolicy, now, false);
        }
        if (isStatisticsEnabled()) {
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (result != null) {
                statistics.increaseCacheHits(1);
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return result;
    }

    @Override
    public boolean contains(Data key) {
        long now = Clock.currentTimeMillis();
        CacheRecord record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);
        return record != null && !isExpired;
    }

    @Override
    public MapEntrySet getAll(Set<Data> keySet, ExpiryPolicy expiryPolicy) {
        //we don not call loadAll. shouldn't we ?
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final MapEntrySet result = new MapEntrySet();
        for (Data key : keySet) {
            final Object value = get(key, expiryPolicy);
            if (value != null) {
                result.add(key, cacheService.toData(value));
            }
        }
        return result;
    }

    @Override
    public void clear() {
        records.clear();
    }

    @Override
    public void removeAll(Set<Data> keys) {
        final long now = Clock.currentTimeMillis();
        final Set<Data> localKeys = new HashSet<Data>(keys.isEmpty() ? records.keySet() : keys);
        try {
            deleteAllCacheEntry(localKeys);
        } finally {
            final Set<Data> keysToClean = new HashSet<Data>(keys.isEmpty() ? records.keySet() : keys);
            for (Data key : keysToClean) {
                isEventBatchingEnabled = true;
                final CacheRecord record = records.get(key);
                if (localKeys.contains(key) && record != null) {
                    final boolean isExpired = processExpiredEntry(key, record, now);
                    if (!isExpired) {
                        deleteRecord(key);
                        if (isStatisticsEnabled()) {
                            statistics.increaseCacheRemovals(1);
                        }
                    }
                } else {
                    keys.remove(key);
                }
                isEventBatchingEnabled = false;
                int orderKey = keys.hashCode();
                publishBatchedEvents(name, CacheEventType.REMOVED, orderKey);
            }
        }
    }

    @Override
    public void destroy() {
        clear();
        onDestroy();
        closeResources();
        //close the configured CacheEntryListeners
        EventService eventService = cacheService.getNodeEngine().getEventService();
        Collection<EventRegistration> candidates =
                eventService.getRegistrations(CacheService.SERVICE_NAME, name);

        for (EventRegistration registration : candidates) {
            if (((EventServiceImpl.Registration) registration).getListener() instanceof Closeable) {
                try {
                    ((Closeable) registration).close();
                } catch (IOException e) {
                    EmptyStatement.ignore(e);
                    //log
                }
            }
        }
    }

    protected void closeResources() {
        //close the configured CacheWriter
        if (cacheWriter instanceof Closeable) {
            IOUtil.closeResource((Closeable) cacheWriter);
        }

        //close the configured CacheLoader
        if (cacheLoader instanceof Closeable) {
            IOUtil.closeResource((Closeable) cacheLoader);
        }

        //close the configured defaultExpiryPolicy
        if (defaultExpiryPolicy instanceof Closeable) {
            IOUtil.closeResource((Closeable) defaultExpiryPolicy);
        }
    }

    public void onDestroy() {
        ScheduledFuture<?> f = evictionTaskFuture;
        if (f != null) {
            f.cancel(true);
        }
    }

    @Override
    public CacheRecord getRecord(Data key) {
        return records.get(key);
    }

    @Override
    public void setRecord(Data key, CacheRecord record) {
        if (key != null && record != null) {
            records.put(key, record);
        }
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        if (key != null) {
            return records.remove(key);
        }
        return null;
    }

    @Override
    public CacheKeyIteratorResult iterator(int tableIndex, int size) {
        return records.fetchNext(tableIndex, size);
    }

    @Override
    public Object invoke(Data key, EntryProcessor entryProcessor, Object[] arguments) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        CacheRecord record = records.get(key);
        final boolean isExpired = processExpiredEntry(key, record, now);
        if (isExpired) {
            record = null;
        }
        if (isStatisticsEnabled()) {
            if (record == null || isExpired) {
                statistics.increaseCacheMisses(1);
            } else {
                statistics.increaseCacheHits(1);
            }
        }
        if (isStatisticsEnabled()) {
            statistics.addGetTimeNano(System.nanoTime() - start);
        }
        CacheEntryProcessorEntry entry = createCacheEntryProcessorEntry(key, record, this, now);
        final Object process = entryProcessor.process(entry, arguments);
        entry.applyChanges();
        return process;
    }

    protected CacheEntryProcessorEntry createCacheEntryProcessorEntry(Data key,
                                                                      CacheRecord record,
                                                                      CacheRecordStore cacheRecordStore,
                                                                      long now) {
        return new CacheEntryProcessorEntry(key, record, this, now);
    }

    @Override
    public Set<Data> loadAll(Set<Data> keys, boolean replaceExistingValues) {
        Set<Data> keysLoaded = new HashSet<Data>();
        Map<Data, Object> loaded = loadAllCacheEntry(keys);
        if (loaded == null || loaded.isEmpty()) {
            return keysLoaded;
        }
        if (replaceExistingValues) {
            for (Map.Entry<Data, Object> entry : loaded.entrySet()) {
                final Data key = entry.getKey();
                final Object value = entry.getValue();
                if (value != null) {
                    getAndPut(key, value, null, null, false, true);
                    keysLoaded.add(key);
                }
            }
        } else {
            for (Map.Entry<Data, Object> entry : loaded.entrySet()) {
                final Data key = entry.getKey();
                final Object value = entry.getValue();
                if (value != null) {
                    final boolean hasPut = putIfAbsent(key, value, null, null, true);
                    if (hasPut) {
                        keysLoaded.add(key);
                    }
                }
            }
        }
        return keysLoaded;
    }

    public int evictExpiredRecords() {
        //TODO: Evict expired records and returns count of evicted records
        return 0;
    }

    protected boolean createRecordWithExpiry(Data key,
                                             Object value,
                                             ExpiryPolicy localExpiryPolicy,
                                             long now,
                                             boolean disableWriteThrough) {
        Duration expiryDuration;
        try {
            expiryDuration = localExpiryPolicy.getExpiryForCreation();
        } catch (Exception e) {
            expiryDuration = Duration.ETERNAL;
        }
        long expiryTime = expiryDuration.getAdjustedTime(now);

        if (!disableWriteThrough) {
            writeThroughCache(key, value);
        }

        if (!isExpiredAt(expiryTime, now)) {
            CacheRecord record = createRecord(key, value, expiryTime);
            records.put(key, record);
            return true;
        }
        return false;
    }

    protected CacheRecord createRecord(Data keyData, Object value, long expirationTime) {
        final CacheRecord record =
                cacheRecordFactory.newRecordWithExpiry(keyData, value, expirationTime);
        if (isEventsEnabled) {
            final Object recordValue = record.getValue();
            Data dataValue;
            if (!(recordValue instanceof Data)) {
                dataValue = cacheService.toData(recordValue);
            } else {
                dataValue = (Data) recordValue;
            }
            publishEvent(name, CacheEventType.CREATED, keyData, null, dataValue, false);
        }
        return record;
    }

    protected boolean updateRecordWithExpiry(Data key,
                                             Object value,
                                             CacheRecord record,
                                             ExpiryPolicy localExpiryPolicy,
                                             long now,
                                             boolean disableWriteThrough) {
        long expiryTime = -1L;
        try {
            Duration expiryDuration = localExpiryPolicy.getExpiryForUpdate();
            if (expiryDuration != null) {
                expiryTime = expiryDuration.getAdjustedTime(now);
                record.setExpirationTime(expiryTime);
            }
        } catch (Exception e) {
            EmptyStatement.ignore(e);
            //leave the expiry time untouched when we can't determine a duration
        }
        if (!disableWriteThrough) {
            writeThroughCache(key, value);
        }
        updateRecord(key, record, value);
        return !processExpiredEntry(key, record, expiryTime, now);
    }

    protected CacheRecord updateRecord(Data key, CacheRecord record, Object value) {
        final Data dataOldValue;
        final Data dataValue;
        Object v = value;
        switch (cacheConfig.getInMemoryFormat()) {
            case BINARY:
                if (!(value instanceof Data)) {
                    v = cacheService.toData(value);
                }
                dataValue = (Data) v;
                dataOldValue = (Data) record.getValue();
                break;
            case OBJECT:
                if (value instanceof Data) {
                    v = cacheService.toObject(value);
                    dataValue = (Data) value;
                } else {
                    dataValue = cacheService.toData(value);
                }
                dataOldValue = cacheService.toData(record.getValue());
                break;
            default:
                throw new IllegalArgumentException("Invalid storage format: "
                        + cacheConfig.getInMemoryFormat());
        }
        record.setValue(v);
        if (isEventsEnabled) {
            publishEvent(name,
                    CacheEventType.UPDATED,
                    key,
                    dataOldValue,
                    dataValue,
                    true);
        }
        return record;
    }

    protected void deleteRecord(Data key) {
        final CacheRecord record = records.remove(key);
        final Data dataValue;
        switch (cacheConfig.getInMemoryFormat()) {
            case BINARY:
                dataValue = (Data) record.getValue();
                break;
            case OBJECT:
                dataValue = cacheService.toData(record.getValue());
                break;
            default:
                throw new IllegalArgumentException("Invalid storage format: "
                        + cacheConfig.getInMemoryFormat());
        }
        if (isEventsEnabled) {
            publishEvent(name,
                    CacheEventType.REMOVED,
                    key,
                    null,
                    dataValue,
                    false);
        }
    }

    protected CacheRecord readThroughRecord(Data key, long now) {
        final ExpiryPolicy localExpiryPolicy = defaultExpiryPolicy;
        Object value = readThroughCache(key);
        if (value == null) {
            return null;
        }
        Duration expiryDuration;
        try {
            expiryDuration = localExpiryPolicy.getExpiryForCreation();
        } catch (Exception e) {
            expiryDuration = Duration.ETERNAL;
        }
        long expiryTime = expiryDuration.getAdjustedTime(now);

        if (isExpiredAt(expiryTime, now)) {
            return null;
        }
        //TODO below createRecord may fire create event, is it OK?
        final CacheRecord record = createRecord(key, value, expiryTime);
        return record;
    }

    protected Object readThroughCache(Data key) throws CacheLoaderException {
        if (this.isReadThrough() && cacheLoader != null) {
            try {
                Object o = cacheService.toObject(key);
                return cacheLoader.load(o);
            } catch (Exception e) {
                if (!(e instanceof CacheLoaderException)) {
                    throw new CacheLoaderException("Exception in CacheLoader during load", e);
                } else {
                    throw (CacheLoaderException) e;
                }
            }
        }
        return null;
    }

    protected void writeThroughCache(Data key, Object value) throws CacheWriterException {
        if (isWriteThrough() && cacheWriter != null) {
            try {
                final Object objKey = cacheService.toObject(key);
                final Object objValue;
                switch (cacheConfig.getInMemoryFormat()) {
                    case BINARY:
                        objValue = cacheService.toObject(value);
                        break;
                    case OBJECT:
                        objValue = value;
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid storage format: "
                                + cacheConfig.getInMemoryFormat());
                }
                CacheEntry<?, ?> entry = new CacheEntry<Object, Object>(objKey, objValue);
                cacheWriter.write(entry);
            } catch (Exception e) {
                if (!(e instanceof CacheWriterException)) {
                    throw new CacheWriterException("Exception in CacheWriter during write", e);
                } else {
                    throw (CacheWriterException) e;
                }
            }
        }
    }

    protected void deleteCacheEntry(Data key) {
        if (isWriteThrough() && cacheWriter != null) {
            try {
                final Object objKey = cacheService.toObject(key);
                cacheWriter.delete(objKey);
            } catch (Exception e) {
                if (!(e instanceof CacheWriterException)) {
                    throw new CacheWriterException("Exception in CacheWriter during delete", e);
                } else {
                    throw (CacheWriterException) e;
                }
            }
        }
    }

    /**
     * modifies the keys
     *
     * @param keys : keys to delete, after method returns it includes only deleted keys
     */
    protected void deleteAllCacheEntry(Set<Data> keys) {
        if (isWriteThrough() && cacheWriter != null && keys != null && !keys.isEmpty()) {
            Map<Object, Data> keysToDelete = new HashMap<Object, Data>();
            for (Data key : keys) {
                final Object localKeyObj = cacheService.toObject(key);
                keysToDelete.put(localKeyObj, key);
            }
            final Set<Object> keysObject = keysToDelete.keySet();
            try {
                cacheWriter.deleteAll(keysObject);
            } catch (Exception e) {
                if (!(e instanceof CacheWriterException)) {
                    throw new CacheWriterException("Exception in CacheWriter during deleteAll", e);
                } else {
                    throw (CacheWriterException) e;
                }
            } finally {
                for (Object undeletedKey : keysObject) {
                    final Data undeletedKeyData = keysToDelete.get(undeletedKey);
                    keys.remove(undeletedKeyData);
                }
            }
        }
    }

    protected Map<Data, Object> loadAllCacheEntry(Set<Data> keys) {
        if (cacheLoader != null) {
            Map<Object, Data> keysToLoad = new HashMap<Object, Data>();
            for (Data key : keys) {
                final Object localKeyObj = cacheService.toObject(key);
                keysToLoad.put(localKeyObj, key);
            }

            Map<Object, Object> loaded;
            try {
                loaded = cacheLoader.loadAll(keysToLoad.keySet());
            } catch (Throwable e) {
                if (!(e instanceof CacheLoaderException)) {
                    throw new CacheLoaderException("Exception in CacheLoader during loadAll", e);
                } else {
                    throw (CacheLoaderException) e;
                }
            }
            Map<Data, Object> result = new HashMap<Data, Object>();

            for (Map.Entry<Object, Data> entry : keysToLoad.entrySet()) {
                final Object keyObj = entry.getKey();
                final Object valueObject = loaded.get(keyObj);
                final Data keyData = entry.getValue();
                result.put(keyData, valueObject);
            }
            return result;
        }
        return null;
    }

    protected boolean processExpiredEntry(Data key, CacheRecord record, long now) {
        final boolean isExpired = record != null && record.isExpiredAt(now);
        if (!isExpired) {
            return false;
        }
        records.remove(key);
        if (isEventsEnabled) {
            final Data dataValue;
            switch (cacheConfig.getInMemoryFormat()) {
                case BINARY:
                    dataValue = (Data) record.getValue();
                    break;
                case OBJECT:
                    dataValue = cacheService.toData(record.getValue());
                    break;
                default:
                    throw new IllegalArgumentException("Invalid storage format: "
                            + cacheConfig.getInMemoryFormat());
            }
            publishEvent(name, CacheEventType.EXPIRED, key, null, dataValue, false);
        }
        return true;
    }

    protected boolean processExpiredEntry(Data key,
                                          CacheRecord record,
                                          long expiryTime,
                                          long now) {
        final boolean isExpired = isExpiredAt(expiryTime, now);
        if (!isExpired) {
            return false;
        }
        if (isStatisticsEnabled()) {
            statistics.increaseCacheExpiries(1);
        }
        records.remove(key);
        if (isEventsEnabled) {
            final Data dataValue;
            switch (cacheConfig.getInMemoryFormat()) {
                case BINARY:
                    dataValue = (Data) record.getValue();
                    break;
                case OBJECT:
                    dataValue = cacheService.toData(record.getValue());
                    break;
                default:
                    throw new IllegalArgumentException("Invalid storage format: "
                            + cacheConfig.getInMemoryFormat());
            }
            publishEvent(name,
                    CacheEventType.EXPIRED,
                    key,
                    null,
                    dataValue,
                    false);
        }
        return true;
    }

    public void publishCompletedEvent(String cacheName,
                                      int completionId,
                                      Data dataKey,
                                      int orderKey) {
        if (completionId > 0) {
            cacheService.publishEvent(cacheName,
                    CacheEventType.COMPLETED,
                    dataKey,
                    cacheService.toData(completionId),
                    null,
                    false,
                    orderKey);
        }
    }

    @Override
    public int forceEvict() {
        return evictExpiredRecords();
    }

    protected class EvictionTask implements Runnable {

        public void run() {
            if (hasExpiringEntry) {
                evictExpiredRecords();
            }
        }
    }

}
