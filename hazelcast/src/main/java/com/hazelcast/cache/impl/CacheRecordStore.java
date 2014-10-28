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
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.EventServiceImpl;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * <h1>On-Heap implementation of the {@link ICacheRecordStore} </h1>
 * <p>
 * Hazelcast splits data homogeneously to partitions using keys. CacheRecordStore represents a named ICache on-heap
 * data store for a single partition.<br/>
 * This data structure is responsible for CRUD operations, entry processing, statistics, publishing events, cache
 * loader and writer and internal data operations like backup.
 * </p>
 * <p>CacheRecordStore is accessed through {@linkplain com.hazelcast.cache.impl.CachePartitionSegment} and
 * {@linkplain com.hazelcast.cache.impl.CacheService}.</p>
 * CacheRecordStore is managed by {@linkplain com.hazelcast.cache.impl.CachePartitionSegment}.
 * <p>Sample code accessing a CacheRecordStore and getting a value. Typical operation implementation:
 * <pre>
 *         <code>CacheService service = getService();
 *         ICacheRecordStore cache = service.getOrCreateCache(name, partitionId);
 *         cache.get(key, expiryPolicy);
 *         </code>
 *     </pre>
 * See {@link com.hazelcast.cache.impl.operation.AbstractCacheOperation} subclasses for actual examples.
 * </p>
 *
 * @see com.hazelcast.cache.impl.CachePartitionSegment
 * @see com.hazelcast.cache.impl.CacheService
 * @see com.hazelcast.cache.impl.operation.AbstractCacheOperation
 */
public class CacheRecordStore
        extends AbstractCacheRecordStore<CacheRecord, CacheRecordMap<Data, CacheRecord>> {

    protected SerializationService serializationService;
    protected CacheRecordFactory cacheRecordFactory;
    protected boolean hasExpiringEntry;
    protected final ScheduledFuture<?> evictionTaskFuture;

    public CacheRecordStore(String name,
                            int partitionId,
                            NodeEngine nodeEngine,
                            AbstractCacheService cacheService) {
        this(name, partitionId, nodeEngine, cacheService, null);
    }

    public CacheRecordStore(String name,
                            int partitionId,
                            NodeEngine nodeEngine,
                            AbstractCacheService cacheService,
                            ExpiryPolicy expiryPolicy) {
        super(name, partitionId, nodeEngine, cacheService, expiryPolicy);
        this.records = createRecordCacheMap();
        this.serializationService = nodeEngine.getSerializationService();
        this.cacheRecordFactory = createCacheRecordFactory();
        this.evictionTaskFuture =
                nodeEngine.getExecutionService()
                        .scheduleWithFixedDelay("hz:cache",
                                new EvictionTask(),
                                INITIAL_DELAY, PERIOD,
                                TimeUnit.SECONDS);
    }

    @Override
    protected CacheRecordMap createRecordCacheMap() {
        return new CacheRecordHashMap<Data, CacheRecord>(INITIAL_MAP_CAPACITY);
    }

    protected CacheRecordFactory createCacheRecordFactory() {
        return new CacheRecordFactory(cacheConfig.getInMemoryFormat(),
                nodeEngine.getSerializationService());
    }

    @Override
    protected <T> CacheRecord createRecord(T value, long creationTime, long expiryTime) {
        return cacheRecordFactory.newRecordWithExpiry(value, expiryTime);
    }

    @Override
    protected Data toData(Object o) {
        return nodeEngine.toData(o);
    }

    @Override
    public boolean putIfAbsent(Data key,
                               Object value,
                               ExpiryPolicy expiryPolicy,
                               String caller) {
        return putIfAbsent(key, value, expiryPolicy, caller, false);
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
            statistics.addGetTimeNanos(System.nanoTime() - start);
            if (result != null) {
                statistics.increaseCacheHits(1);
                statistics.increaseCacheRemovals(1);
                statistics.addRemoveTimeNanos(System.nanoTime() - start);
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
            statistics.addGetTimeNanos(System.nanoTime() - start);
            if (result) {
                statistics.increaseCachePuts(1);
                statistics.increaseCacheHits(1);
                statistics.addPutTimeNanos(System.nanoTime() - start);
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
            Object currentValue = nodeEngine.toObject(record.getValue());
            Object oldValueObject = nodeEngine.toObject(oldValue);
            if (compare(currentValue, oldValueObject)) {
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
            statistics.addGetTimeNanos(System.nanoTime() - start);
            if (result != null) {
                statistics.increaseCacheHits(1);
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNanos(System.nanoTime() - start);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
        return result;
    }

    @Override
    public void clear() {
        records.clear();
    }

    /*
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
    */

    @Override
    public void destroy() {
        clear();
        onDestroy();
        closeResources();
        //close the configured CacheEntryListeners
        EventService eventService = nodeEngine.getEventService();
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
            statistics.addGetTimeNanos(System.nanoTime() - start);
        }
        CacheEntryProcessorEntry entry = new CacheEntryProcessorEntry(key, record, this, serializationService, now);
        final Object process = entryProcessor.process(entry, arguments);
        entry.applyChanges();
        return process;
    }

    public int evictExpiredRecords() {
        //TODO: Evict expired records and returns count of evicted records
        return 0;
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
