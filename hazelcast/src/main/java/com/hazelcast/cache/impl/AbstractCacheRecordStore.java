/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.eviction.EvictionListener;
import com.hazelcast.internal.eviction.EvictionPolicyEvaluator;
import com.hazelcast.internal.eviction.EvictionPolicyEvaluatorProvider;
import com.hazelcast.internal.eviction.EvictionStrategy;
import com.hazelcast.internal.eviction.EvictionStrategyProvider;
import com.hazelcast.cache.impl.maxsize.MaxSizeChecker;
import com.hazelcast.cache.impl.maxsize.impl.EntryCountCacheMaxSizeChecker;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.cache.impl.record.SampleableCacheRecordMap;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.eventservice.InternalEventService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.ExceptionUtil;

import javax.cache.configuration.Factory;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.expiry.ModifiedExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.CacheEventContextUtil.createBaseEventContext;
import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheCompleteEvent;
import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheCreatedEvent;
import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheExpiredEvent;
import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheRemovedEvent;
import static com.hazelcast.cache.impl.CacheEventContextUtil.createCacheUpdatedEvent;
import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;
import static com.hazelcast.cache.impl.record.CacheRecordFactory.isExpiredAt;

/**
 * @author sozal 14/10/14
 */
public abstract class AbstractCacheRecordStore<R extends CacheRecord, CRM extends SampleableCacheRecordMap<Data, R>>
        implements ICacheRecordStore, EvictionListener<Data, R> {

    protected static final int DEFAULT_INITIAL_CAPACITY = 256;
    protected static final String SOURCE_NOT_AVAILABLE = "<NA>";

    protected final String name;
    protected final int partitionId;
    protected final int partitionCount;
    protected final NodeEngine nodeEngine;
    protected final InternalPartitionService partitionService;
    protected final AbstractCacheService cacheService;
    protected final CacheConfig cacheConfig;
    protected CRM records;
    protected CacheContext cacheContext;
    protected CacheStatisticsImpl statistics;
    protected CacheLoader cacheLoader;
    protected CacheWriter cacheWriter;
    protected boolean eventsEnabled = true;
    protected boolean eventsBatchingEnabled;
    protected ExpiryPolicy defaultExpiryPolicy;
    protected final EvictionConfig evictionConfig;
    protected final Map<CacheEventType, Set<CacheEventData>> batchEvent = new HashMap<CacheEventType, Set<CacheEventData>>();
    protected final MaxSizeChecker maxSizeChecker;
    protected final EvictionPolicyEvaluator<Data, R> evictionPolicyEvaluator;
    protected final EvictionChecker evictionChecker;
    protected final EvictionStrategy<Data, R, CRM> evictionStrategy;
    protected final boolean wanReplicationEnabled;

    //CHECKSTYLE:OFF
    public AbstractCacheRecordStore(final String name, final int partitionId, final NodeEngine nodeEngine,
                                    final AbstractCacheService cacheService) {
        this.name = name;
        this.partitionId = partitionId;
        this.nodeEngine = nodeEngine;
        this.partitionService = nodeEngine.getPartitionService();
        this.partitionCount = partitionService.getPartitionCount();
        this.cacheService = cacheService;
        this.cacheConfig = cacheService.getCacheConfig(name);
        this.cacheContext = cacheService.getOrCreateCacheContext(name);
        if (cacheConfig == null) {
            throw new CacheNotExistsException("Cache is already destroyed or not created yet, on "
                    + nodeEngine.getLocalMember());
        }
        this.wanReplicationEnabled = cacheConfig.getWanReplicationRef() != null;
        this.evictionConfig = cacheConfig.getEvictionConfig();
        if (evictionConfig == null) {
            throw new IllegalStateException("Eviction config cannot be null");
        }
        this.records = createRecordCacheMap();
        if (cacheConfig.getCacheLoaderFactory() != null) {
            final Factory<CacheLoader> cacheLoaderFactory = cacheConfig.getCacheLoaderFactory();
            cacheLoader = cacheLoaderFactory.create();
        }
        if (cacheConfig.getCacheWriterFactory() != null) {
            final Factory<CacheWriter> cacheWriterFactory = cacheConfig.getCacheWriterFactory();
            cacheWriter = cacheWriterFactory.create();
        }
        if (cacheConfig.isStatisticsEnabled()) {
            this.statistics = cacheService.createCacheStatIfAbsent(name);
        }
        final Factory<ExpiryPolicy> expiryPolicyFactory = cacheConfig.getExpiryPolicyFactory();
        this.defaultExpiryPolicy = expiryPolicyFactory.create();
        this.maxSizeChecker = createCacheMaxSizeChecker(evictionConfig.getSize(), evictionConfig.getMaximumSizePolicy());
        this.evictionPolicyEvaluator = createEvictionPolicyEvaluator(evictionConfig);
        this.evictionChecker = createEvictionChecker(evictionConfig);
        this.evictionStrategy = createEvictionStrategy(evictionConfig);

        // Register "cacheWriter" if it is "Closable" to be closed while cache is being destroyed
        if (cacheWriter instanceof Closeable) {
            cacheService.addCacheResource(name, (Closeable) cacheWriter);
        }
        // Register "cacheLoader" if it is "Closable" to be closed while cache is being destroyed
        if (cacheLoader instanceof Closeable) {
            cacheService.addCacheResource(name, (Closeable) cacheLoader);
        }
        // Register "defaultExpiryPolicy" if it is "Closable" to be closed while cache is being destroyed
        if (defaultExpiryPolicy instanceof Closeable) {
            cacheService.addCacheResource(name, (Closeable) defaultExpiryPolicy);
        }
    }
    //CHECKSTYLE:ON

    protected boolean isReadThrough() {
        return cacheConfig.isReadThrough();
    }

    protected boolean isWriteThrough() {
        return cacheConfig.isWriteThrough();
    }

    protected boolean isStatisticsEnabled() {
        return statistics != null;
    }

    protected abstract CRM createRecordCacheMap();
    protected abstract CacheEntryProcessorEntry createCacheEntryProcessorEntry(Data key, R record,
                                                                               long now, int completionId);
    protected abstract R createRecord(Object value, long creationTime, long expiryTime);
    protected abstract Data valueToData(Object value);
    protected abstract Object dataToValue(Data data);
    protected abstract Object recordToValue(R record);
    protected abstract Data recordToData(R record);
    protected abstract Data toHeapData(Object obj);

    protected MaxSizeChecker createCacheMaxSizeChecker(int size, EvictionConfig.MaxSizePolicy maxSizePolicy) {
        if (maxSizePolicy == null) {
            throw new IllegalArgumentException("Max-Size policy cannot be null");
        }
        if (maxSizePolicy == EvictionConfig.MaxSizePolicy.ENTRY_COUNT) {
            return new EntryCountCacheMaxSizeChecker(size, records, partitionCount);
        }
        return null;
    }

    protected EvictionPolicyEvaluator<Data, R> createEvictionPolicyEvaluator(EvictionConfig cacheEvictionConfig) {
        final EvictionPolicy evictionPolicy = cacheEvictionConfig.getEvictionPolicy();
        if (evictionPolicy == null || evictionPolicy == EvictionPolicy.NONE) {
            throw new IllegalArgumentException("Eviction policy cannot be null or NONE");
        }
        return EvictionPolicyEvaluatorProvider.getEvictionPolicyEvaluator(cacheEvictionConfig);
    }

    protected EvictionChecker createEvictionChecker(EvictionConfig cacheEvictionConfig) {
        return new MaxSizeEvictionChecker();
    }

    protected EvictionStrategy<Data, R, CRM> createEvictionStrategy(EvictionConfig cacheEvictionConfig) {
        return EvictionStrategyProvider.getEvictionStrategy(cacheEvictionConfig);
    }

    protected boolean isEvictionEnabled() {
        return evictionStrategy != null && evictionPolicyEvaluator != null;
    }

    protected boolean isEventsEnabled() {
        return eventsEnabled && (cacheContext.getCacheEntryListenerCount() > 0 || wanReplicationEnabled);
    }

    protected boolean isInvalidationEnabled() {
        return cacheContext.getInvalidationListenerCount() > 0;
    }

    @Override
    public int evictIfRequired() {
        int evictedCount = 0;
        if (isEvictionEnabled()) {
            evictedCount = evictionStrategy.evict(records, evictionPolicyEvaluator, evictionChecker, this);
            if (isStatisticsEnabled() && evictedCount > 0) {
                statistics.increaseCacheEvictions(evictedCount);
            }
        }
        return evictedCount;
    }

    protected Data toData(Object obj) {
        if (obj instanceof Data) {
            return (Data) obj;
        } else if (obj instanceof CacheRecord) {
            return recordToData((R) obj);
        } else {
            return valueToData(obj);
        }
    }

    protected Object toValue(Object obj) {
        if (obj instanceof Data) {
            return dataToValue((Data) obj);
        } else if (obj instanceof CacheRecord) {
            return recordToValue((R) obj);
        } else {
            return obj;
        }
    }

    protected Object toStorageValue(Object obj) {
        if (obj instanceof Data) {
            if (cacheConfig.getInMemoryFormat() == InMemoryFormat.OBJECT) {
                return dataToValue((Data) obj);
            } else {
                return obj;
            }
        } else if (obj instanceof CacheRecord) {
            return recordToValue((R) obj);
        } else {
            return obj;
        }
    }

    protected Data toEventData(Object obj) {
        return isEventsEnabled() ? toHeapData(obj) : null;
    }

    protected ExpiryPolicy getExpiryPolicy(ExpiryPolicy expiryPolicy) {
        return expiryPolicy != null ? expiryPolicy : defaultExpiryPolicy;
    }

    protected boolean processExpiredEntry(Data key, R record, long now) {
        return processExpiredEntry(key, record, now, SOURCE_NOT_AVAILABLE);
    }

    protected boolean processExpiredEntry(Data key, R record, long now, String source) {
        return processExpiredEntry(key, record, now, source, null);
    }

    protected boolean processExpiredEntry(Data key, R record, long now, String source, String origin) {
        final boolean isExpired = record != null && record.isExpiredAt(now);
        if (!isExpired) {
            return false;
        }
        if (isStatisticsEnabled()) {
            statistics.increaseCacheExpiries(1);
        }
        R removedRecord = doRemoveRecord(key, source);
        Data keyEventData = toEventData(key);
        Data recordEventData = toEventData(removedRecord);
        onProcessExpiredEntry(key, removedRecord, removedRecord.getExpirationTime(), now, source, origin);
        if (isEventsEnabled()) {
            publishEvent(createCacheExpiredEvent(keyEventData, recordEventData,
                                                 CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE,
                                                 origin, IGNORE_COMPLETION));
        }
        return true;
    }

    protected R processExpiredEntry(Data key, R record, long expiryTime, long now) {
        return processExpiredEntry(key, record, expiryTime, now, SOURCE_NOT_AVAILABLE);
    }

    protected R processExpiredEntry(Data key, R record, long expiryTime, long now, String source) {
        return processExpiredEntry(key, record, expiryTime, now, source,  null);
    }

    protected R processExpiredEntry(Data key, R record, long expiryTime, long now, String source, String origin) {
        final boolean isExpired = isExpiredAt(expiryTime, now);
        if (!isExpired) {
            return record;
        }
        if (isStatisticsEnabled()) {
            statistics.increaseCacheExpiries(1);
        }
        R removedRecord = doRemoveRecord(key, source);
        Data keyEventData = toEventData(key);
        Data recordEventData = toEventData(removedRecord);
        onProcessExpiredEntry(key, removedRecord, expiryTime, now, source, origin);
        if (isEventsEnabled()) {
            publishEvent(createCacheExpiredEvent(keyEventData, recordEventData,
                                                 CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE,
                                                 origin, IGNORE_COMPLETION));
        }
        return null;
    }

    protected void onProcessExpiredEntry(Data key, R record, long expiryTime, long now, String source, String origin) {
    }

    public R accessRecord(Data key, R record, ExpiryPolicy expiryPolicy, long now) {
        onRecordAccess(key, record, getExpiryPolicy(expiryPolicy), now);
        return record;
    }

    @Override
    public void onEvict(Data key, R record) {
        invalidateEntry(key);
    }

    protected void invalidateEntry(Data key, String source) {
        if (isInvalidationEnabled()) {
            cacheService.sendInvalidationEvent(name, toHeapData(key), source);
        }
    }

    protected void invalidateEntry(Data key) {
        invalidateEntry(key, SOURCE_NOT_AVAILABLE);
    }

    protected void invalidateAllEntries() {
        invalidateAllEntries(SOURCE_NOT_AVAILABLE);
    }

    protected void invalidateAllEntries(String source) {
        invalidateEntry(null, source);
    }

    protected void updateGetAndPutStat(boolean isPutSucceed, boolean getValue, boolean oldValueNull, long start) {
        if (isStatisticsEnabled()) {
            if (isPutSucceed) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNanos(System.nanoTime() - start);
            }
            if (getValue) {
                if (oldValueNull) {
                    statistics.increaseCacheMisses(1);
                } else {
                    statistics.increaseCacheHits(1);
                }
                statistics.addGetTimeNanos(System.nanoTime() - start);
            }
        }
    }

    protected long updateAccessDuration(Data key, R record, ExpiryPolicy expiryPolicy, long now) {
        long expiryTime = -1L;
        try {
            Duration expiryDuration = expiryPolicy.getExpiryForAccess();
            if (expiryDuration != null) {
                expiryTime = expiryDuration.getAdjustedTime(now);
                record.setExpirationTime(expiryTime);
                if (isEventsEnabled()) {
                    CacheEventContext cacheEventContext =
                            createBaseEventContext(CacheEventType.EXPIRATION_TIME_UPDATED, toEventData(key),
                                                   toEventData(record.getValue()), expiryTime, null, IGNORE_COMPLETION);
                    cacheEventContext.setAccessHit(record.getAccessHit());
                    publishEvent(cacheEventContext);
                }
            }
        } catch (Exception e) {
            EmptyStatement.ignore(e);
        }
        return expiryTime;
    }

    protected long onRecordAccess(Data key, R record, ExpiryPolicy expiryPolicy, long now) {
        record.setAccessTime(now);
        record.incrementAccessHit();
        return updateAccessDuration(key, record, expiryPolicy, now);
    }

    protected void updateReplaceStat(boolean result, boolean isHit, long start) {
        if (isStatisticsEnabled()) {
            if (result) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNanos(System.nanoTime() - start);
            }
            statistics.addGetTimeNanos(System.nanoTime() - start);
            if (isHit) {
                statistics.increaseCacheHits(1);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
    }

    protected void publishEvent(CacheEventContext cacheEventContext) {
        if (isEventsEnabled()) {
            cacheEventContext.setCacheName(name);
            if (eventsBatchingEnabled) {
                final CacheEventDataImpl cacheEventData =
                        new CacheEventDataImpl(name, cacheEventContext.getEventType(), cacheEventContext.getDataKey(),
                                               cacheEventContext.getDataValue(), cacheEventContext.getDataOldValue(),
                                               cacheEventContext.isOldValueAvailable());
                Set<CacheEventData> cacheEventDataSet = batchEvent.get(cacheEventContext.getEventType());
                if (cacheEventDataSet == null) {
                    cacheEventDataSet = new HashSet<CacheEventData>();
                    batchEvent.put(cacheEventContext.getEventType(), cacheEventDataSet);
                }
                cacheEventDataSet.add(cacheEventData);
            } else {
                cacheService.publishEvent(cacheEventContext);
            }
        }
    }

    protected void publishBatchedEvents(String cacheName, CacheEventType cacheEventType, int orderKey) {
        if (isEventsEnabled()) {
            final Set<CacheEventData> cacheEventDatas = batchEvent.get(cacheEventType);
            CacheEventSet ces = new CacheEventSet(cacheEventType, cacheEventDatas);
            cacheService.publishEvent(cacheName, ces, orderKey);
        }
    }

    protected boolean compare(Object v1, Object v2) {
        if (v1 == null && v2 == null) {
            return true;
        }
        if (v1 == null) {
            return false;
        }
        if (v2 == null) {
            return false;
        }
        return v1.equals(v2);
    }

    protected long expiryPolicyToTTL(ExpiryPolicy expiryPolicy) {
        if (expiryPolicy == null) {
            return CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE;
        }
        try {
            Duration expiryDuration = expiryPolicy.getExpiryForCreation();
            if (expiryDuration == null || expiryDuration.isEternal()) {
                return CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE;
            }
            long durationAmount = expiryDuration.getDurationAmount();
            TimeUnit durationTimeUnit = expiryDuration.getTimeUnit();
            return TimeUnit.MILLISECONDS.convert(durationAmount, durationTimeUnit);
        } catch (Exception e) {
            return CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE;
        }
    }

    protected ExpiryPolicy ttlToExpirePolicy(long ttl) {
        if (ttl >= 0) {
            return new ModifiedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, ttl));
        } else {
            return new CreatedExpiryPolicy(Duration.ETERNAL);
        }
    }

    protected R createRecord(long expiryTime) {
        return createRecord(null, Clock.currentTimeMillis(), expiryTime);
    }

    protected R createRecord(Object value, long expiryTime) {
        return createRecord(value, Clock.currentTimeMillis(), expiryTime);
    }

    protected R createRecord(Data keyData, Object value, long expirationTime, int completionId) {
        return createRecord(keyData, value, expirationTime, completionId, null);
    }

    protected R createRecord(Data keyData, Object value, long expirationTime, int completionId, String origin) {
        final R record = createRecord(value, expirationTime);
        if (isEventsEnabled()) {
            publishEvent(createCacheCreatedEvent(toEventData(keyData), toEventData(value),
                                                 expirationTime, origin, completionId));
        }
        return record;
    }

    protected R createRecordWithExpiry(Data key, Object value, long expiryTime,
                                       long now, boolean disableWriteThrough, int completionId) {
        return createRecordWithExpiry(key, value, expiryTime, now, disableWriteThrough, completionId, null);
    }

    protected R createRecordWithExpiry(Data key, Object value, long expiryTime,
                                       long now, boolean disableWriteThrough, int completionId, String origin) {
        if (!disableWriteThrough) {
            writeThroughCache(key, value);
        }
        if (!isExpiredAt(expiryTime, now)) {
            R record = createRecord(key, value, expiryTime, completionId, origin);
            try {
                doPutRecord(key, record);
            } catch (Throwable error) {
                onCreateRecordWithExpiryError(key, value, expiryTime, now, disableWriteThrough,
                                              completionId, origin, record, error);
                throw ExceptionUtil.rethrow(error);
            }
            return record;
        }
        if (isEventsEnabled()) {
            publishEvent(createCacheCompleteEvent(toEventData(key), CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE,
                                                  origin, completionId));
        }
        return null;
    }

    protected R createRecordWithExpiry(Data key, Object value, ExpiryPolicy expiryPolicy,
                                       long now, boolean disableWriteThrough, int completionId) {
        return createRecordWithExpiry(key, value, expiryPolicy, now, disableWriteThrough, completionId, null);
    }

    protected R createRecordWithExpiry(Data key, Object value, ExpiryPolicy expiryPolicy,
                                       long now, boolean disableWriteThrough, int completionId, String origin) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);
        Duration expiryDuration;
        try {
            expiryDuration = expiryPolicy.getExpiryForCreation();
        } catch (Exception e) {
            expiryDuration = Duration.ETERNAL;
        }
        long expiryTime = expiryDuration.getAdjustedTime(now);
        return createRecordWithExpiry(key, value, expiryTime, now, disableWriteThrough, completionId, origin);
    }

    protected void onCreateRecordWithExpiryError(Data key, Object value, long expiryTime, long now,
                                                 boolean disableWriteThrough, int completionId, String origin,
                                                 R record, Throwable error) {
    }

    protected void onUpdateRecord(Data key, R record, Object value, Data oldDataValue) {
    }

    protected void onUpdateRecordError(Data key, R record, Object value, Data newDataValue,
                                       Data oldDataValue, Throwable error) {
    }

    protected R updateRecord(Data key, R record, Object value, int completionId) {
        return updateRecord(key, record, value, completionId, SOURCE_NOT_AVAILABLE);
    }

    protected R updateRecord(Data key, R record, Object value, int completionId, String source) {
        return updateRecord(key, record, value, completionId, source, null);
    }

    protected R updateRecord(Data key, R record, Object value, int completionId, String source, String origin) {
        Data dataOldValue = null;
        Data dataValue = null;
        Object recordValue = value;
        try {
            switch (cacheConfig.getInMemoryFormat()) {
                case BINARY:
                    recordValue = toData(value);
                    dataValue = (Data) recordValue;
                    dataOldValue = toData(record);
                    break;
                case OBJECT:
                    if (value instanceof Data) {
                        recordValue = dataToValue((Data) value);
                        dataValue = (Data) value;
                    } else {
                        dataValue = valueToData(value);
                    }
                    dataOldValue = toData(record);
                    break;
                case NATIVE:
                    recordValue = toData(value);
                    dataValue = (Data) recordValue;
                    dataOldValue = toData(record);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid storage format: " + cacheConfig.getInMemoryFormat());
            }

            Data eventDataKey = toEventData(key);
            Data eventDataValue = toEventData(dataValue);
            Data eventDataOldValue = toEventData(dataOldValue);

            record.setValue(recordValue);
            onUpdateRecord(key, record, value, dataOldValue);
            invalidateEntry(key, source);

            if (isEventsEnabled()) {
                publishEvent(createCacheUpdatedEvent(eventDataKey, eventDataValue, eventDataOldValue,
                                                     record.getExpirationTime(),
                                                     record.getAccessTime(),
                                                     record.getAccessHit(),
                                                     origin, completionId));
            }
            return record;
        } catch (Throwable error) {
            onUpdateRecordError(key, record, value, dataValue, dataOldValue, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    protected boolean updateRecordWithExpiry(Data key, Object value, R record, long expiryTime,
                                             long now, boolean disableWriteThrough, int completionId) {
        return updateRecordWithExpiry(key, value, record, expiryTime, now, disableWriteThrough,
                                      completionId, SOURCE_NOT_AVAILABLE);
    }

    protected boolean updateRecordWithExpiry(Data key, Object value, R record, long expiryTime,
                                             long now, boolean disableWriteThrough, int completionId, String source) {
        return updateRecordWithExpiry(key, value, record, expiryTime, now,
                                      disableWriteThrough, completionId, source, null);
    }

    protected boolean updateRecordWithExpiry(Data key, Object value, R record, long expiryTime, long now,
                                             boolean disableWriteThrough, int completionId, String source, String origin) {
        record.setExpirationTime(expiryTime);
        if (!disableWriteThrough) {
            writeThroughCache(key, value);
        }
        if (!isExpiredAt(expiryTime, now)) {
            // No need to update record value if it is expired
            updateRecord(key, record, value, completionId, source, origin);
        }
        return processExpiredEntry(key, record, expiryTime, now, source) != null;
    }

    protected boolean updateRecordWithExpiry(Data key, Object value, R record, ExpiryPolicy expiryPolicy,
                                             long now, boolean disableWriteThrough, int completionId) {
        return updateRecordWithExpiry(key, value, record, expiryPolicy, now,
                                      disableWriteThrough, completionId, SOURCE_NOT_AVAILABLE);
    }

    protected boolean updateRecordWithExpiry(Data key, Object value, R record, ExpiryPolicy expiryPolicy,
                                             long now, boolean disableWriteThrough, int completionId, String source) {
        return updateRecordWithExpiry(key, value, record, expiryPolicy, now,
                                      disableWriteThrough, completionId, source, null);
    }

    protected boolean updateRecordWithExpiry(Data key, Object value, R record, ExpiryPolicy expiryPolicy, long now,
                                             boolean disableWriteThrough, int completionId, String source, String origin) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);
        long expiryTime = CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE;
        try {
            Duration expiryDuration = expiryPolicy.getExpiryForUpdate();
            if (expiryDuration != null) {
                expiryTime = expiryDuration.getAdjustedTime(now);
            }
        } catch (Exception e) {
            EmptyStatement.ignore(e);
        }
        return updateRecordWithExpiry(key, value, record, expiryTime, now,
                                      disableWriteThrough, completionId, source, origin);
    }

    protected void onDeleteRecord(Data key, R record, Data dataValue, boolean deleted) {
    }

    protected void onDeleteRecordError(Data key, R record, Data dataValue, boolean deleted, Throwable error) {
    }

    protected boolean deleteRecord(Data key, int completionId) {
        return deleteRecord(key, completionId, SOURCE_NOT_AVAILABLE);
    }

    protected boolean deleteRecord(Data key, int completionId, String source) {
        return deleteRecord(key, completionId, source, null);
    }

    protected boolean deleteRecord(Data key, int completionId, String source, String origin) {
        final R record = doRemoveRecord(key, source);
        Data dataValue = null;
        try {
            switch (cacheConfig.getInMemoryFormat()) {
                case BINARY:
                case OBJECT:
                case NATIVE:
                    dataValue = toData(record);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid storage format: " + cacheConfig.getInMemoryFormat());
            }
            Data eventDataKey = toEventData(key);
            Data eventDataValue = toEventData(dataValue);
            onDeleteRecord(key, record, dataValue, record != null);
            if (isEventsEnabled()) {
                publishEvent(createCacheRemovedEvent(eventDataKey, eventDataValue,
                                                     CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE, origin, completionId));
            }
            return record != null;
        } catch (Throwable error) {
            onDeleteRecordError(key, record, dataValue, record != null, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    public R readThroughRecord(Data key, long now) {
        Object value = readThroughCache(key);
        if (value == null) {
            return null;
        }
        Duration expiryDuration;
        try {
            expiryDuration = defaultExpiryPolicy.getExpiryForCreation();
        } catch (Exception e) {
            expiryDuration = Duration.ETERNAL;
        }
        long expiryTime = expiryDuration.getAdjustedTime(now);
        if (isExpiredAt(expiryTime, now)) {
            return null;
        }
        return createRecord(key, value, expiryTime, IGNORE_COMPLETION);
    }

    public Object readThroughCache(Data key) throws CacheLoaderException {
        if (this.isReadThrough() && cacheLoader != null) {
            try {
                Object o = dataToValue(key);
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

    public void writeThroughCache(Data key, Object value) throws CacheWriterException {
        if (isWriteThrough() && cacheWriter != null) {
            try {
                final Object objKey = dataToValue(key);
                final Object objValue = toValue(value);
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
                final Object objKey = dataToValue(key);
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

    protected void deleteAllCacheEntry(Set<Data> keys) {
        if (isWriteThrough() && cacheWriter != null && keys != null && !keys.isEmpty()) {
            Map<Object, Data> keysToDelete = new HashMap<Object, Data>();
            for (Data key : keys) {
                final Object localKeyObj = dataToValue(key);
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
                final Object localKeyObj = dataToValue(key);
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

    @Override
    public CacheRecord getRecord(Data key) {
        return records.get(key);
    }

    @Override
    public void setRecord(Data key, CacheRecord record) {
        doPutRecord(key, (R) record);
    }

    @Override
    public void putRecord(Data key, CacheRecord record) {
        evictIfRequired();

        doPutRecord(key, (R) record);
    }

    protected final R doPutRecord(Data key, R record) {
        return doPutRecord(key, record, SOURCE_NOT_AVAILABLE);
    }

    protected final R doPutRecord(Data key, R record, String source) {
        R oldRecord = records.put(key, record);
        if (oldRecord != null) {
            invalidateEntry(key, source);
        }
        return oldRecord;
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        return doRemoveRecord(key);
    }

    protected R doRemoveRecord(Data key) {
        return doRemoveRecord(key, SOURCE_NOT_AVAILABLE);
    }

    protected R doRemoveRecord(Data key, String source) {
        R removedRecord = records.remove(key);
        if (removedRecord != null) {
            invalidateEntry(key, source);
        }
        return removedRecord;
    }

    protected void onGet(Data key, ExpiryPolicy expiryPolicy, Object value, R record) {
    }

    protected void onGetError(Data key, ExpiryPolicy expiryPolicy, Object value, R record, Throwable error) {
    }

    @Override
    public Object get(Data key, ExpiryPolicy expiryPolicy) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);
        long now = Clock.currentTimeMillis();
        Object value = null;
        R record = records.get(key);
        final boolean isExpired = processExpiredEntry(key, record, now);

        try {
            if (record == null || isExpired) {
                if (isStatisticsEnabled()) {
                    statistics.increaseCacheMisses(1);
                }
                value = readThroughCache(key);
                if (value == null) {
                    return null;
                }
                record = createRecordWithExpiry(key, value, expiryPolicy, now, true, IGNORE_COMPLETION);
            } else {
                value = recordToValue(record);
                onRecordAccess(key, record, expiryPolicy, now);
                if (isStatisticsEnabled()) {
                    statistics.increaseCacheHits(1);
                }
            }
            onGet(key, expiryPolicy, value, record);
            return value;
        } catch (Throwable error) {
            onGetError(key, expiryPolicy, value, record, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public boolean contains(Data key) {
        long now = Clock.currentTimeMillis();
        R record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);
        return record != null && !isExpired;
    }

    protected void onPut(Data key, Object value, ExpiryPolicy expiryPolicy, String source,
                         boolean getValue, boolean disableWriteThrough, R record, Object oldValue,
                         boolean isExpired, boolean isNewPut, boolean isSaveSucceed) {
    }

    protected void onPutError(Data key, Object value, ExpiryPolicy expiryPolicy, String source,
                              boolean getValue, boolean disableWriteThrough, R record,
                              Object oldValue, boolean wouldBeNewPut, Throwable error) {
    }

    protected Object put(Data key, Object value, ExpiryPolicy expiryPolicy, String source,
                         boolean getValue, boolean disableWriteThrough, int completionId) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        boolean isOnNewPut = false;
        boolean isSaveSucceed;
        Object oldValue = null;
        R record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now, source);

        try {
            // Check that new entry is not already expired, in which case it should
            // not be added to the cache or listeners called or writers called.
            if (record == null || isExpired) {
                isOnNewPut = true;
                record = createRecordWithExpiry(key, value, expiryPolicy, now,
                                                disableWriteThrough, completionId);
                isSaveSucceed = record != null;
            } else {
                if (getValue) {
                    oldValue = toValue(record);
                }
                isSaveSucceed = updateRecordWithExpiry(key, value, record, expiryPolicy,
                                                       now, disableWriteThrough, completionId, source);
            }
            onPut(key, value, expiryPolicy, source, getValue, disableWriteThrough,
                  record, oldValue, isExpired, isOnNewPut, isSaveSucceed);
            updateGetAndPutStat(isSaveSucceed, getValue, oldValue == null, start);
            if (getValue) {
                return oldValue;
            } else {
                return record;
            }
        } catch (Throwable error) {
            onPutError(key, value, expiryPolicy, source, getValue, disableWriteThrough,
                       record, oldValue, isOnNewPut, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    protected Object put(Data key, Object value, ExpiryPolicy expiryPolicy, String source,
                         boolean getValue, int completionId) {
        return put(key, value, expiryPolicy, source, getValue, false, completionId);
    }

    @Override
    public R put(Data key, Object value, ExpiryPolicy expiryPolicy, String source, int completionId) {
        return (R) put(key, value, expiryPolicy, source, false, false, completionId);
    }

    @Override
    public Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy,
                            String source, int completionId) {
        return put(key, value, expiryPolicy, source, true, false, completionId);
    }

    protected void onPutIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, String source,
                                 boolean disableWriteThrough, R record, boolean isExpired, boolean isSaveSucceed) {
    }

    protected void onPutIfAbsentError(Data key, Object value, ExpiryPolicy expiryPolicy,
                                      String source, boolean disableWriteThrough, R record, Throwable error) {
    }

    protected boolean putIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, String source,
                                  boolean disableWriteThrough, int completionId) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        boolean saved = false;
        R record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now, source);

        try {
            if (record == null || isExpired) {
                saved = createRecordWithExpiry(key, value, expiryPolicy, now,
                                               disableWriteThrough, completionId) != null;
            } else {
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), completionId));
                }
            }
            onPutIfAbsent(key, value, expiryPolicy, source, disableWriteThrough,
                          record, isExpired, saved);
            if (saved && isStatisticsEnabled()) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNanos(System.nanoTime() - start);
            }
            return saved;
        } catch (Throwable error) {
            onPutIfAbsentError(key, value, expiryPolicy, source, disableWriteThrough, record, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public boolean putIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy,
                               String source, int completionId) {
        return putIfAbsent(key, value, expiryPolicy, source, false, completionId);
    }

    protected void onReplace(Data key, Object oldValue, Object newValue, ExpiryPolicy expiryPolicy,
                             String source, boolean getValue, R record, boolean isExpired, boolean replaced) {
    }

    protected void onReplaceError(Data key, Object oldValue, Object newValue, ExpiryPolicy expiryPolicy, String source,
                                  boolean getValue, R record, boolean isExpired, boolean replaced, Throwable error) {
    }

    @Override
    public boolean replace(Data key, Object value, ExpiryPolicy expiryPolicy, String source, int completionId) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        boolean replaced = false;
        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);

        try {
            if (record == null || isExpired) {
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), completionId));
                }
            } else {
                replaced = updateRecordWithExpiry(key, value, record, expiryPolicy,
                                                  now, false, completionId, source);
            }
            onReplace(key, null, value, expiryPolicy, source, false, record, isExpired, replaced);
            if (isStatisticsEnabled()) {
                statistics.addGetTimeNanos(System.nanoTime() - start);
                if (replaced) {
                    statistics.increaseCachePuts(1);
                    statistics.increaseCacheHits(1);
                    statistics.addPutTimeNanos(System.nanoTime() - start);
                } else {
                    statistics.increaseCacheMisses(1);
                }
            }
            return replaced;
        } catch (Throwable error) {
            onReplaceError(key, null, value, expiryPolicy, source, false, record, isExpired, replaced, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public boolean replace(Data key, Object oldValue, Object newValue, ExpiryPolicy expiryPolicy,
                           String source, int completionId) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        boolean isHit = false;
        boolean replaced = false;
        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);

        try {
            if (record != null && !isExpired) {
                isHit = true;
                Object currentValue = toStorageValue(record);
                if (compare(currentValue, toStorageValue(oldValue))) {
                    replaced = updateRecordWithExpiry(key, newValue, record, expiryPolicy,
                                                      now, false, completionId, source);
                } else {
                    onRecordAccess(key, record, expiryPolicy, now);
                }
            }
            if (!replaced) {
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), completionId));
                }
            }
            onReplace(key, oldValue, newValue, expiryPolicy, source, false, record, isExpired, replaced);
            updateReplaceStat(replaced, isHit, start);
            return replaced;
        } catch (Throwable error) {
            onReplaceError(key, oldValue, newValue, expiryPolicy, source, false,
                           record, isExpired, replaced, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public Object getAndReplace(Data key, Object value, ExpiryPolicy expiryPolicy, String source, int completionId) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        Object obj = null;
        boolean replaced = false;
        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);

        try {
            obj = toValue(record);
            if (record == null || isExpired) {
                obj = null;
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), completionId));
                }
            } else {
                replaced = updateRecordWithExpiry(key, value, record, expiryPolicy,
                                                  now, false, completionId, source);
            }
            onReplace(key, null, value, expiryPolicy, source, false, record, isExpired, replaced);
            if (isStatisticsEnabled()) {
                statistics.addGetTimeNanos(System.nanoTime() - start);
                if (obj != null) {
                    statistics.increaseCacheHits(1);
                    statistics.increaseCachePuts(1);
                    statistics.addPutTimeNanos(System.nanoTime() - start);
                } else {
                    statistics.increaseCacheMisses(1);
                }
            }
            return obj;
        } catch (Throwable error) {
            onReplaceError(key, null, value, expiryPolicy, source, false, record, isExpired, replaced, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    protected void onRemove(Data key, Object value, String source, boolean getValue, R record, boolean removed) {
    }

    protected void onRemoveError(Data key, Object value, String source, boolean getValue,
                                 R record, boolean removed, Throwable error) {
    }

    @Override
    public boolean remove(Data key, String source, int completionId) {
        return remove(key, source, completionId, null);
    }

    public boolean remove(Data key, String source, int completionId, String origin) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        deleteCacheEntry(key);

        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        boolean removed = false;

        try {
            if (record == null || isExpired) {
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE,
                                                          origin, completionId));
                }
            } else {
                removed = deleteRecord(key, completionId, source, origin);
            }
            onRemove(key, null, source, false, record, removed);
            if (removed && isStatisticsEnabled()) {
                statistics.increaseCacheRemovals(1);
                statistics.addRemoveTimeNanos(System.nanoTime() - start);
            }
            return removed;
        } catch (Throwable error) {
            onRemoveError(key, null, source, false, record, removed, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public boolean remove(Data key, Object value, String source, int completionId) {
        return remove(key, value, source, completionId, null);
    }

    public boolean remove(Data key, Object value, String source, int completionId, String origin) {
        final long now = Clock.currentTimeMillis();
        final long start = System.nanoTime();
        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        int hitCount = 0;
        boolean removed = false;

        try {
            if (record == null || isExpired) {
                if (isStatisticsEnabled()) {
                    statistics.increaseCacheMisses(1);
                }
            } else {
                hitCount++;
                if (compare(toStorageValue(record), toStorageValue(value))) {
                    deleteCacheEntry(key);
                    removed = deleteRecord(key, completionId, source, origin);
                } else {
                    long expiryTime = onRecordAccess(key, record, defaultExpiryPolicy, now);
                    processExpiredEntry(key, record, expiryTime, now, source, origin);
                }
            }
            if (!removed) {
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE,
                                                          origin, completionId));
                }
            }
            onRemove(key, value, source, false, record, removed);
            updateRemoveStatistics(removed, hitCount, start);
            return removed;
        } catch (Throwable error) {
            onRemoveError(key, null, source, false, record, removed, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    private void updateRemoveStatistics(boolean result, int hitCount, long start) {
        if (result && isStatisticsEnabled()) {
            statistics.increaseCacheRemovals(1);
            statistics.addRemoveTimeNanos(System.nanoTime() - start);
            if (hitCount == 1) {
                statistics.increaseCacheHits(hitCount);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
    }

    @Override
    public Object getAndRemove(Data key, String source, int completionId) {
        return getAndRemove(key, source, completionId, null);
    }

    public Object getAndRemove(Data key, String source, int completionId, String origin) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        deleteCacheEntry(key);
        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        final Object obj;
        boolean removed = false;

        try {
            if (record == null || isExpired) {
                obj = null;
                if (isEventsEnabled()) {
                    publishEvent(createCacheCompleteEvent(toEventData(key), CacheRecord.EXPIRATION_TIME_NOT_AVAILABLE,
                                                          origin, completionId));
                }
            } else {
                obj = toValue(record);
                removed = deleteRecord(key, completionId, source, origin);
            }
            onRemove(key, null, source, false, record, removed);
            if (isStatisticsEnabled()) {
                statistics.addGetTimeNanos(System.nanoTime() - start);
                if (obj != null) {
                    statistics.increaseCacheHits(1);
                    statistics.increaseCacheRemovals(1);
                    statistics.addRemoveTimeNanos(System.nanoTime() - start);
                } else {
                    statistics.increaseCacheMisses(1);
                }
            }
            return obj;
        } catch (Throwable error) {
            onRemoveError(key, null, source, false, record, removed, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public MapEntrySet getAll(Set<Data> keySet, ExpiryPolicy expiryPolicy) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);
        final MapEntrySet result = new MapEntrySet();
        for (Data key : keySet) {
            final Object value = get(key, expiryPolicy);
            if (value != null) {
                result.add(key, toHeapData(value));
            }
        }
        return result;
    }

    @Override
    public void removeAll(Set<Data> keys, int completionId) {
        final long now = Clock.currentTimeMillis();
        final Set<Data> localKeys = new HashSet<Data>(keys.isEmpty() ? records.keySet() : keys);
        try {
            deleteAllCacheEntry(localKeys);
        } finally {
            final Set<Data> keysToClean = new HashSet<Data>(keys.isEmpty() ? records.keySet() : keys);
            for (Data key : keysToClean) {
                eventsBatchingEnabled = true;
                final R record = records.get(key);
                if (localKeys.contains(key) && record != null) {
                    final boolean isExpired = processExpiredEntry(key, record, now);
                    if (!isExpired) {
                        deleteRecord(key, IGNORE_COMPLETION);
                        if (isStatisticsEnabled()) {
                            statistics.increaseCacheRemovals(1);
                        }
                    }
                    keys.add(key);
                } else {
                    keys.remove(key);
                }
                eventsBatchingEnabled = false;
            }
            int orderKey = keys.hashCode();
            publishBatchedEvents(name, CacheEventType.REMOVED, orderKey);
            if (isEventsEnabled()) {
                publishEvent(createCacheCompleteEvent(completionId));
            }
        }
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
                    put(key, value, null, null, false, true, IGNORE_COMPLETION);
                    keysLoaded.add(key);
                }
            }
        } else {
            for (Map.Entry<Data, Object> entry : loaded.entrySet()) {
                final Data key = entry.getKey();
                final Object value = entry.getValue();
                if (value != null) {
                    final boolean hasPut = putIfAbsent(key, value, null, null, true, IGNORE_COMPLETION);
                    if (hasPut) {
                        keysLoaded.add(key);
                    }
                }
            }
        }
        return keysLoaded;
    }

    @Override
    public CacheKeyIteratorResult iterator(int tableIndex, int size) {
        return records.fetchNext(tableIndex, size);
    }

    @Override
    public Object invoke(Data key, EntryProcessor entryProcessor, Object[] arguments, int completionId) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;
        R record = records.get(key);
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
            statistics.addGetTimeNanos(System.nanoTime() - start);
        }
        CacheEntryProcessorEntry entry = createCacheEntryProcessorEntry(key, record, now, completionId);
        final Object result = entryProcessor.process(entry, arguments);
        entry.applyChanges();
        return result;
    }

    @Override
    public int size() {
        return records.size();
    }

    @Override
    public CacheStatisticsImpl getCacheStats() {
        return statistics;
    }

    @Override
    public CacheConfig getConfig() {
        return cacheConfig;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Map<Data, CacheRecord> getReadOnlyRecords() {
        return (Map<Data, CacheRecord>) Collections.unmodifiableMap(records);
    }

    @Override
    public void clear() {
        records.clear();
        onClear();
    }

    protected void onClear() {
        invalidateAllEntries();
    }

    @Override
    public void destroy() {
        clear();
        closeListeners();
        onDestroy();
    }

    protected void onDestroy() {
    }

    protected void closeListeners() {
        InternalEventService eventService = (InternalEventService) cacheService.getNodeEngine().getEventService();
        Collection<EventRegistration> candidates = eventService.getRegistrations(ICacheService.SERVICE_NAME, name);
        for (EventRegistration eventRegistration : candidates) {
            eventService.close(eventRegistration);
        }
    }

    protected class MaxSizeEvictionChecker implements EvictionChecker {
        @Override
        public boolean isEvictionRequired() {
            return maxSizeChecker != null && maxSizeChecker.isReachedToMaxSize();
        }
    }

}
