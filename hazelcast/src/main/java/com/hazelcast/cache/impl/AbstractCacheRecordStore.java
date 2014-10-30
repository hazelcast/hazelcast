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

import com.hazelcast.cache.CacheNotExistsException;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.cache.impl.record.CacheRecordMap;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.map.impl.MapEntrySet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.record.CacheRecordFactory.isExpiredAt;

/**
 * @author sozal 14/10/14
 */
public abstract class AbstractCacheRecordStore<
            R extends CacheRecord, CRM extends CacheRecordMap<Data, R>>
        implements ICacheRecordStore {

    protected static final long DEFAULT_EVICTION_TASK_INITIAL_DELAY = 5;
    protected static final long DEFAULT_EVICTION_TASK_PERIOD = 5;
    protected static final boolean DEFAULT_IS_EVICTION_TASK_ENABLE = false;

    protected final String name;
    protected final int partitionId;
    protected final NodeEngine nodeEngine;
    protected final AbstractCacheService cacheService;
    protected final CacheConfig cacheConfig;
    protected CRM records;
    protected CacheStatisticsImpl statistics;
    protected CacheLoader cacheLoader;
    protected CacheWriter cacheWriter;
    protected boolean isEventsEnabled = true;
    protected boolean isEventBatchingEnabled;
    protected ExpiryPolicy defaultExpiryPolicy;
    protected final EvictionPolicy evictionPolicy;
    protected final boolean evictionEnabled;
    protected final int evictionPercentage;
    protected final float evictionThreshold;
    protected Map<CacheEventType, Set<CacheEventData>> batchEvent = new HashMap<CacheEventType, Set<CacheEventData>>();
    protected final ScheduledFuture<?> evictionTaskFuture;

    public AbstractCacheRecordStore(final String name,
                                    final int partitionId,
                                    final NodeEngine nodeEngine,
                                    final AbstractCacheService cacheService) {
        this(name, partitionId, nodeEngine, cacheService, null,
             DEFAULT_EVICTION_PERCENTAGE,
             DEFAULT_EVICTION_THRESHOLD_PERCENTAGE,
             DEFAULT_IS_EVICTION_TASK_ENABLE);
    }

    //CHECKSTYLE:OFF
    public AbstractCacheRecordStore(final String name,
                                    final int partitionId,
                                    final NodeEngine nodeEngine,
                                    final AbstractCacheService cacheService,
                                    final EvictionPolicy evictionPolicy,
                                    final int evictionPercentage,
                                    final int evictionThresholdPercentage,
                                    final boolean evictionTaskEnable) {
        this.name = name;
        this.partitionId = partitionId;
        this.nodeEngine = nodeEngine;
        this.cacheService = cacheService;
        this.cacheConfig = cacheService.getCacheConfig(name);
        if (cacheConfig == null) {
            throw new CacheNotExistsException("Cache already destroyed, node " + nodeEngine.getLocalMember());
        }
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
        Factory<ExpiryPolicy> expiryPolicyFactory = cacheConfig.getExpiryPolicyFactory();
        this.defaultExpiryPolicy = expiryPolicyFactory.create();
        this.evictionPolicy =
                evictionPolicy != null
                        ? evictionPolicy
                        : cacheConfig.getEvictionPolicy();
        this.evictionEnabled = evictionPolicy != EvictionPolicy.NONE;
        this.evictionPercentage = evictionPercentage;
        this.evictionThreshold = (float) Math.max(1, ONE_HUNDRED_PERCENT - evictionThresholdPercentage)
                                        / ONE_HUNDRED_PERCENT;
        if (evictionTaskEnable) {
            this.evictionTaskFuture = createEvictionTaskFuture();
        } else {
            this.evictionTaskFuture = null;
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

    protected abstract CacheEntryProcessorEntry createCacheEntryProcessorEntry(Data key,
                                                                               R record,
                                                                               long now);

    protected abstract <T> R createRecord(T value, long creationTime, long expiryTime);

    protected abstract <T> Data valueToData(T value);
    protected abstract <T> T dataToValue(Data data);

    protected abstract <T> R valueToRecord(T value);
    protected abstract <T> T recordToValue(R record);

    protected abstract Data recordToData(R record);
    protected abstract R dataToRecord(Data data);

    protected abstract Data toHeapData(Object obj);

    protected abstract boolean isEvictionRequired();

    @Override
    public int evictIfRequired() {
        if (evictionEnabled) {
            if (isEvictionRequired()) {
                return records.evictRecords(evictionPercentage, evictionPolicy);
            }
        }
        return 0;
    }

    @Override
    public int evictExpiredRecords(int percentage) {
        return records.evictExpiredRecords(percentage);
    }

    @Override
    public int forceEvict() {
        int percentage = Math.max(MIN_FORCED_EVICT_PERCENTAGE, evictionPercentage);
        return records.evictRecords(percentage, EvictionPolicy.RANDOM);
    }

    protected ScheduledFuture<?> createEvictionTaskFuture() {
        return
            nodeEngine.getExecutionService()
                .scheduleWithFixedDelay("hz:cache",
                        createEvictionTask(),
                        DEFAULT_EVICTION_TASK_INITIAL_DELAY,
                        DEFAULT_EVICTION_TASK_PERIOD,
                        TimeUnit.SECONDS);
    }

    protected EvictionTask createEvictionTask() {
        return new EvictionTask();
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

    protected <T> T toValue(Object obj) {
        if (obj instanceof Data) {
            return (T) dataToValue((Data) obj);
        } else if (obj instanceof CacheRecord) {
            return (T) recordToValue((R) obj);
        } else {
            return (T) obj;
        }
    }

    protected R toRecord(Object obj) {
        if (obj instanceof Data) {
            return dataToRecord((Data) obj);
        } else if (obj instanceof CacheRecord) {
            return (R) obj;
        } else {
            return (R) valueToRecord(obj);
        }
    }

    protected Data toEventData(Object obj) {
        if (isEventsEnabled) {
            return toHeapData(obj);
        } else {
            return null;
        }
    }

    protected ExpiryPolicy getExpiryPolicy(ExpiryPolicy expiryPolicy) {
        if (expiryPolicy != null) {
            return expiryPolicy;
        } else {
            return defaultExpiryPolicy;
        }
    }

    public boolean processExpiredEntry(Data key, R record, long now) {
        final boolean isExpired = record != null && record.isExpiredAt(now);
        if (!isExpired) {
            return false;
        }
        records.remove(key);
        if (isEventsEnabled) {
            final Data dataValue;
            switch (cacheConfig.getInMemoryFormat()) {
                case BINARY:
                    dataValue = toEventData(record);
                    break;
                case OBJECT:
                    dataValue = toEventData(record);
                    break;
                case OFFHEAP:
                    dataValue = toEventData(record);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid storage format: "
                            + cacheConfig.getInMemoryFormat());
            }
            publishEvent(name, CacheEventType.EXPIRED, key, null, dataValue, false);
        }
        return true;
    }

    public R processExpiredEntry(Data key, R record, long expiryTime, long now) {
        final boolean isExpired = isExpiredAt(expiryTime, now);
        if (!isExpired) {
            return record;
        }
        if (isStatisticsEnabled()) {
            statistics.increaseCacheExpiries(1);
        }
        records.remove(key);
        if (isEventsEnabled) {
            final Data dataValue;
            switch (cacheConfig.getInMemoryFormat()) {
                case BINARY:
                    dataValue = toEventData(record);
                    break;
                case OBJECT:
                    dataValue = toEventData(record);
                    break;
                case OFFHEAP:
                    dataValue = toEventData(record);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid storage format: "
                            + cacheConfig.getInMemoryFormat());
            }
            publishEvent(name, CacheEventType.EXPIRED, key, null, dataValue, false);
        }
        return null;
    }

    public R accessRecord(R record, ExpiryPolicy expiryPolicy, long now) {
        updateAccessDuration(record, getExpiryPolicy(expiryPolicy), now);
        return record;
    }

    protected void updateGetAndPutStat(boolean isPutSucceed, boolean getValue,
                                       boolean oldValueNull, long start) {
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

    protected long updateAccessDuration(CacheRecord record, ExpiryPolicy expiryPolicy,
                                        long now) {
        long expiryTime = -1L;
        try {
            Duration expiryDuration = expiryPolicy.getExpiryForAccess();
            if (expiryDuration != null) {
                expiryTime = expiryDuration.getAdjustedTime(now);
                record.setExpirationTime(expiryTime);
            }
        } catch (Exception e) {
            EmptyStatement.ignore(e);
            //leave the expiry time untouched when we can't determine a duration
        }
        return expiryTime;
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

    protected void publishEvent(String cacheName, CacheEventType eventType,
                                Data dataKey, Data dataOldValue,
                                Data dataValue, boolean isOldValueAvailable) {
        if (isEventBatchingEnabled) {
            final CacheEventDataImpl cacheEventData =
                    new CacheEventDataImpl(cacheName, eventType, dataKey,
                            dataValue, dataOldValue, isOldValueAvailable);
            Set<CacheEventData> cacheEventDatas = batchEvent.get(eventType);
            if (cacheEventDatas == null) {
                cacheEventDatas = new HashSet<CacheEventData>();
                batchEvent.put(eventType, cacheEventDatas);
            }
            cacheEventDatas.add(cacheEventData);
        } else {
            cacheService.publishEvent(cacheName, eventType, dataKey, dataValue,
                                      dataOldValue, isOldValueAvailable, dataKey.hashCode());
        }
    }

    protected void publishBatchedEvents(String cacheName, CacheEventType cacheEventType,
                                        int orderKey) {
        final Set<CacheEventData> cacheEventDatas = batchEvent.get(cacheEventType);
        CacheEventSet ces = new CacheEventSet(cacheEventType, cacheEventDatas);
        cacheService.publishEvent(cacheName, ces, orderKey);
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
            return -1;
        }
        try {
            Duration expiryDuration = expiryPolicy.getExpiryForCreation();
            if (expiryDuration == null || expiryDuration.isEternal()) {
                return -1;
            }
            long durationAmount = expiryDuration.getDurationAmount();
            TimeUnit durationTimeUnit = expiryDuration.getTimeUnit();
            return TimeUnit.MILLISECONDS.convert(durationAmount, durationTimeUnit);
        } catch (Exception e) {
            return -1;
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

    protected R createRecord(Data keyData, Object value, long expirationTime) {
        final R record = createRecord(value, expirationTime);
        if (isEventsEnabled) {
            Data dataValue = toEventData(value);
            publishEvent(name, CacheEventType.CREATED, keyData, null, dataValue, false);
        }
        return record;
    }

    public R createRecordWithExpiry(Data key, Object value, ExpiryPolicy expiryPolicy,
                                    long now, boolean disableWriteThrough) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        Duration expiryDuration;
        try {
            expiryDuration = expiryPolicy.getExpiryForCreation();
        } catch (Exception e) {
            expiryDuration = Duration.ETERNAL;
        }
        long expiryTime = expiryDuration.getAdjustedTime(now);

        if (!disableWriteThrough) {
            writeThroughCache(key, value);
        }

        if (!isExpiredAt(expiryTime, now)) {
            R record = createRecord(key, value, expiryTime);
            records.put(key, record);
            return record;
        }
        return null;
    }

    protected void onBeforeUpdateRecord(Data key, R record,
                                        Object value, Data oldDataValue) {
    }

    protected void onAfterUpdateRecord(Data key, R record,
                                       Object value, Data oldDataValue) {
    }

    protected void onUpdateRecordError(Data key, R record, Object value,
                                       Data newDataValue, Data oldDataValue,
                                       Throwable error) {
    }

    protected R updateRecord(Data key, R record, Object value) {
        Data dataOldValue = null;
        Data dataValue = null;
        Object v = value;
        try {
            switch (cacheConfig.getInMemoryFormat()) {
                case BINARY:
                    v = toData(value);
                    dataValue = (Data) v;
                    dataOldValue = toData(record);
                    break;
                case OBJECT:
                    if (value instanceof Data) {
                        v = dataToValue((Data) value);
                        dataValue = (Data) value;
                    } else {
                        dataValue = valueToData(value);
                    }
                    dataOldValue = toData(record);
                    break;
                case OFFHEAP:
                    v = toData(value);
                    dataValue = (Data) v;
                    dataOldValue = toData(record);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid storage format: "
                            + cacheConfig.getInMemoryFormat());
            }

            Data eventDataKey = toEventData(key);
            Data eventDataValue = toEventData(dataValue);
            Data eventDataOldValue = toEventData(dataOldValue);

            onBeforeUpdateRecord(key, record, value, dataOldValue);
            record.setValue(v);
            onAfterUpdateRecord(key, record, value, dataOldValue);

            if (isEventsEnabled) {
                publishEvent(name, CacheEventType.UPDATED, eventDataKey,
                        eventDataOldValue, eventDataValue, true);
            }
            return record;
        } catch (Throwable error) {
            onUpdateRecordError(key, record, value, dataValue, dataOldValue, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    public boolean updateRecordWithExpiry(Data key, Object value, R record,
                                          ExpiryPolicy expiryPolicy, long now,
                                          boolean disableWriteThrough) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        long expiryTime = -1L;
        try {
            Duration expiryDuration = expiryPolicy.getExpiryForUpdate();
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
        return processExpiredEntry(key, record, expiryTime, now) != null;
    }

    protected void onDeleteRecord(Data key, R record,
                                  Data dataValue, boolean deleted) {
    }

    protected void onDeleteRecordError(Data key, R record,
                                       Data dataValue, Throwable error) {
    }

    protected boolean deleteRecord(Data key) {
        final R record = records.remove(key);
        Data dataValue = null;
        try {
            switch (cacheConfig.getInMemoryFormat()) {
                case BINARY:
                    dataValue = toData(record);
                    break;
                case OBJECT:
                    dataValue = toData(record);
                    break;
                case OFFHEAP:
                    dataValue = toData(record);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid storage format: "
                            + cacheConfig.getInMemoryFormat());
            }

            Data eventDataKey = toEventData(key);
            Data eventDataValue = toEventData(dataValue);

            onDeleteRecord(key, record, dataValue, record != null);

            if (isEventsEnabled) {
                publishEvent(name, CacheEventType.REMOVED, eventDataKey,
                             null, eventDataValue, false);
            }

            return record != null;
        } catch (Throwable error) {
            onDeleteRecordError(key, record, dataValue, error);
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
        //TODO below createRecord may fire create event, is it OK?
        return createRecord(key, value, expiryTime);
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
    public void publishCompletedEvent(String cacheName, int completionId,
                                      Data dataKey, int orderKey) {
        if (completionId > 0) {
            cacheService
                    .publishEvent(cacheName, CacheEventType.COMPLETED, dataKey,
                                  cacheService.toData(completionId), null, false, orderKey);
        }
    }

    @Override
    public CacheRecord getRecord(Data key) {
        return records.get(key);
    }

    @Override
    public void setRecord(Data key, CacheRecord record) {
        records.put(key, (R) record);
    }

    @Override
    public CacheRecord removeRecord(Data key) {
        return records.remove(key);
    }

    @Override
    public Object get(Data key, ExpiryPolicy expiryPolicy) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        long now = Clock.currentTimeMillis();
        Object value;
        R record = records.get(key);
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
            value = recordToValue(record);
            updateAccessDuration(record, expiryPolicy, now);
            if (isStatisticsEnabled()) {
                statistics.increaseCacheHits(1);
            }
            onGet(key, expiryPolicy, value, record);
            return value;
        }
    }

    protected void onGet(Data key, ExpiryPolicy expiryPolicy,
                         Object value, R record) {
    }

    @Override
    public boolean contains(Data key) {
        long now = Clock.currentTimeMillis();
        R record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);
        return record != null && !isExpired;
    }

    protected Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy,
                               String caller, boolean getValue,
                               boolean disableWriteThrough) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean isOnNewPut = false;
        boolean isSaveSucceed = false;
        Object oldValue = null;
        R record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);

        try {
            // check that new entry is not already expired, in which case it should
            // not be added to the cache or listeners called or writers called.
            if (record == null || isExpired) {
                isOnNewPut = true;
                onBeforeGetAndPut(key, value, expiryPolicy, caller, getValue, disableWriteThrough,
                                  record, oldValue, isExpired, isOnNewPut);
                record = createRecordWithExpiry(key, value, expiryPolicy, now, disableWriteThrough);
                isSaveSucceed = record != null;
            } else {
                if (getValue) {
                    oldValue = toValue(record);
                }
                onBeforeGetAndPut(key, value, expiryPolicy, caller, getValue, disableWriteThrough,
                                  record, oldValue, isExpired, isOnNewPut);
                isSaveSucceed = updateRecordWithExpiry(key, value, record, expiryPolicy,
                                                       now, disableWriteThrough);
            }

            onAfterGetAndPut(key, value, expiryPolicy, caller, getValue, disableWriteThrough,
                             record, oldValue, isExpired, isOnNewPut, isSaveSucceed);

            updateGetAndPutStat(isSaveSucceed, getValue, oldValue == null, start);

            return oldValue;
        } catch (Throwable error) {
            onGetAndPutError(key, value, expiryPolicy, caller, getValue, disableWriteThrough,
                             record, oldValue, isOnNewPut, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    protected Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy,
                               String caller, boolean getValue) {
        return getAndPut(key, value, expiryPolicy, caller, getValue, false);
    }

    protected void onBeforeGetAndPut(Data key, Object value, ExpiryPolicy expiryPolicy,
                                     String caller, boolean getValue,
                                     boolean disableWriteThrough, R record,
                                     Object oldValue, boolean isExpired,
                                     boolean willBeNewPut) {
    }

    protected void onAfterGetAndPut(Data key, Object value,
                                    ExpiryPolicy expiryPolicy, String caller,
                                    boolean getValue, boolean disableWriteThrough,
                                    R record, Object oldValue, boolean isExpired,
                                    boolean isNewPut, boolean isSaveSucceed) {
    }

    protected void onGetAndPutError(Data key, Object value, ExpiryPolicy expiryPolicy,
                                    String caller, boolean getValue,
                                    boolean disableWriteThrough, R record,
                                    Object oldValue, boolean wouldBeNewPut,
                                    Throwable error) {
    }

    @Override
    public void put(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        getAndPut(key, value, expiryPolicy, caller, false, false);
    }

    @Override
    public Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy,
                            String caller) {
        return getAndPut(key, value, expiryPolicy, caller, true, false);
    }

    protected void onBeforePutIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy,
                                       String caller, boolean disableWriteThrough,
                                       R record, boolean isExpired) {
    }

    protected void onAfterPutIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy,
                                      String caller, boolean disableWriteThrough,
                                      R record, boolean isExpired, boolean isSaveSucceed) {
    }

    protected void onPutIfAbsentError(Data key, Object value, ExpiryPolicy expiryPolicy,
                                      String caller, boolean disableWriteThrough,
                                      R record, Throwable error) {
    }

    protected boolean putIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy,
                                  String caller, boolean disableWriteThrough) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean result;
        R record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);

        try {
            if (record == null || isExpired) {
                onBeforePutIfAbsent(key, value, expiryPolicy, caller,
                                    disableWriteThrough, record, isExpired);
                result = createRecordWithExpiry(key, value, expiryPolicy,
                                                now, disableWriteThrough) != null;
            } else {
                result = false;
            }

            onAfterPutIfAbsent(key, value, expiryPolicy, caller,
                               disableWriteThrough, record, isExpired, result);

            if (result && isStatisticsEnabled()) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNanos(System.nanoTime() - start);
            }

            return result;
        } catch (Throwable error) {
            onPutIfAbsentError(key, value, expiryPolicy, caller,
                               disableWriteThrough, record, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public boolean putIfAbsent(Data key, Object value,
                               ExpiryPolicy expiryPolicy, String caller) {
        return putIfAbsent(key, value, expiryPolicy, caller, false);
    }

    protected void onBeforeGetAndReplace(Data key, Object oldValue,
                                         Object newValue, ExpiryPolicy expiryPolicy,
                                         String caller, boolean getValue,
                                         R record, boolean isExpired) {
    }

    protected void onAfterGetAndReplace(Data key, Object oldValue,
                                        Object newValue, ExpiryPolicy expiryPolicy,
                                        String caller, boolean getValue,
                                        R record, boolean isExpired, boolean replaced) {
    }

    @Override
    public boolean replace(Data key, Object value, ExpiryPolicy expiryPolicy,
                           String caller) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean result;
        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);

        onBeforeGetAndReplace(key, null, value, expiryPolicy, caller,
                              false, record, isExpired);

        if (record == null || isExpired) {
            result = false;
        } else {
            result = updateRecordWithExpiry(key, value, record, expiryPolicy, now, false);
        }

        onAfterGetAndReplace(key, null, value, expiryPolicy, caller,
                             false, record, isExpired, result);

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
    public boolean replace(Data key, Object oldValue, Object newValue,
                           ExpiryPolicy expiryPolicy, String caller) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean isHit = false;
        boolean result;
        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);

        onBeforeGetAndReplace(key, oldValue, newValue, expiryPolicy,
                              caller, false, record, isExpired);

        if (record == null || isExpired) {
            result = false;
        } else {
            isHit = true;
            Object currentValue = toValue(record);
            if (compare(currentValue, toValue(oldValue))) {
                result = updateRecordWithExpiry(key, newValue, record,
                                                expiryPolicy, now, false);
            } else {
                updateAccessDuration(record, expiryPolicy, now);
                result = false;
            }
        }

        onAfterGetAndReplace(key, oldValue, newValue, expiryPolicy, caller,
                             false, record, isExpired, result);

        updateReplaceStat(result, isHit, start);

        return result;
    }

    @Override
    public Object getAndReplace(Data key, Object value, ExpiryPolicy expiryPolicy,
                                String caller) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        Object obj = null;
        boolean result;
        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);

        onBeforeGetAndReplace(key, null, value, expiryPolicy, caller,
                              true, record, isExpired);

        if (record != null) {
            obj = toValue(record);
        }
        if (record == null || isExpired) {
            obj = null;
            result = false;
        } else {
            result = updateRecordWithExpiry(key, value, record, expiryPolicy, now, false);
        }

        onAfterGetAndReplace(key, null, value, expiryPolicy, caller,
                             false, record, isExpired, result);

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
    }

    protected void onRemove(Data key, Object value, String caller,
                            boolean getValue, R record, boolean removed) {
    }

    @Override
    public boolean remove(Data key, String caller) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        deleteCacheEntry(key);

        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);

        boolean result = true;
        if (record == null || isExpired) {
            result = false;
        } else {
            deleteRecord(key);
        }

        onRemove(key, null, caller, false, record, result);

        if (result && isStatisticsEnabled()) {
            statistics.increaseCacheRemovals(1);
            statistics.addRemoveTimeNanos(System.nanoTime() - start);
        }

        return result;
    }

    @Override
    public boolean remove(Data key, Object value, String caller) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        R record = records.get(key);
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
            if (compare(toValue(record), toValue(value))) {
                deleteCacheEntry(key);
                deleteRecord(key);
            } else {
                long expiryTime = updateAccessDuration(record, defaultExpiryPolicy, now);
                processExpiredEntry(key, record, expiryTime, now);
                result = false;
            }
        }

        onRemove(key, value, caller, false, record, result);

        if (result && isStatisticsEnabled()) {
            statistics.increaseCacheRemovals(1);
            statistics.addRemoveTimeNanos(System.nanoTime() - start);
            if (hitCount == 1) {
                statistics.increaseCacheHits(hitCount);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }

        return result;
    }

    @Override
    public Object getAndRemove(Data key, String caller) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        deleteCacheEntry(key);

        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);

        final Object obj;
        boolean result;
        if (record == null || isExpired) {
            obj = null;
            result = false;
        } else {
            obj = toValue(record);
            result = deleteRecord(key);
        }

        onRemove(key, null, caller, false, record, result);

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
    }

    @Override
    public void clear() {
        records.clear();
    }

    @Override
    public MapEntrySet getAll(Set<Data> keySet, ExpiryPolicy expiryPolicy) {
        //we don not call loadAll. shouldn't we ?
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
    public void removeAll(Set<Data> keys) {
        final long now = Clock.currentTimeMillis();
        final Set<Data> localKeys =
                new HashSet<Data>(keys.isEmpty() ? records.keySet() : keys);
        try {
            deleteAllCacheEntry(localKeys);
        } finally {
            final Set<Data> keysToClean =
                    new HashSet<Data>(keys.isEmpty() ? records.keySet() : keys);
            for (Data key : keysToClean) {
                isEventBatchingEnabled = true;
                final R record = records.get(key);
                if (localKeys.contains(key) && record != null) {
                    final boolean isExpired = processExpiredEntry(key, record, now);
                    if (!isExpired) {
                        deleteRecord(key);
                        if (isStatisticsEnabled()) {
                            statistics.increaseCacheRemovals(1);
                        }
                    }
                    keys.add(key);
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

    @Override
    public CacheKeyIteratorResult iterator(int tableIndex, int size) {
        return records.fetchNext(tableIndex, size);
    }

    @Override
    public Object invoke(Data key, EntryProcessor entryProcessor, Object[] arguments) {
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
        }
        if (isStatisticsEnabled()) {
            statistics.addGetTimeNanos(System.nanoTime() - start);
        }
        CacheEntryProcessorEntry entry = createCacheEntryProcessorEntry(key, record, now);
        final Object process = entryProcessor.process(entry, arguments);
        entry.applyChanges();
        return process;
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

    protected void onEvict() {
        evictIfRequired();
    }

    protected class EvictionTask implements Runnable {
        public void run() {
            onEvict();
        }
    }

}
