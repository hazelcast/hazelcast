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
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.nio.serialization.DefaultData;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.EventServiceImpl;
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
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.operation.MutableOperation.IGNORE_COMPLETION;
import static com.hazelcast.cache.impl.record.CacheRecordFactory.isExpiredAt;

/**
 * @author sozal 14/10/14
 */
public abstract class AbstractCacheRecordStore<
        R extends CacheRecord, CRM extends CacheRecordMap<Data, R>>
        implements ICacheRecordStore {

    protected static final int DEFAULT_INITIAL_CAPACITY = 1000;

    protected static final long DEFAULT_EXPIRATION_TASK_INITIAL_DELAY = 10;
    protected static final long DEFAULT_EXPIRATION_TASK_PERIOD = 10;

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
    protected volatile boolean hasExpiringEntry;
    protected final boolean evictionEnabled;
    protected final int evictionPercentage;
    protected final int evictionThresholdPercentage;
    protected final Map<CacheEventType, Set<CacheEventData>> batchEvent = new HashMap<CacheEventType, Set<CacheEventData>>();
    protected ScheduledFuture<?> expirationTaskScheduler;

    //CHECKSTYLE:OFF
    public AbstractCacheRecordStore(final String name,
                                    final int partitionId,
                                    final NodeEngine nodeEngine,
                                    final AbstractCacheService cacheService) {
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
        this.evictionPolicy = cacheConfig.getEvictionPolicy() != null
                ? cacheConfig.getEvictionPolicy() : EvictionPolicy.NONE;
        this.evictionEnabled = evictionPolicy != EvictionPolicy.NONE;
        this.evictionPercentage = cacheConfig.getEvictionPercentage();
        this.evictionThresholdPercentage = cacheConfig.getEvictionThresholdPercentage();
        this.expirationTaskScheduler = scheduleExpirationTask();
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
                                                                               long now, int completionId);

    protected abstract <T> R createRecord(T value, long creationTime, long expiryTime);

    protected abstract <T> Data valueToData(T value);

    protected abstract <T> T dataToValue(Data data);

    protected abstract <T> R valueToRecord(T value);

    protected abstract <T> T recordToValue(R record);

    protected abstract Data recordToData(R record);

    protected abstract R dataToRecord(Data data);

    protected abstract Data toHeapData(Object obj);

    protected abstract boolean isEvictionRequired();

    protected void updateHasExpiringEntry(R record) {
        if (record != null) {
            if (!hasExpiringEntry && record.getExpirationTime() >= 0) {
                hasExpiringEntry = true;
            }
        }
    }

    @Override
    public int evictIfRequired() {
        if (evictionEnabled) {
            if (isEvictionRequired()) {
                int evictedCount = records.evictRecords(evictionPercentage, evictionPolicy);
                if (isStatisticsEnabled()) {
                    statistics.increaseCacheEvictions(evictedCount);
                }
            }
        }
        return 0;
    }

    @Override
    public int evictExpiredRecords(int percentage) {
        int expiredCount = records.evictExpiredRecords(percentage);
        if (isStatisticsEnabled()) {
            statistics.increaseCacheExpiries(expiredCount);
        }
        return expiredCount;
    }

    @Override
    public int forceEvict() {
        int evicted = 0;
        int percentage = Math.max(MIN_FORCED_EVICT_PERCENTAGE, evictionPercentage);

        if (hasExpiringEntry) {
            int expiredCount = records.evictExpiredRecords(ONE_HUNDRED_PERCENT);
            if (isStatisticsEnabled()) {
                statistics.increaseCacheExpiries(expiredCount);
            }
            evicted += expiredCount;
        }

        int evictedCount = records.evictRecords(percentage, EvictionPolicy.RANDOM);
        if (isStatisticsEnabled()) {
            statistics.increaseCacheEvictions(evictedCount);
        }
        evicted += evictedCount;

        return evicted;
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
                case OBJECT:
                case NATIVE:
                    dataValue = toEventData(record);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid storage format: "
                            + cacheConfig.getInMemoryFormat());
            }
            publishEvent(CacheEventType.EXPIRED, key, null, dataValue, false, IGNORE_COMPLETION);
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
                case OBJECT:
                case NATIVE:
                    dataValue = toEventData(record);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid storage format: "
                            + cacheConfig.getInMemoryFormat());
            }
            publishEvent(CacheEventType.EXPIRED, key, null, dataValue, false, IGNORE_COMPLETION);
        }
        return null;
    }

    public R accessRecord(R record, ExpiryPolicy expiryPolicy, long now) {
        onRecordAccess(record, getExpiryPolicy(expiryPolicy), now);
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

    protected long updateAccessDuration(R record, ExpiryPolicy expiryPolicy, long now) {
        long expiryTime = -1L;
        try {
            Duration expiryDuration = expiryPolicy.getExpiryForAccess();
            if (expiryDuration != null) {
                expiryTime = expiryDuration.getAdjustedTime(now);
                record.setExpirationTime(expiryTime);
            }
        } catch (Exception e) {
            EmptyStatement.ignore(e);
            // leave the expiry time untouched when we can't determine a duration
        }
        return expiryTime;
    }

    protected long onRecordAccess(R record, ExpiryPolicy expiryPolicy, long now) {
        record.setAccessTime(now);
        record.incrementAccessHit();
        return updateAccessDuration(record, expiryPolicy, now);
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

    protected void publishEvent(CacheEventType eventType,
                                Data dataKey, Data dataOldValue,
                                Data dataValue, boolean isOldValueAvailable, int completionId) {
        if (isEventBatchingEnabled) {
            final CacheEventDataImpl cacheEventData =
                    new CacheEventDataImpl(name, eventType, dataKey,
                            dataValue, dataOldValue, isOldValueAvailable);
            Set<CacheEventData> cacheEventDatas = batchEvent.get(eventType);
            if (cacheEventDatas == null) {
                cacheEventDatas = new HashSet<CacheEventData>();
                batchEvent.put(eventType, cacheEventDatas);
            }
            cacheEventDatas.add(cacheEventData);
        } else {
            cacheService.publishEvent(name, eventType, dataKey, dataValue,
                    dataOldValue, isOldValueAvailable, dataKey.hashCode(), completionId);
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

    protected R createRecord(Data keyData, Object value, long expirationTime, int completionId) {
        final R record = createRecord(value, expirationTime);
        updateHasExpiringEntry(record);
        if (isEventsEnabled) {
            Data dataValue = toEventData(value);
            publishEvent(CacheEventType.CREATED, keyData, null, dataValue, false, completionId);
        }
        return record;
    }

    public R createRecordWithExpiry(Data key, Object value, ExpiryPolicy expiryPolicy,
                                    long now, boolean disableWriteThrough, int completionId) {
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
            R record = createRecord(key, value, expiryTime, completionId);
            records.put(key, record);
            return record;
        }
        publishEvent(CacheEventType.COMPLETED, key, null, null, false, completionId);
        return null;
    }
    protected void onUpdateRecord(Data key, R record, Object value, Data oldDataValue) {
    }

    protected void onUpdateRecordError(Data key, R record, Object value, Data newDataValue,
                                       Data oldDataValue, Throwable error) {
    }

    protected void onUpdateRecordError(Data key, R record, Object value,
                                       Data newDataValue, Data oldDataValue,
                                       Throwable error) {
    }

    protected R updateRecord(Data key, R record, Object value, int completionId) {
    protected R updateRecord(Data key, R record, Object value) {
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
                    throw new IllegalArgumentException("Invalid storage format: "
                            + cacheConfig.getInMemoryFormat());
            }

            Data eventDataKey = toEventData(key);
            Data eventDataValue = toEventData(dataValue);
            Data eventDataOldValue = toEventData(dataOldValue);

            record.setValue(recordValue);

            onUpdateRecord(key, record, value, dataOldValue);

            updateHasExpiringEntry(record);

            if (isEventsEnabled) {
                publishEvent(CacheEventType.UPDATED, eventDataKey,
                        eventDataOldValue, eventDataValue, true, completionId);
            }
            return record;
        } catch (Throwable error) {
            onUpdateRecordError(key, record, value, dataValue, dataOldValue, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    public boolean updateRecordWithExpiry(Data key, Object value, R record,
                                          ExpiryPolicy expiryPolicy, long now,
                                          boolean disableWriteThrough, int completionId) {
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
        updateRecord(key, record, value, completionId);
        return processExpiredEntry(key, record, expiryTime, now) != null;
    }

    protected void onDeleteRecord(Data key, R record, Data dataValue, boolean deleted) {
    }

    protected void onDeleteRecordError(Data key, R record, Data dataValue,
            boolean deleted, Throwable error) {
    }

    protected boolean deleteRecord(Data key, int completionId) {
        final R record = records.remove(key);
        Data dataValue = null;
        try {
            switch (cacheConfig.getInMemoryFormat()) {
                case BINARY:
                case OBJECT:
                case NATIVE:
                    dataValue = toData(record);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid storage format: "
                            + cacheConfig.getInMemoryFormat());
            }

            Data eventDataKey = toEventData(key);
            Data eventDataValue = toEventData(dataValue);

            onDeleteRecord(key, record, dataValue, record != null);

            if (records.size() == 0) {
                hasExpiringEntry = false;
            }

            if (isEventsEnabled) {
                publishEvent(CacheEventType.REMOVED, eventDataKey,
                        null, eventDataValue, false, completionId);
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
        //TODO below createRecord may fire create event, is it OK?
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
    public void publishCompletedEvent(String cacheName, int completionId, Data dataKey, int orderKey) {
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

    protected void onGet(Data key, ExpiryPolicy expiryPolicy, Object value, R record) {
    }

    protected void onGetError(Data key, ExpiryPolicy expiryPolicy, Object value,
                              R record, Throwable error) {
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
                record = createRecordWithExpiry(key, value, expiryPolicy, now, true);
            } else {
                value = recordToValue(record);
                onRecordAccess(record, expiryPolicy, now);
                if (isStatisticsEnabled()) {
                    statistics.increaseCacheHits(1);
                }
            }
            value = readThroughCache(key);
            if (value == null) {
                return null;
            }
            createRecordWithExpiry(key, value, expiryPolicy, now, true, IGNORE_COMPLETION);
            return value;
        } else {
            value = recordToValue(record);
            updateAccessDuration(record, expiryPolicy, now);
            if (isStatisticsEnabled()) {
                statistics.increaseCacheHits(1);
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

    protected Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy,
                               String caller, boolean getValue,
                               boolean disableWriteThrough, int completionId) {
    protected void onPut(Data key, Object value, ExpiryPolicy expiryPolicy, String caller,
                         boolean getValue, boolean disableWriteThrough, R record, Object oldValue,
                         boolean isExpired, boolean isNewPut, boolean isSaveSucceed) {
    }

    protected void onPutError(Data key, Object value, ExpiryPolicy expiryPolicy, String caller,
                              boolean getValue, boolean disableWriteThrough, R record,
                              Object oldValue, boolean wouldBeNewPut, Throwable error) {
    }

    protected Object put(Data key, Object value, ExpiryPolicy expiryPolicy,
                         String caller, boolean getValue, boolean disableWriteThrough) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean isOnNewPut = false;
        boolean isSaveSucceed;
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
                record = createRecordWithExpiry(key, value, expiryPolicy, now, disableWriteThrough, completionId);
                record = createRecordWithExpiry(key, value, expiryPolicy, now, disableWriteThrough);
                isSaveSucceed = record != null;
            } else {
                if (getValue) {
                    oldValue = toValue(record);
                }
                onBeforeGetAndPut(key, value, expiryPolicy, caller, getValue, disableWriteThrough,
                        record, oldValue, isExpired, isOnNewPut);
                isSaveSucceed = updateRecordWithExpiry(key, value, record, expiryPolicy,
                        now, disableWriteThrough, completionId);
            }

            onAfterGetAndPut(key, value, expiryPolicy, caller, getValue, disableWriteThrough,
                    record, oldValue, isExpired, isOnNewPut, isSaveSucceed);
            onPut(key, value, expiryPolicy, caller, getValue, disableWriteThrough,
                    record, oldValue, isExpired, isOnNewPut, isSaveSucceed);

            updateGetAndPutStat(isSaveSucceed, getValue, oldValue == null, start);

            updateHasExpiringEntry(record);

            return oldValue;
        } catch (Throwable error) {
            onGetAndPutError(key, value, expiryPolicy, caller, getValue, disableWriteThrough,
                    record, oldValue, isOnNewPut, error);
            onPutError(key, value, expiryPolicy, caller, getValue, disableWriteThrough,
                    record, oldValue, isOnNewPut, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    protected Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy,
                               String caller, boolean getValue, int completionId) {
        return getAndPut(key, value, expiryPolicy, caller, getValue, false, completionId);
    protected Object put(Data key, Object value, ExpiryPolicy expiryPolicy,
                         String caller, boolean getValue) {
        return put(key, value, expiryPolicy, caller, getValue, false);
    }

    @Override
    public void put(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        put(key, value, expiryPolicy, caller, false, false);
    public void put(Data key, Object value, ExpiryPolicy expiryPolicy, String caller, int completionId) {
        getAndPut(key, value, expiryPolicy, caller, false, false, completionId);
    }

    @Override
    public Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy,
                            String caller, int completionId) {
        return getAndPut(key, value, expiryPolicy, caller, true, false, completionId);
    public Object getAndPut(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        return put(key, value, expiryPolicy, caller, true, false);
    }

    protected void onPutIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, String caller,
                                 boolean disableWriteThrough, R record, boolean isExpired,
                                 boolean isSaveSucceed) {
    }

    protected void onPutIfAbsentError(Data key, Object value, ExpiryPolicy expiryPolicy,
                                      String caller, boolean disableWriteThrough,
                                      R record, Throwable error) {
    }

    protected boolean putIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy,
                                  String caller, boolean disableWriteThrough, int completionId) {
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
                        now, disableWriteThrough, completionId) != null;
            } else {
                result = false;
            }

            onAfterPutIfAbsent(key, value, expiryPolicy, caller,
                    disableWriteThrough, record, isExpired, result);
            onPutIfAbsent(key, value, expiryPolicy, caller,
                          disableWriteThrough, record, isExpired, result);

            updateHasExpiringEntry(record);

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
                               ExpiryPolicy expiryPolicy, String caller, int completionId) {
        return putIfAbsent(key, value, expiryPolicy, caller, false, completionId);
    public boolean putIfAbsent(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        return putIfAbsent(key, value, expiryPolicy, caller, false);
    }

    protected void onReplace(Data key, Object oldValue, Object newValue, ExpiryPolicy expiryPolicy,
                             String caller, boolean getValue, R record,
                             boolean isExpired, boolean replaced) {
    }

    protected void onReplaceError(Data key, Object oldValue, Object newValue, ExpiryPolicy expiryPolicy,
                                  String caller, boolean getValue, R record,
                                  boolean isExpired, boolean replaced, Throwable error) {
    }

    @Override
    public boolean replace(Data key, Object value, ExpiryPolicy expiryPolicy,
                           String caller, int completionId) {
    public boolean replace(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean replaced = false;
        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);

        try {
            if (record == null || isExpired) {
                replaced = false;
            } else {
                replaced = updateRecordWithExpiry(key, value, record, expiryPolicy, now, false);
            }
        onBeforeGetAndReplace(key, null, value, expiryPolicy, caller,
                false, record, isExpired);

        if (record == null || isExpired) {
            result = false;
        } else {
            result = updateRecordWithExpiry(key, value, record, expiryPolicy, now, false, completionId);
        }
            onReplace(key, null, value, expiryPolicy, caller,
                      false, record, isExpired, replaced);

            updateHasExpiringEntry(record);
        onAfterGetAndReplace(key, null, value, expiryPolicy, caller,
                false, record, isExpired, result);

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
            onReplaceError(key, null, value, expiryPolicy, caller,
                    false, record, isExpired, replaced, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public boolean replace(Data key, Object oldValue, Object newValue,
                           ExpiryPolicy expiryPolicy, String caller, int completionId) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        boolean isHit = false;
        boolean replaced = false;
        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);

        try {
            if (record == null || isExpired) {
                replaced = false;
        onBeforeGetAndReplace(key, oldValue, newValue, expiryPolicy,
                caller, false, record, isExpired);

        if (record == null || isExpired) {
            result = false;
        } else {
            isHit = true;
            Object currentValue = toValue(record);
            if (compare(currentValue, toValue(oldValue))) {
                result = updateRecordWithExpiry(key, newValue, record,
                        expiryPolicy, now, false, completionId);
            } else {
                isHit = true;
                Object currentValue = toValue(record);
                if (compare(currentValue, toValue(oldValue))) {
                    replaced = updateRecordWithExpiry(key, newValue, record,
                                                      expiryPolicy, now, false);
                } else {
                    onRecordAccess(record, expiryPolicy, now);
                    replaced = false;
                }
            }

        onAfterGetAndReplace(key, oldValue, newValue, expiryPolicy, caller,
                false, record, isExpired, result);
            onReplace(key, oldValue, newValue, expiryPolicy, caller,
                      false, record, isExpired, replaced);

            updateReplaceStat(replaced, isHit, start);

            updateHasExpiringEntry(record);

            return replaced;
        } catch (Throwable error) {
            onReplaceError(key, oldValue, newValue, expiryPolicy, caller,
                           false, record, isExpired, replaced, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public Object getAndReplace(Data key, Object value, ExpiryPolicy expiryPolicy,
                                String caller, int completionId) {
    public Object getAndReplace(Data key, Object value, ExpiryPolicy expiryPolicy, String caller) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        Object obj = null;
        boolean replaced = false;
        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);

        onBeforeGetAndReplace(key, null, value, expiryPolicy, caller,
                true, record, isExpired);
        try {
            if (record != null) {
                obj = toValue(record);
            }
            if (record == null || isExpired) {
                obj = null;
                replaced = false;
            } else {
                replaced = updateRecordWithExpiry(key, value, record, expiryPolicy, now, false);
            }

        if (record != null) {
            obj = toValue(record);
        }
        if (record == null || isExpired) {
            obj = null;
            result = false;
        } else {
            result = updateRecordWithExpiry(key, value, record, expiryPolicy, now, false, completionId);
        }
            onReplace(key, null, value, expiryPolicy, caller,
                      false, record, isExpired, replaced);

        onAfterGetAndReplace(key, null, value, expiryPolicy, caller,
                false, record, isExpired, result);
            updateHasExpiringEntry(record);

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
            onReplaceError(key, null, value, expiryPolicy, caller,
                           false, record, isExpired, replaced, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    protected void onRemove(Data key, Object value, String caller,
                            boolean getValue, R record, boolean removed) {
    }

    protected void onRemoveError(Data key, Object value, String caller, boolean getValue,
                                 R record, boolean removed, Throwable error) {
    }

    @Override
    public boolean remove(Data key, String caller, int completionId) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        deleteCacheEntry(key);

        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        boolean removed = false;

        boolean result = true;
        if (record == null || isExpired) {
            result = false;
            publishEvent(CacheEventType.COMPLETED, key, null, null, false, completionId);
        } else {
            deleteRecord(key, completionId);
        }
        try {
            if (record == null || isExpired) {
                removed = false;
            } else {
                removed = deleteRecord(key);
            }

            onRemove(key, null, caller, false, record, removed);

            if (records.size() == 0) {
                hasExpiringEntry = false;
            }

            if (removed && isStatisticsEnabled()) {
                statistics.increaseCacheRemovals(1);
                statistics.addRemoveTimeNanos(System.nanoTime() - start);
            }

            return removed;
        } catch (Throwable error) {
            onRemoveError(key, null, caller, false, record, removed, error);
            throw ExceptionUtil.rethrow(error);
        }
    }

    @Override
    public boolean remove(Data key, Object value, String caller, int completionId) {
        final long now = Clock.currentTimeMillis();
        final long start = System.nanoTime();

        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        int hitCount = 0;
        boolean removed = false;

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
                deleteRecord(key, completionId);
        try {
            if (record == null || isExpired) {
                if (isStatisticsEnabled()) {
                    statistics.increaseCacheMisses(1);
                }
                removed = false;
            } else {
                hitCount++;
                if (compare(toValue(record), toValue(value))) {
                    deleteCacheEntry(key);
                    removed = deleteRecord(key);
                } else {
                    long expiryTime = onRecordAccess(record, defaultExpiryPolicy, now);
                    processExpiredEntry(key, record, expiryTime, now);
                    removed = false;
                }
            }
        }
        if (!result) {
            publishEvent(CacheEventType.COMPLETED, key, null, null, false, completionId);
        }

            onRemove(key, value, caller, false, record, removed);

            if (records.size() == 0) {
                hasExpiringEntry = false;
            }

        updateRemoveStatistics(result, hitCount, start);

        return result;
    }

    private void updateRemoveStatistics(boolean result, int hitCount, long start) {
        if (result && isStatisticsEnabled()) {
            statistics.increaseCacheRemovals(1);
            statistics.addRemoveTimeNanos(System.nanoTime() - start);
            if (hitCount == 1) {
                statistics.increaseCacheHits(hitCount);
            } else {
                statistics.increaseCacheMisses(1);
            if (removed && isStatisticsEnabled()) {
                statistics.increaseCacheRemovals(1);
                statistics.addRemoveTimeNanos(System.nanoTime() - start);
                if (hitCount == 1) {
                    statistics.increaseCacheHits(hitCount);
                } else {
                    statistics.increaseCacheMisses(1);
                }
            }

            return removed;
        } catch (Throwable error) {
            onRemoveError(key, null, caller, false, record, removed, error);
            throw ExceptionUtil.rethrow(error);
        }
        }
    }

    @Override
    public Object getAndRemove(Data key, String caller, int completionId) {
        final long now = Clock.currentTimeMillis();
        final long start = isStatisticsEnabled() ? System.nanoTime() : 0;

        deleteCacheEntry(key);

        R record = records.get(key);
        boolean isExpired = record != null && record.isExpiredAt(now);
        final Object obj;
        boolean removed = false;
        boolean result;
        if (record == null || isExpired) {
            obj = null;
            result = false;
        } else {
            obj = toValue(record);
            result = deleteRecord(key, completionId);
        }

        try {
            if (record == null || isExpired) {
                obj = null;
                removed = false;
            } else {
                obj = toValue(record);
                removed = deleteRecord(key);
            }

            onRemove(key, null, caller, false, record, removed);

            if (records.size() == 0) {
                hasExpiringEntry = false;
            }

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
            onRemoveError(key, null, caller, false, record, removed, error);
            throw ExceptionUtil.rethrow(error);
        }
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
    public void removeAll(Set<Data> keys, int completionId) {
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
                        deleteRecord(key, IGNORE_COMPLETION);
                        if (isStatisticsEnabled()) {
                            statistics.increaseCacheRemovals(1);
                        }
                    }
                    keys.add(key);
                } else {
                    keys.remove(key);
                }
                isEventBatchingEnabled = false;
                hasExpiringEntry = false;
            }
            int orderKey = keys.hashCode();
            publishBatchedEvents(name, CacheEventType.REMOVED, orderKey);
            publishEvent(CacheEventType.COMPLETED, new DefaultData(), null, null, false, completionId);
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
                    getAndPut(key, value, null, null, false, true, IGNORE_COMPLETION);
                    put(key, value, null, null, false, true);
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
        }
        if (isStatisticsEnabled()) {
            statistics.addGetTimeNanos(System.nanoTime() - start);
        }
        CacheEntryProcessorEntry entry = createCacheEntryProcessorEntry(key, record, now, completionId);
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

    @Override
    public void destroy() {
        closeScheduledTasks();
        clear();
        closeResources();
        closeListeners();
    }

    protected void closeScheduledTasks() {
        if (expirationTaskScheduler != null) {
            expirationTaskScheduler.cancel(true);
        }
    }

    protected void closeListeners() {
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

    protected ScheduledFuture<?> scheduleExpirationTask() {
        return nodeEngine.getExecutionService()
                .scheduleWithFixedDelay("hz:cache",
                        new ExpirationTask(),
                        DEFAULT_EXPIRATION_TASK_INITIAL_DELAY,
                        DEFAULT_EXPIRATION_TASK_PERIOD,
                        TimeUnit.SECONDS);
    }

    protected void onExpiry() {
        if (hasExpiringEntry) {
            evictExpiredRecords(evictionPercentage);
        }
    }

    /**
     * Task to evict expired records
     */
    protected class ExpirationTask implements Runnable {
        @Override
        public void run() {
            onExpiry();
        }
    }

}
