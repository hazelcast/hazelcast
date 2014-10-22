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
import com.hazelcast.cache.impl.record.CacheRecordMap;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;

import javax.cache.configuration.Factory;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriter;
import javax.cache.integration.CacheWriterException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cache.impl.record.CacheRecordFactory.isExpiredAt;

public abstract class AbstractCacheRecordStore<
            R extends CacheRecord,
            CRM extends CacheRecordMap<Data, R>>
        implements ICacheRecordStore {

    protected static final long INITIAL_DELAY = 5;
    protected static final long PERIOD = 5;

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
    protected Map<CacheEventType, Set<CacheEventData>> batchEvent = new HashMap<CacheEventType, Set<CacheEventData>>();

    public AbstractCacheRecordStore(String name,
                                    int partitionId,
                                    NodeEngine nodeEngine,
                                    AbstractCacheService cacheService,
                                    ExpiryPolicy expiryPolicy) {
        this.name = name;
        this.partitionId = partitionId;
        this.nodeEngine = nodeEngine;
        this.cacheService = cacheService;
        this.cacheConfig = cacheService.getCacheConfig(name);
        if (this.cacheConfig == null) {
            throw new IllegalStateException("Cache is not exist !");
        }
        if (cacheConfig.getCacheLoaderFactory() != null) {
            final Factory<CacheLoader> cacheLoaderFactory = cacheConfig.getCacheLoaderFactory();
            cacheLoader = cacheLoaderFactory.create();
        }
        if (cacheConfig.getCacheWriterFactory() != null) {
            final Factory<CacheWriter> cacheWriterFactory = cacheConfig.getCacheWriterFactory();
            cacheWriter = cacheWriterFactory.create();
        }
        this.defaultExpiryPolicy =
                expiryPolicy != null
                        ? expiryPolicy :
                        (ExpiryPolicy) cacheConfig.getExpiryPolicyFactory().create();
        this.records = createRecordCacheMap();
    }

    protected boolean isReadThrough() {
        return cacheConfig.isReadThrough();
    }

    protected boolean isWriteThrough() {
        return cacheConfig.isWriteThrough();
    }

    protected boolean isStatisticsEnabled() {
        if (!cacheConfig.isStatisticsEnabled()) {
            return false;
        }
        if (statistics == null) {
            this.statistics = cacheService.createCacheStatIfAbsent(name);
        }
        return true;
    }

    abstract protected CRM createRecordCacheMap();
    abstract protected <T> R createRecord(T value, long creationTime, long expiryTime);

    abstract protected <T> Data valueToData(T value);
    abstract protected <T> T dataToValue(Data data);

    abstract protected <T> R valueToRecord(T value);
    abstract protected <T> T recordToValue(R record);

    abstract protected Data recordToData(R record);
    abstract protected R dataToRecord(Data data);

    protected R createRecord(Object value, long expiryTime) {
        return createRecord(value, Clock.currentTimeMillis(), expiryTime);
    }

    protected R createRecord(long expiryTime) {
        return createRecord(null, Clock.currentTimeMillis(), expiryTime);
    }

    protected ExpiryPolicy getExpiryPolicy(ExpiryPolicy expiryPolicy) {
        if (expiryPolicy != null) {
            return expiryPolicy;
        } else {
            return defaultExpiryPolicy;
        }
    }

    protected <R extends CacheRecord> Data getRecordData(R record) {
        if (record == null) {
            return null;
        }
        Object value = record.getValue();
        if (value == null) {
            return null;
        }
        if (value instanceof Data) {
            return (Data) value;
        } else {
            return valueToData(value);
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
                    dataValue = (Data) record.getValue();
                    break;
                case OBJECT:
                    dataValue = recordToData(record);
                    break;
                case OFFHEAP:
                    dataValue = recordToData(record);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid storage format: "
                            + cacheConfig.getInMemoryFormat());
            }
            publishEvent(name, CacheEventType.EXPIRED, key, null, dataValue, false);
        }
        return true;
    }

    public boolean processExpiredEntry(Data key,
                                       R record,
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
                    dataValue = recordToData(record);
                    break;
                case OFFHEAP:
                    dataValue = recordToData(record);
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

    protected CacheRecord accessRecord(CacheRecord record,
                                       ExpiryPolicy expiryPolicy,
                                       long now) {
        updateAccessDuration(record, getExpiryPolicy(expiryPolicy), now);
        return record;
    }

    protected void updateGetAndPutStat(boolean isPutSucceed,
                                       boolean getValue,
                                       boolean oldValueNull,
                                       long start) {
        if (isStatisticsEnabled()) {
            if (isPutSucceed) {
                statistics.increaseCachePuts(1);
                statistics.addPutTimeNano(System.nanoTime() - start);
            }
            if (getValue) {
                if (oldValueNull) {
                    statistics.increaseCacheMisses(1);
                } else {
                    statistics.increaseCacheHits(1);
                }
                statistics.addGetTimeNano(System.nanoTime() - start);
            }
        }
    }

    protected long updateAccessDuration(CacheRecord record,
                                        ExpiryPolicy expiryPolicy,
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
                statistics.addPutTimeNano(System.nanoTime() - start);
            }
            statistics.addGetTimeNano(System.nanoTime() - start);
            if (isHit) {
                statistics.increaseCacheHits(1);
            } else {
                statistics.increaseCacheMisses(1);
            }
        }
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

    protected void publishEvent(String cacheName,
                                CacheEventType eventType,
                                Data dataKey,
                                Data dataOldValue,
                                Data dataValue,
                                boolean isOldValueAvailable) {
        if (isEventBatchingEnabled) {
            final CacheEventDataImpl cacheEventData =
                    new CacheEventDataImpl(cacheName,
                            eventType,
                            dataKey,
                            dataValue,
                            dataOldValue,
                            isOldValueAvailable);
            Set<CacheEventData> cacheEventDatas = batchEvent.get(eventType);
            if (cacheEventDatas == null) {
                cacheEventDatas = new HashSet<CacheEventData>();
                batchEvent.put(eventType, cacheEventDatas);
            }
            cacheEventDatas.add(cacheEventData);
        } else {
            cacheService.publishEvent(cacheName,
                                      eventType,
                                      dataKey,
                                      dataValue,
                                      dataOldValue,
                                      isOldValueAvailable,
                                      dataKey.hashCode());
        }
    }

    protected void publishBatchedEvents(String cacheName,
                                        CacheEventType cacheEventType,
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
        Duration expiryDuration;
        try {
            expiryDuration = expiryPolicy.getExpiryForCreation();
            long durationAmount = expiryDuration.getDurationAmount();
            TimeUnit durationTimeUnit = expiryDuration.getTimeUnit();
            return TimeUnit.MILLISECONDS.convert(durationAmount, durationTimeUnit);
        } catch (Exception e) {
            return -1;
        }
    }

    protected ExpiryPolicy ttlToExpirePolicy(long ttl) {
        return new CreatedExpiryPolicy(new Duration(TimeUnit.MILLISECONDS, ttl));
    }

    protected R createRecord(Data keyData, Object value, long expirationTime) {
        final R record = createRecord(value, expirationTime);
        if (isEventsEnabled) {
            final Object recordValue = recordToValue(record);
            Data dataValue;
            if (!(recordValue instanceof Data)) {
                dataValue = valueToData(recordValue);
            } else {
                dataValue = (Data) recordValue;
            }
            publishEvent(name, CacheEventType.CREATED, keyData, null, dataValue, false);
        }
        return record;
    }

    public boolean createRecordWithExpiry(Data key,
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
            R record = createRecord(key, value, expiryTime);
            records.put(key, record);
            return true;
        }
        return false;
    }

    protected R updateRecord(Data key, R record, Object value) {
        final Data dataOldValue;
        final Data dataValue;
        Object v = value;
        switch (cacheConfig.getInMemoryFormat()) {
            case BINARY:
                if (!(value instanceof Data)) {
                    v = valueToData(value);
                }
                dataValue = (Data) v;
                dataOldValue = (Data) record.getValue();
                break;
            case OBJECT:
                if (value instanceof Data) {
                    v = dataToValue((Data) value);
                    dataValue = (Data) value;
                } else {
                    dataValue = valueToData(value);
                }
                dataOldValue = valueToData(record.getValue());
                break;
            case OFFHEAP:
                if (value instanceof Data) {
                    v = dataToValue((Data) value);
                    dataValue = (Data) value;
                } else {
                    dataValue = valueToData(value);
                }
                dataOldValue = recordToValue(record);
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

    public boolean updateRecordWithExpiry(Data key,
                                          Object value,
                                          R record,
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
                final Object objValue;
                switch (cacheConfig.getInMemoryFormat()) {
                    case BINARY:
                        objValue = dataToValue((Data) value);
                        break;
                    case OBJECT:
                        objValue = value;
                        break;
                    case OFFHEAP:
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

    @Override
    public void publishCompletedEvent(String cacheName,
                                      int completionId,
                                      Data dataKey,
                                      int orderKey) {
        if (completionId > 0) {
            cacheService
                    .publishEvent(cacheName,
                            CacheEventType.COMPLETED,
                            dataKey,
                            cacheService.toData(completionId),
                            null,
                            false,
                            orderKey);
        }
    }

    @Override
    public Object get(Data key, ExpiryPolicy expiryPolicy) {
        expiryPolicy = getExpiryPolicy(expiryPolicy);

        long now = Clock.currentTimeMillis();
        Object value = null;
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
        }
        else {
            value = recordToValue(record);
            updateAccessDuration(record, expiryPolicy, now);
            if (isStatisticsEnabled()) {
                statistics.increaseCacheHits(1);
            }
            onGet(key, expiryPolicy, value, record);
            return value;
        }
    }

    protected void onGet(Data key,
                         ExpiryPolicy expiryPolicy,
                         Object value,
                         R record) {

    }

    @Override
    public boolean contains(Data key) {
        long now = Clock.currentTimeMillis();
        R record = records.get(key);
        boolean isExpired = processExpiredEntry(key, record, now);
        return record != null && !isExpired;
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
        R record = records.get(key);
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

}
