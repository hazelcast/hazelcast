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
import com.hazelcast.cache.impl.record.CacheRecordMap;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.EmptyStatement;

import javax.cache.configuration.Factory;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import java.util.*;

public abstract class AbstractCacheRecordStore<CRM extends CacheRecordMap<? extends Data, ? extends CacheRecord>> implements ICacheRecordStore {

    protected static final long INITIAL_DELAY = 5;
    protected static final long PERIOD = 5;

    protected final String name;
    protected final int partitionId;
    protected final NodeEngine nodeEngine;
    protected final AbstractCacheService cacheService;
    protected final SerializationService serializationService;
    protected final CacheConfig cacheConfig;
    protected final CRM records;
    protected final CacheRecordFactory cacheRecordFactory;
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
                                    SerializationService serializationService,
                                    ExpiryPolicy expiryPolicy) {
        this(name, partitionId, nodeEngine, cacheService, serializationService, null, expiryPolicy);
    }

    public AbstractCacheRecordStore(String name,
                                    int partitionId,
                                    NodeEngine nodeEngine,
                                    AbstractCacheService cacheService,
                                    SerializationService serializationService,
                                    CacheRecordFactory cacheRecordFactory,
                                    ExpiryPolicy expiryPolicy) {
        this.name = name;
        this.partitionId = partitionId;
        this.nodeEngine = nodeEngine;
        this.cacheService = cacheService;
        this.serializationService = serializationService;
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
        if (cacheRecordFactory != null) {
            this.cacheRecordFactory = cacheRecordFactory;
        } else {
            this.cacheRecordFactory =
                    createCacheRecordFactory(cacheConfig.getInMemoryFormat(),
                            nodeEngine.getSerializationService());
        }
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
    abstract protected CacheRecordFactory createCacheRecordFactory(InMemoryFormat inMemoryFormat,
                                                                   SerializationService serializationService);

    abstract protected <T> Data valueToData(T value);

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

}
