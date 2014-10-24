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
import com.hazelcast.util.EmptyStatement;

import javax.cache.expiry.ExpiryPolicy;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
        return new CacheRecordHashMap<Data, CacheRecord>(1000);
    }

    @Override
    protected CacheEntryProcessorEntry createCacheEntryProcessorEntry(Data key,
                                                                      CacheRecord record,
                                                                      long now) {
        return new CacheEntryProcessorEntry(key, record, this, now);
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
    protected <T> Data valueToData(T value) {
        return cacheService.toData(value);
    }

    @Override
    protected <T> T dataToValue(Data data) {
        return (T) serializationService.toObject(data);
    }

    @Override
    protected <T> CacheRecord valueToRecord(T value) {
        return cacheRecordFactory.newRecord(value);
    }

    @Override
    protected <T> T recordToValue(CacheRecord record) {
        Object value = record.getValue();
        if (value instanceof Data) {
            return dataToValue((Data) value);
        } else {
            return (T) value;
        }
    }

    @Override
    protected Data recordToData(CacheRecord record) {
        Object value = recordToValue(record);
        if (value == null) {
            return null;
        } else if (value instanceof Data) {
            return (Data) value;
        } else {
            return valueToData(value);
        }
    }

    @Override
    protected CacheRecord dataToRecord(Data data) {
        Object value = dataToValue(data);
        if (value == null) {
            return null;
        } else if (value instanceof CacheRecord) {
            return (CacheRecord) value;
        } else {
            return valueToRecord(value);
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

    protected void onDestroy() {
        ScheduledFuture<?> f = evictionTaskFuture;
        if (f != null) {
            f.cancel(true);
        }
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
