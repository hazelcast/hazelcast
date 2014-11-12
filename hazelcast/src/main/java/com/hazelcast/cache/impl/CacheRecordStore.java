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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.NodeEngine;

import java.util.concurrent.ScheduledFuture;

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
 *<p>Sample code accessing a CacheRecordStore and getting a value. Typical operation implementation:
 *     <pre>
 *         <code>CacheService service = getService();
 *         ICacheRecordStore cache = service.getOrCreateCache(name, partitionId);
 *         cache.get(key, expiryPolicy);
 *         </code>
 *     </pre>
 * See {@link com.hazelcast.cache.impl.operation.AbstractCacheOperation} subclasses for actual examples.
 *</p>
 * @see com.hazelcast.cache.impl.CachePartitionSegment
 * @see com.hazelcast.cache.impl.CacheService
 * @see com.hazelcast.cache.impl.operation.AbstractCacheOperation
 */
public class CacheRecordStore
        extends AbstractCacheRecordStore<CacheRecord, CacheRecordHashMap> {

    protected SerializationService serializationService;
    protected CacheRecordFactory cacheRecordFactory;

    public CacheRecordStore(String name, int partitionId, NodeEngine nodeEngine,
            AbstractCacheService cacheService) {
        super(name, partitionId, nodeEngine, cacheService);
        this.serializationService = nodeEngine.getSerializationService();
        this.records = createRecordCacheMap();
        this.cacheRecordFactory = createCacheRecordFactory();
    }

    @Override
    protected CacheRecordHashMap createRecordCacheMap() {
        return new CacheRecordHashMap(DEFAULT_INITIAL_CAPACITY);
    }

    @Override
    protected CacheEntryProcessorEntry createCacheEntryProcessorEntry(Data key,
            CacheRecord record, long now) {
        return new CacheEntryProcessorEntry(key, record, this, now);
    }

    protected CacheRecordFactory createCacheRecordFactory() {
        return new CacheRecordFactory(cacheConfig.getInMemoryFormat(),
                                      nodeEngine.getSerializationService());
    }

    @Override
    protected <T> CacheRecord createRecord(T value, long creationTime, long expiryTime) {
        evictIfRequired();

        return cacheRecordFactory.newRecordWithExpiry(value, creationTime, expiryTime);
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
    protected Data toHeapData(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Data) {
            return (Data) obj;
        } else if (obj instanceof CacheRecord) {
            CacheRecord record = (CacheRecord) obj;
            Object value = record.getValue();
            return toHeapData(value);
        } else {
            return serializationService.toData(obj);
        }
    }

    @Override
    protected boolean isEvictionRequired() {
        // Eviction is not supported by CacheRecordStore
        return false;
    }

    @Override
    protected ScheduledFuture<?> scheduleExpirationTask() {
        // Expiration task is not supported by CacheRecordStore
        return null;
    }

}
