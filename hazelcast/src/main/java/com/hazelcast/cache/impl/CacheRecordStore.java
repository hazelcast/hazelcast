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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.impl.operation.KeyBasedCacheOperation;
import com.hazelcast.cache.impl.record.CacheRecord;
import com.hazelcast.cache.impl.record.CacheRecordHashMap;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.serialization.SerializationService;

/**
 * <h1>On-Heap implementation of the {@link ICacheRecordStore} </h1>
 * <p>
 * Hazelcast splits data homogeneously to partitions using keys. CacheRecordStore represents a named ICache on-heap
 * data store for a single partition.<br>
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
 * See {@link KeyBasedCacheOperation} subclasses for actual examples.
 *
 * @see com.hazelcast.cache.impl.CachePartitionSegment
 * @see com.hazelcast.cache.impl.CacheService
 * @see KeyBasedCacheOperation
 */
public class CacheRecordStore
        extends AbstractCacheRecordStore<CacheRecord, CacheRecordHashMap> {

    protected SerializationService serializationService;

    public CacheRecordStore(String cacheNameWithPrefix, int partitionId, NodeEngine nodeEngine,
                            AbstractCacheService cacheService) {
        super(cacheNameWithPrefix, partitionId, nodeEngine, cacheService);
        this.serializationService = nodeEngine.getSerializationService();
    }

    /**
     * Creates an instance for checking if the maximum cache size has been reached. Supports only the
     * {@link MaxSizePolicy#ENTRY_COUNT} policy. Throws an {@link IllegalArgumentException} if other {@code maxSizePolicy} is
     * used.
     *
     * @param size          the maximum number of entries
     * @param maxSizePolicy the way in which the size is interpreted, only the {@link MaxSizePolicy#ENTRY_COUNT}
     *                      {@code maxSizePolicy} is supported.
     * @return the instance which will check if the maximum number of entries has been reached
     * @throws IllegalArgumentException if the policy is not {@link MaxSizePolicy#ENTRY_COUNT} or if the {@code maxSizePolicy}
     *                                  is null
     */
    @Override
    protected EvictionChecker createCacheEvictionChecker(int size, MaxSizePolicy maxSizePolicy) {
        if (maxSizePolicy == null) {
            throw new IllegalArgumentException("Max-Size policy cannot be null");
        }

        if (maxSizePolicy != MaxSizePolicy.ENTRY_COUNT) {
            throw new IllegalArgumentException("Invalid max-size policy "
                    + '(' + maxSizePolicy + ") for " + getClass().getName() + "! Only "
                    + MaxSizePolicy.ENTRY_COUNT + " is supported.");
        } else {
            return super.createCacheEvictionChecker(size, maxSizePolicy);
        }
    }

    @Override
    protected CacheRecordHashMap createRecordCacheMap() {
        return new CacheRecordHashMap(nodeEngine.getSerializationService(), DEFAULT_INITIAL_CAPACITY, cacheContext);
    }

    @Override
    protected CacheEntryProcessorEntry createCacheEntryProcessorEntry(Data key, CacheRecord record,
                                                                      long now, int completionId) {
        return new CacheEntryProcessorEntry(key, record, this, now, completionId);
    }

    @Override
    protected CacheRecord createRecord(Object value, long creationTime, long expiryTime) {
        evictIfRequired();

        markExpirable(expiryTime);
        return cacheRecordFactory.newRecordWithExpiry(value, creationTime, expiryTime);
    }

    @Override
    protected Data valueToData(Object value) {
        return cacheService.toData(value);
    }

    @Override
    protected Object dataToValue(Data data) {
        return serializationService.toObject(data);
    }

    @Override
    protected Object recordToValue(CacheRecord record) {
        Object value = record.getValue();
        if (value instanceof Data) {
            switch (cacheConfig.getInMemoryFormat()) {
                case BINARY:
                    return value;
                case OBJECT:
                    return dataToValue((Data) value);
                default:
                    throw new IllegalStateException("Unsupported in-memory format: "
                            + cacheConfig.getInMemoryFormat());
            }
        } else {
            return value;
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
    public void disposeDeferredBlocks() {
        // NOP
    }
}
