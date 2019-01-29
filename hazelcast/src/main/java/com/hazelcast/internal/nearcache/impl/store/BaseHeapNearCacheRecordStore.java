/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nearcache.impl.store;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionConfig.MaxSizePolicy;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.core.IBiFunction;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.nearcache.impl.maxsize.EntryCountNearCacheEvictionChecker;
import com.hazelcast.internal.nearcache.impl.preloader.NearCachePreloader;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Map;

import static com.hazelcast.internal.nearcache.NearCacheRecord.READ_PERMITTED;
import static java.lang.String.format;

/**
 * Base implementation of {@link AbstractNearCacheRecordStore} for on-heap Near Caches.
 *
 * @param <K> the type of the key stored in Near Cache
 * @param <V> the type of the value stored in Near Cache
 * @param <R> the type of the value of the underlying {@link com.hazelcast.internal.nearcache.impl.NearCacheRecordMap}
 */
public abstract class BaseHeapNearCacheRecordStore<K, V, R extends NearCacheRecord>
        extends AbstractNearCacheRecordStore<K, V, K, R, HeapNearCacheRecordMap<K, R>> {

    private static final int DEFAULT_INITIAL_CAPACITY = 1000;

    private final NearCachePreloader<K> nearCachePreloader;

    BaseHeapNearCacheRecordStore(String name, NearCacheConfig nearCacheConfig, SerializationService serializationService,
                                 ClassLoader classLoader) {
        super(nearCacheConfig, serializationService, classLoader);

        NearCachePreloaderConfig preloaderConfig = nearCacheConfig.getPreloaderConfig();
        this.nearCachePreloader = preloaderConfig.isEnabled() ? new NearCachePreloader<K>(name, preloaderConfig, nearCacheStats,
                serializationService) : null;
    }

    @Override
    protected EvictionChecker createNearCacheEvictionChecker(EvictionConfig evictionConfig, NearCacheConfig nearCacheConfig) {
        MaxSizePolicy maxSizePolicy = evictionConfig.getMaximumSizePolicy();
        if (maxSizePolicy != MaxSizePolicy.ENTRY_COUNT) {
            throw new IllegalArgumentException(format("Invalid max-size policy (%s) for %s! Only %s is supported.",
                    maxSizePolicy, getClass().getName(), MaxSizePolicy.ENTRY_COUNT));
        }
        return new EntryCountNearCacheEvictionChecker(evictionConfig.getSize(), records);
    }

    @Override
    protected HeapNearCacheRecordMap<K, R> createNearCacheRecordMap(NearCacheConfig nearCacheConfig) {
        return new HeapNearCacheRecordMap<K, R>(serializationService, DEFAULT_INITIAL_CAPACITY);
    }

    @Override
    public R getRecord(K key) {
        return records.get(key);
    }

    @Override
    protected R putRecord(K key, R record) {
        R oldRecord = records.put(key, record);
        nearCacheStats.incrementOwnedEntryMemoryCost(getTotalStorageMemoryCost(key, record));
        if (oldRecord != null) {
            nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost(key, oldRecord));
        }
        return oldRecord;
    }

    @Override
    protected R removeRecord(K key) {
        R removedRecord = records.remove(key);
        if (removedRecord != null && removedRecord.getRecordState() == READ_PERMITTED) {
            nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost(key, removedRecord));
        }
        return removedRecord;
    }

    @Override
    protected boolean containsRecordKey(K key) {
        return records.containsKey(key);
    }

    @Override
    public void onEvict(K key, R record, boolean wasExpired) {
        super.onEvict(key, record, wasExpired);
        nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost(key, record));
    }

    @Override
    public void doExpiration() {
        for (Map.Entry<K, R> entry : records.entrySet()) {
            K key = entry.getKey();
            R value = entry.getValue();
            if (isRecordExpired(value)) {
                remove(key);
                onExpire(key, value);
            }
        }
    }

    @Override
    public void loadKeys(DataStructureAdapter<Object, ?> adapter) {
        if (nearCachePreloader != null) {
            nearCachePreloader.loadKeys(adapter);
        }
    }

    @Override
    public void storeKeys() {
        if (nearCachePreloader != null) {
            nearCachePreloader.storeKeys(records.keySet().iterator());
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        if (nearCachePreloader != null) {
            nearCachePreloader.destroy();
        }
    }

    @Override
    protected R getOrCreateToReserve(K key, Data keyData) {
        return records.applyIfAbsent(key, new ReserveForUpdateFunction(keyData));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected V updateAndGetReserved(K key, final V value, final long reservationId, boolean deserialize) {
        R existingRecord = records.applyIfPresent(key, new IBiFunction<K, R, R>() {
            @Override
            public R apply(K key, R reservedRecord) {
                return updateReservedRecordInternal(key, value, reservedRecord, reservationId);
            }
        });

        if (existingRecord == null || !deserialize) {
            return null;
        }

        Object cachedValue = existingRecord.getValue();
        return cachedValue instanceof Data ? toValue(cachedValue) : (V) cachedValue;
    }
}
