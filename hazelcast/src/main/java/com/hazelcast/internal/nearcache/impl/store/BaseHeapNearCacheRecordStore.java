/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.eviction.EvictionChecker;
import com.hazelcast.internal.nearcache.FutureSupplier;
import com.hazelcast.internal.nearcache.MemoDelegatingFuture;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.nearcache.impl.invalidation.MinimalPartitionService;
import com.hazelcast.internal.nearcache.impl.maxsize.EntryCountNearCacheEvictionChecker;
import com.hazelcast.internal.nearcache.impl.preloader.NearCachePreloader;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.executor.CompletedFuture;
import com.hazelcast.util.function.Supplier;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;

import static com.hazelcast.util.MapUtil.isNullOrEmpty;
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

    private final boolean serializeKeys;
    private final Executor executor;
    private final NearCachePreloader<K> nearCachePreloader;
    private final IBiFunction<? super K, ? super R, ? extends R> invalidatorFunction = createInvalidatorFunction();

    BaseHeapNearCacheRecordStore(String name,
                                 NearCacheConfig nearCacheConfig,
                                 SerializationService serializationService,
                                 MinimalPartitionService partitionService,
                                 PartitioningStrategy partitioningStrategy,
                                 Executor executor,
                                 ClassLoader classLoader) {
        super(nearCacheConfig, serializationService, partitionService, partitioningStrategy, executor, classLoader);

        this.serializeKeys = nearCacheConfig.isSerializeKeys();
        this.nearCachePreloader = createPreLoader(name, nearCacheConfig, serializationService);
        this.executor = executor;
    }

    private NearCachePreloader<K> createPreLoader(String name, NearCacheConfig nearCacheConfig, SerializationService ss) {
        NearCachePreloaderConfig preloaderConfig = nearCacheConfig.getPreloaderConfig();
        return preloaderConfig.isEnabled() ? new NearCachePreloader<K>(name, preloaderConfig, nearCacheStats, ss) : null;
    }

    @Override
    protected EvictionChecker createNearCacheEvictionChecker(EvictionConfig evictionConfig,
                                                             NearCacheConfig nearCacheConfig) {
        MaxSizePolicy maxSizePolicy = evictionConfig.getMaximumSizePolicy();
        if (maxSizePolicy != MaxSizePolicy.ENTRY_COUNT) {
            throw new IllegalArgumentException(format("Invalid max-size policy (%s) for %s! Only %s is supported.",
                    maxSizePolicy, getClass().getName(), MaxSizePolicy.ENTRY_COUNT));
        }
        return new EntryCountNearCacheEvictionChecker(evictionConfig.getSize(), records);
    }

    @Override
    protected HeapNearCacheRecordMap<K, R> createNearCacheRecordMap(NearCacheConfig nearCacheConfig) {
        return new HeapNearCacheRecordMap<K, R>(ss, DEFAULT_INITIAL_CAPACITY);
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
    public void onEvict(K key, R record, boolean wasExpired) {
        super.onEvict(key, record, wasExpired);
        nearCacheStats.decrementOwnedEntryMemoryCost(getTotalStorageMemoryCost(key, record));
    }

    @Override
    public void doExpiration() {
        for (Map.Entry<K, R> entry : records.entrySet()) {
            K key = entry.getKey();
            R value = entry.getValue();
            if (isExpired(value)) {
                invalidate(key);
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

    private static boolean cachedAsNull(Object readValue) {
        return readValue != null && readValue == NearCache.CACHED_AS_NULL;
    }

    @Override
    public boolean cacheFullButEvictionDisabled(K ncKey) {
        // if there is no eviction configured we return if the Near Cache is
        // full and it's a new key (we have to check the key, otherwise we might
        // lose updates on existing keys)
        return evictionDisabled
                && evictionChecker.isEvictionRequired()
                && !records.containsKey(ncKey);
    }

    @Override
    public void invalidate(K key) {
        records.applyIfPresent(key, invalidatorFunction);

        nearCacheStats.incrementInvalidationRequests();
    }

    private IBiFunction<K, R, R> createInvalidatorFunction() {
        return new IBiFunction<K, R, R>() {
            @Override
            public R apply(K key, R record) {
                updateInvalidationStats(key, record);
                return null;
            }
        };
    }

    @Override
    public V get(K ncKey) {
        R record = records.get(ncKey);
        if (record != null) {
            record = removeIfStaleOrExpired(ncKey, record);
        }

        if (record == null) {
            nearCacheStats.incrementMisses();
            return null;
        }
        increaseAccessStats(record);
        return readValue(ncKey, record);
    }

    @Override
    public V getOrFetch(K ncKey, Supplier<ICompletableFuture> supplier) {
        doEviction(false);

        R record = records.apply(ncKey, new GetOrCreateWithSupplier(supplier));
        V v = readValue(ncKey, record);
        return v;
    }

    protected V readValue(K ncKey, R record) {
        Object state = record.getState();
        if (state instanceof FutureSupplier) {
            FutureSupplier futureSupplier = (FutureSupplier) state;
            state = ((MemoDelegatingFuture) futureSupplier.get()).getOrWaitValue(ncKey);
        }
        return (V) (state == null ? NearCache.CACHED_AS_NULL : state);
    }

    private void increaseAccessStats(R record) {
        if (canUpdateStats(record)) {
            record.setAccessTime(Clock.currentTimeMillis());
            record.incrementAccessHit();
            nearCacheStats.incrementHits();
        }
    }

    @Override
    public ICompletableFuture getOrFetchAsync(K ncKey, Supplier<ICompletableFuture> supplier) {
        doEviction(false);

        R record = records.apply(ncKey, new GetOrCreateWithSupplier(supplier));

        Object state = record.getState();
        if (state instanceof FutureSupplier) {
            FutureSupplier futureSupplier = (FutureSupplier) state;
            return futureSupplier.get();
        }
        return new CompletedFuture(ss, state, executor);
    }

    //TODO: null caching enable/disable
    @Override
    public void getAll(Collection<K> ncKeys, Map result) {
        Iterator<K> iterator = ncKeys.iterator();
        while (iterator.hasNext()) {
            K ncKey = iterator.next();
            V value = get(ncKey);
            if (value != null) {
                if (!cachedAsNull(value)) {
                    result.put(toObject(ncKey), toObject(value));
                }
                iterator.remove();
            }
        }
    }

    @Override
    public void getAllOrFetch(@Nonnull Collection<K> ncKeys,
                              @Nonnull Supplier<ICompletableFuture> supplier,
                              @Nonnull Map resultMap) {

        // TODO improve this.
        for (int i = 0; i < ncKeys.size(); i++) {
            doEviction(false);
        }

        Map<K, R> matchingRecords = collectRecordsMatchingWith(ncKeys, supplier);
        // TODO created records for missing values are still in near cache

        FutureSupplier futureSupplier = (FutureSupplier) supplier;
        Map<Object, NearCacheRecord> recordsByNcKey = futureSupplier.getRecordsByNcKey();
        if (!isNullOrEmpty(recordsByNcKey)) {
            Map userResponse = (Map) ((MemoDelegatingFuture) futureSupplier.get()).getOrWaitUserResponse();
            assert userResponse != null;
            resultMap.putAll(userResponse);
        }

        for (Map.Entry<K, R> entry : matchingRecords.entrySet()) {
            K ncKey = entry.getKey();
            if (recordsByNcKey.containsKey(ncKey)) {
                continue;
            }
            R record = entry.getValue();
            V value = readValue(ncKey, record);
            if (!cachedAsNull(value)) {
                resultMap.put(toObject(ncKey), toObject(value));
            }
        }
    }

    protected Map<K, R> collectRecordsMatchingWith(Collection<K> ncKeys, @Nonnull Supplier<ICompletableFuture> supplier) {
        Map<K, R> ncKeyToRecord = new HashMap<K, R>();
        GetOrCreateWithSupplier getOrCreateWithSupplier = new GetOrCreateWithSupplier(supplier);
        for (K ncKey : ncKeys) {
            R record = records.apply(ncKey, getOrCreateWithSupplier);
            ncKeyToRecord.put(ncKey, record);
        }
        return ncKeyToRecord;
    }

    protected <T> T toObject(Object key) {
        return ss.toObject(key);
    }
}
