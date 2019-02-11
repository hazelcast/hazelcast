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

package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.nearcache.FutureSupplier;
import com.hazelcast.internal.nearcache.GetAllMemoDelegatingFuture;
import com.hazelcast.internal.nearcache.GetMemoDelegatingFuture;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheRecord;
import com.hazelcast.internal.nearcache.NearCacheRecordStore;
import com.hazelcast.internal.nearcache.impl.invalidation.MinimalPartitionService;
import com.hazelcast.internal.nearcache.impl.store.NearCacheDataRecordStore;
import com.hazelcast.internal.nearcache.impl.store.NearCacheObjectRecordStore;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.executor.CompletedFuture;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.NearCacheConfig.DEFAULT_MEMORY_FORMAT;
import static com.hazelcast.util.Preconditions.checkNotInstanceOf;

@SuppressWarnings("checkstyle:methodcount")
public class DefaultNearCache<K, V> implements NearCache<K, V> {
    protected final String name;
    protected final TaskScheduler scheduler;
    protected final ClassLoader classLoader;
    protected final SerializationService ss;
    protected final NearCacheConfig nearCacheConfig;

    protected ScheduledFuture expirationTaskFuture;
    protected NearCacheRecordStore nearCacheRecordStore;

    private final boolean serializeKeys;
    private final Executor executor;
    private final HazelcastProperties properties;
    private final MinimalPartitionService partitionService;
    private final PartitioningStrategy partitioningStrategy;

    private volatile boolean preloadDone;
    private volatile NearCacheDataFetcher nearCacheDataFetcher;

    public DefaultNearCache(String name, NearCacheConfig nearCacheConfig,
                            SerializationService ss,
                            MinimalPartitionService partitionService,
                            PartitioningStrategy partitioningStrategy,
                            Executor executor,
                            TaskScheduler scheduler,
                            ClassLoader classLoader,
                            HazelcastProperties properties) {
        this(name, nearCacheConfig, null,
                ss, partitionService, partitioningStrategy,
                executor, scheduler, classLoader, properties);
    }

    public DefaultNearCache(String name, NearCacheConfig nearCacheConfig,
                            NearCacheRecordStore nearCacheRecordStore,
                            SerializationService ss,
                            MinimalPartitionService partitionService,
                            PartitioningStrategy partitioningStrategy,
                            Executor executor,
                            TaskScheduler scheduler,
                            ClassLoader classLoader,
                            HazelcastProperties properties) {
        this.name = name;
        this.nearCacheConfig = nearCacheConfig;
        this.ss = ss;
        this.classLoader = classLoader;
        this.scheduler = scheduler;
        this.nearCacheRecordStore = nearCacheRecordStore;
        this.serializeKeys = nearCacheConfig.isSerializeKeys();
        this.properties = properties;
        this.partitionService = partitionService;
        this.partitioningStrategy = partitioningStrategy;
        this.executor = executor;
    }

    @Override
    public void initialize() {
        if (nearCacheRecordStore == null) {
            nearCacheRecordStore = createNearCacheRecordStore(name, nearCacheConfig);
        }
        nearCacheRecordStore.initialize();

        expirationTaskFuture = createAndScheduleExpirationTask();
    }

    protected <NCK, NCV> NearCacheRecordStore<NCK, NCV> createNearCacheRecordStore(String name, NearCacheConfig nearCacheConfig) {
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (inMemoryFormat == null) {
            inMemoryFormat = DEFAULT_MEMORY_FORMAT;
        }
        switch (inMemoryFormat) {
            case BINARY:
                return new NearCacheDataRecordStore<NCK, NCV>(name, nearCacheConfig, ss,
                        partitionService, partitioningStrategy, executor, classLoader);
            case OBJECT:
                return new NearCacheObjectRecordStore<NCK, NCV>(name, nearCacheConfig, ss,
                        partitionService, partitioningStrategy, executor, classLoader);
            default:
                throw new IllegalArgumentException("Invalid in memory format: " + inMemoryFormat);
        }
    }

    private ScheduledFuture createAndScheduleExpirationTask() {
        if (nearCacheConfig.getMaxIdleSeconds() > 0L || nearCacheConfig.getTimeToLiveSeconds() > 0L) {
            ExpirationTask expirationTask = new ExpirationTask();
            return expirationTask.schedule(scheduler);
        }
        return null;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean containsKey(K key) {
        Object ncKey = toNearCacheKey(key);

        Object value = nearCacheRecordStore.get(ncKey);
        if (value != null) {
            return value != CACHED_AS_NULL;
        }

        return (Boolean) nearCacheDataFetcher.invokeContainsKeyOperation(ncKey).join();
    }

    @Override
    public V get(K key) {
        return get(key, null);
    }

    // TODO return obj form
    @Override
    public V get(K key, Object param) {
        final long startTimeNanos = nearCacheDataFetcher.isStatsEnabled() ? System.nanoTime() : -1;
        Object ncKey = toNearCacheKey(key);

        Object value = nearCacheRecordStore.get(ncKey);

        if (value != null) {
            V val = value == CACHED_AS_NULL ? null : (V) value;
            nearCacheDataFetcher.interceptAfterGet(name, val);
            return val;
        }

        if (nearCacheRecordStore.cacheFullButEvictionDisabled(ncKey)) {
            return (V) nearCacheDataFetcher.getWithoutNearCaching(toDataWithStrategy(ncKey), param, startTimeNanos);
        }

        FutureSupplier supplier = newFutureSupplier(ncKey, param, false, startTimeNanos);
        value = nearCacheRecordStore.getOrFetch(ncKey, supplier);
        return value == CACHED_AS_NULL ? null : (V) value;
    }

    @Override
    public Object getCached(K key) {
        Object ncKey = toNearCacheKey(key);
        return nearCacheRecordStore.get(ncKey);
    }

    @Override
    public ICompletableFuture getAsync(K key) {
        return getAsync(key, null);
    }

    @Override
    public ICompletableFuture getAsync(K key, Object param) {
        final long startTimeNanos = nearCacheDataFetcher.isStatsEnabled() ? System.nanoTime() : -1;
        Object ncKey = toNearCacheKey(key);

        Object value = nearCacheRecordStore.get(ncKey);
        if (value != null) {
            value = value == CACHED_AS_NULL ? null : (V) value;
            nearCacheDataFetcher.interceptAfterGet(name, value);
            return new CompletedFuture(ss, value, executor);
        }

        if (nearCacheRecordStore.cacheFullButEvictionDisabled(ncKey)) {
            return nearCacheDataFetcher.getAsyncWithoutNearCaching(toDataWithStrategy(ncKey), param, startTimeNanos);
        }

        FutureSupplier supplier = newFutureSupplier(ncKey, param, true, startTimeNanos);
        return nearCacheRecordStore.getOrFetchAsync(ncKey, supplier);
    }

    @Nonnull
    private FutureSupplier newFutureSupplier(final Object ncKey, final Object param,
                                             final boolean asyncGet, final long startTimeNanos) {
        FutureSupplier supplier = new FutureSupplier() {
            @Override
            protected ICompletableFuture supply() {
                InternalCompletableFuture future
                        = nearCacheDataFetcher.invokeGetOperation(getDataKey(ncKey),
                        ss.toData(param), true, startTimeNanos);
                return new GetMemoDelegatingFuture(asyncGet, this, future, ss);
            }
        };
        Data dataKey = ncKey instanceof Data ? (Data) ncKey : toDataWithStrategy(ncKey);
        supplier.mapNcKeyToDataKey(ncKey, dataKey);
        return supplier;
    }

    //TODO: add strategy also.
    private Data toDataWithStrategy(Object ncKey) {
        return ss.toData(ncKey);
    }

    private Object toNearCacheKey(Object key) {
        return serializeKeys ? toDataWithStrategy(key) : key;
    }

    @Override
    public void getAll(Collection<K> keys, Map result, final long startTimeNanos) {
        getAll(keys, null, result, startTimeNanos);
    }

    @Override
    public void getAll(Collection<K> keys, Data param, Map result, final long startTimeNanos) {
        Collection ncKeys = new ArrayList(keys.size());
        for (K key : keys) {
            ncKeys.add(toNearCacheKey(key));
        }

        nearCacheRecordStore.getAll(ncKeys, result);

        if (ncKeys.isEmpty()) {
            return;
        }
// TODO: Add
//        if (nearCache.cacheFullButEvictionDisabled(null)) {
//            super.getAll0(modifiableKeyCollection, result);
//            return;
//        }

        FutureSupplier supplier = newGetAllFutureSupplier(ncKeys, param, startTimeNanos);
        nearCacheRecordStore.getAllOrFetch(ncKeys, supplier, result);
    }

    @Nonnull
    protected FutureSupplier newGetAllFutureSupplier(Collection ncKeys, final Data param, final long startTimeNanos) {
        FutureSupplier futureSupplier = new FutureSupplier() {
            @Override
            protected ICompletableFuture supply() {
                LinkedList<Data> dataKeys = new LinkedList<Data>();
                Map<Object, NearCacheRecord> recordsByNcKey = getRecordsByNcKey();
                ConcurrentMap<Object, Data> dataKeyByNcKey = getDataKeyByNcKey();
                for (Object ncKey : recordsByNcKey.keySet()) {
                    Data dataKey = dataKeyByNcKey.get(ncKey);
                    dataKeys.add(dataKey);
                }
                InternalCompletableFuture future = nearCacheDataFetcher.invokeGetAllOperation(dataKeys,
                        param, startTimeNanos);
                return new GetAllMemoDelegatingFuture(this, future, ss);
            }
        };

        if (serializeKeys) {
            for (Object ncKey : ncKeys) {
                futureSupplier.mapNcKeyToDataKey(ncKey, (Data) ncKey);
            }
        } else {
            toDataKeys(ncKeys, futureSupplier);
        }

        return futureSupplier;
    }

    // keep this object either object or data form can be passed.
    @Override
    public void invalidate(Object key) {
        Object ncKey = toNearCacheKey(key);
        nearCacheRecordStore.invalidate(ncKey);
    }

    @Override
    public void clear() {
        nearCacheRecordStore.clear();
    }

    @Override
    public void destroy() {
        if (expirationTaskFuture != null) {
            expirationTaskFuture.cancel(true);
        }
        nearCacheRecordStore.destroy();
    }

    @Override
    public NearCacheStats getNearCacheStats() {
        return nearCacheRecordStore.getNearCacheStats();
    }

    @Override
    public boolean isSerializeKeys() {
        return serializeKeys;
    }

    @Override
    public int size() {
        return nearCacheRecordStore.size();
    }

    @Override
    public void preload(DataStructureAdapter<Object, ?> adapter) {
        nearCacheRecordStore.loadKeys(adapter);
        preloadDone = true;
    }

    @Override
    public void storeKeys() {
        // we don't store new keys, until the pre-loader is done
        if (preloadDone) {
            nearCacheRecordStore.storeKeys();
        }
    }

    @Override
    public boolean isPreloadDone() {
        return preloadDone;
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(getClass())) {
            return clazz.cast(this);
        }

        throw new IllegalArgumentException("Unwrapping to " + clazz + " is not supported by this implementation");
    }

    public NearCacheRecordStore getNearCacheRecordStore() {
        return nearCacheRecordStore;
    }

    private void checkKeyFormat(K key) {
        if (!serializeKeys) {
            checkNotInstanceOf(Data.class, key, "key cannot be of type Data!");
        }
    }

    private Collection<Data> toDataKeys(Collection ncKeys, FutureSupplier futureSupplier) {
        Collection<Data> dataKeys = new LinkedList<Data>();
        for (Object ncKey : ncKeys) {
            Data dataKey = toDataWithStrategy(ncKey);
            dataKeys.add(dataKey);

            futureSupplier.mapNcKeyToDataKey(ncKey, dataKey);
        }
        return dataKeys;
    }

    public void setNearCacheDataFetcher(NearCacheDataFetcher nearCacheDataFetcher) {
        this.nearCacheDataFetcher = nearCacheDataFetcher;
    }

    private class ExpirationTask implements Runnable {

        private final AtomicBoolean expirationInProgress = new AtomicBoolean(false);

        @Override
        public void run() {
            if (expirationInProgress.compareAndSet(false, true)) {
                try {
                    nearCacheRecordStore.doExpiration();
                } finally {
                    expirationInProgress.set(false);
                }
            }
        }

        private ScheduledFuture schedule(TaskScheduler scheduler) {
            return scheduler.scheduleWithRepetition(this,
                    properties.getInteger(TASK_INITIAL_DELAY_SECONDS),
                    properties.getInteger(TASK_PERIOD_SECONDS),
                    TimeUnit.SECONDS);
        }
    }
}
