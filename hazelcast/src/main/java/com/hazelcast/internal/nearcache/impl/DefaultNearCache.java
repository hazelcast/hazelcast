/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheRecordStore;
import com.hazelcast.internal.nearcache.impl.invalidation.StaleReadWriteDetector;
import com.hazelcast.internal.nearcache.impl.store.NearCacheDataRecordStore;
import com.hazelcast.internal.nearcache.impl.store.NearCacheObjectRecordStore;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.UUID;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.NearCacheConfig.DEFAULT_MEMORY_FORMAT;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.Long.MAX_VALUE;

public class DefaultNearCache<K, V> implements NearCache<K, V> {

    protected final String name;
    protected final NearCacheConfig nearCacheConfig;
    protected final SerializationService serializationService;
    protected final ExecutionService executionService;
    protected final ClassLoader classLoader;

    protected NearCacheRecordStore<K, V> nearCacheRecordStore;
    protected ScheduledFuture expirationTaskFuture;

    private volatile boolean preloadDone;

    public DefaultNearCache(String name, NearCacheConfig nearCacheConfig,
                            SerializationService serializationService, ExecutionService executionService,
                            ClassLoader classLoader) {
        this(name, nearCacheConfig, null, serializationService, executionService, classLoader);
    }

    public DefaultNearCache(String name, NearCacheConfig nearCacheConfig, NearCacheRecordStore<K, V> nearCacheRecordStore,
                            SerializationService serializationService, ExecutionService executionService,
                            ClassLoader classLoader) {
        this.name = name;
        this.nearCacheConfig = nearCacheConfig;
        this.serializationService = serializationService;
        this.classLoader = classLoader;
        this.executionService = executionService;
        this.nearCacheRecordStore = nearCacheRecordStore;
    }

    @Override
    public void initialize() {
        if (nearCacheRecordStore == null) {
            nearCacheRecordStore = createNearCacheRecordStore(name, nearCacheConfig);
        }
        nearCacheRecordStore.initialize();

        expirationTaskFuture = createAndScheduleExpirationTask();
        expirationTaskFuture = createAndScheduleExpirationTask();
    }

    protected NearCacheRecordStore<K, V> createNearCacheRecordStore(String name, NearCacheConfig nearCacheConfig) {
        InMemoryFormat inMemoryFormat = nearCacheConfig.getInMemoryFormat();
        if (inMemoryFormat == null) {
            inMemoryFormat = DEFAULT_MEMORY_FORMAT;
        }
        switch (inMemoryFormat) {
            case BINARY:
                return new NearCacheDataRecordStore<K, V>(name, nearCacheConfig, serializationService, classLoader);
            case OBJECT:
                return new NearCacheObjectRecordStore<K, V>(name, nearCacheConfig, serializationService, classLoader);
            default:
                throw new IllegalArgumentException("Invalid in memory format: " + inMemoryFormat);
        }
    }

    private ScheduledFuture createAndScheduleExpirationTask() {
        if (nearCacheConfig.getMaxIdleSeconds() > 0L || nearCacheConfig.getTimeToLiveSeconds() > 0L) {
            ExpirationTask expirationTask = new ExpirationTask();
            return expirationTask.schedule(executionService);
        }
        return null;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public V get(K key) {
        checkNotNull(key, "key cannot be null on get!");

        return nearCacheRecordStore.get(key);
    }

    @Override
    public void put(K key, V value) {
        putIdentified(key, value, null, MAX_VALUE);
    }

    @Override
    public void putIdentified(K key, V value, UUID uuid, long sequence) {
        checkNotNull(key, "key cannot be null on put!");

        nearCacheRecordStore.doEvictionIfRequired();

        nearCacheRecordStore.putIdentified(key, value, uuid, sequence);
    }

    @Override
    public boolean remove(K key) {
        checkNotNull(key, "key cannot be null on remove!");

        return nearCacheRecordStore.remove(key);
    }

    @Override
    public boolean isInvalidatedOnChange() {
        return nearCacheConfig.isInvalidateOnChange();
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
    public InMemoryFormat getInMemoryFormat() {
        return nearCacheConfig.getInMemoryFormat();
    }

    @Override
    public NearCachePreloaderConfig getPreloaderConfig() {
        return nearCacheConfig.getPreloaderConfig();
    }

    @Override
    public NearCacheStats getNearCacheStats() {
        return nearCacheRecordStore.getNearCacheStats();
    }

    @Override
    public Object selectToSave(Object... candidates) {
        return nearCacheRecordStore.selectToSave(candidates);
    }

    @Override
    public int size() {
        return nearCacheRecordStore.size();
    }

    @Override
    public void preload(DataStructureAdapter<Data, ?> adapter) {
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

    public <T> T unwrap(Class<T> clazz) {
        if (clazz.isAssignableFrom(getClass())) {
            return clazz.cast(this);
        }
        throw new IllegalArgumentException("Unwrapping to " + clazz + " is not supported by this implementation");
    }

    public void setStaleReadWriteDetector(StaleReadWriteDetector staleReadWriteDetector) {
        nearCacheRecordStore.setStaleReadWriteDetector(staleReadWriteDetector);
    }

    public StaleReadWriteDetector getStaleReadWriteDetector() {
        return nearCacheRecordStore.getStaleReadWriteDetector();
    }

    public NearCacheRecordStore<K, V> getNearCacheRecordStore() {
        return nearCacheRecordStore;
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

        private ScheduledFuture schedule(ExecutionService executionService) {
            return executionService.scheduleWithRepetition(this,
                    DEFAULT_EXPIRATION_TASK_INITIAL_DELAY_IN_SECONDS,
                    DEFAULT_EXPIRATION_TASK_DELAY_IN_SECONDS,
                    TimeUnit.SECONDS);
        }
    }
}
