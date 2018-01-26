/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.nearcache.impl.store.NearCacheDataRecordStore;
import com.hazelcast.internal.nearcache.impl.store.NearCacheObjectRecordStore;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.config.NearCacheConfig.DEFAULT_MEMORY_FORMAT;
import static com.hazelcast.util.Preconditions.checkNotInstanceOf;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class DefaultNearCache<K, V> implements NearCache<K, V> {

    protected final String name;
    protected final NearCacheConfig nearCacheConfig;
    protected final SerializationService serializationService;
    protected final TaskScheduler scheduler;
    protected final ClassLoader classLoader;

    protected NearCacheRecordStore<K, V> nearCacheRecordStore;
    protected ScheduledFuture expirationTaskFuture;

    private final boolean serializeKeys;

    private volatile boolean preloadDone;

    public DefaultNearCache(String name, NearCacheConfig nearCacheConfig,
                            SerializationService serializationService, TaskScheduler scheduler,
                            ClassLoader classLoader) {
        this(name, nearCacheConfig, null, serializationService, scheduler, classLoader);
    }

    public DefaultNearCache(String name, NearCacheConfig nearCacheConfig, NearCacheRecordStore<K, V> nearCacheRecordStore,
                            SerializationService serializationService, TaskScheduler scheduler,
                            ClassLoader classLoader) {
        this.name = name;
        this.nearCacheConfig = nearCacheConfig;
        this.serializationService = serializationService;
        this.classLoader = classLoader;
        this.scheduler = scheduler;
        this.nearCacheRecordStore = nearCacheRecordStore;
        this.serializeKeys = nearCacheConfig.isSerializeKeys();
    }

    @Override
    public void initialize() {
        if (nearCacheRecordStore == null) {
            nearCacheRecordStore = createNearCacheRecordStore(name, nearCacheConfig);
        }
        nearCacheRecordStore.initialize();

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
            return expirationTask.schedule(scheduler);
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
        checkKeyFormat(key);

        return nearCacheRecordStore.get(key);
    }

    @Override
    public void put(K key, Data keyData, V value) {
        checkNotNull(key, "key cannot be null on put!");
        checkKeyFormat(key);

        nearCacheRecordStore.doEvictionIfRequired();

        nearCacheRecordStore.put(key, keyData, value);
    }

    @Override
    public boolean remove(K key) {
        checkNotNull(key, "key cannot be null on remove!");
        checkKeyFormat(key);

        return nearCacheRecordStore.remove(key);
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
    public boolean isSerializeKeys() {
        return serializeKeys;
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

    @Override
    public long tryReserveForUpdate(K key, Data keyData) {
        nearCacheRecordStore.doEvictionIfRequired();

        return nearCacheRecordStore.tryReserveForUpdate(key, keyData);
    }

    @Override
    public V tryPublishReserved(K key, V value, long reservationId, boolean deserialize) {
        return nearCacheRecordStore.tryPublishReserved(key, value, reservationId, deserialize);
    }

    public NearCacheRecordStore<K, V> getNearCacheRecordStore() {
        return nearCacheRecordStore;
    }

    private void checkKeyFormat(K key) {
        if (!serializeKeys) {
            checkNotInstanceOf(Data.class, key, "key cannot be of type Data!");
        }
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
                    DEFAULT_EXPIRATION_TASK_INITIAL_DELAY_IN_SECONDS,
                    DEFAULT_EXPIRATION_TASK_DELAY_IN_SECONDS,
                    TimeUnit.SECONDS);
        }
    }
}
