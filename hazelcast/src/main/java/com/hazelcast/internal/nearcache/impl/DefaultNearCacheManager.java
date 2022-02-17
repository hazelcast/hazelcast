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

package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCacheConfigAccessor;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Collection;
import java.util.HashSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DefaultNearCacheManager implements NearCacheManager {

    protected final TaskScheduler scheduler;
    protected final ClassLoader classLoader;
    protected final HazelcastProperties properties;
    protected final SerializationService serializationService;

    private final Object mutex = new Object();
    private final Queue<ScheduledFuture> preloadTaskFutures = new ConcurrentLinkedQueue<>();
    private final ConcurrentMap<String, NearCache> nearCacheMap = new ConcurrentHashMap<>();
    private final AtomicReference<Object> storageTaskFutureRef = new AtomicReference<>();

    public DefaultNearCacheManager(SerializationService ss, TaskScheduler es,
                                   ClassLoader classLoader, HazelcastProperties properties) {
        assert ss != null;
        assert es != null;

        this.serializationService = ss;
        this.scheduler = es;
        this.classLoader = classLoader;
        this.properties = properties;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> NearCache<K, V> getNearCache(String name) {
        return nearCacheMap.get(name);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> NearCache<K, V> getOrCreateNearCache(String name,
                                                       NearCacheConfig nearCacheConfig) {
        NearCache<K, V> nearCache = nearCacheMap.get(name);
        if (nearCache != null) {
            return nearCache;
        }

        synchronized (mutex) {
            nearCache = nearCacheMap.get(name);
            if (nearCache == null) {
                nearCache = createNearCache(name, nearCacheConfig);
                nearCache.initialize();

                nearCacheMap.put(name, nearCache);
            }
        }
        return nearCache;
    }

    protected <K, V> NearCache<K, V> createNearCache(String name, NearCacheConfig nearCacheConfig) {
        NearCacheConfig copy = NearCacheConfigAccessor.copyWithInitializedDefaultMaxSizeForOnHeapMaps(nearCacheConfig);
        return new DefaultNearCache<>(name, copy, serializationService,
                scheduler, classLoader, properties);
    }

    @Override
    public void startPreloading(NearCache nearCache, DataStructureAdapter dataStructureAdapter) {
        NearCacheConfig nearCacheConfig = nearCache.getNearCacheConfig();
        NearCachePreloaderConfig preloaderConfig = nearCacheConfig.getPreloaderConfig();
        if (preloaderConfig.isEnabled()) {
            createAndSchedulePreloadTask(nearCache, dataStructureAdapter);
            createAndScheduleStorageTask();
        }
    }

    @Override
    public Collection<NearCache> listAllNearCaches() {
        return nearCacheMap.values();
    }

    @Override
    public boolean clearNearCache(String name) {
        NearCache nearCache = nearCacheMap.get(name);
        if (nearCache != null) {
            nearCache.clear();
        }
        return nearCache != null;
    }

    @Override
    public void clearAllNearCaches() {
        for (NearCache nearCache : nearCacheMap.values()) {
            nearCache.clear();
        }
    }

    @Override
    public boolean destroyNearCache(String name) {
        NearCache nearCache = nearCacheMap.get(name);
        if (nearCache != null) {
            synchronized (mutex) {
                nearCache = nearCacheMap.remove(name);
                if (nearCache != null) {
                    nearCache.destroy();
                    return true;
                }
                return false;
            }
        }
        return false;
    }

    @Override
    public void destroyAllNearCaches() {
        for (NearCache nearCache : new HashSet<>(nearCacheMap.values())) {
            destroyNearCache(nearCache.getName());
        }

        for (ScheduledFuture preloadTaskFuture : preloadTaskFutures) {
            preloadTaskFuture.cancel(true);
        }

        Object future = storageTaskFutureRef.get();
        if (future != null) {
            ((ScheduledFuture) future).cancel(true);
        }
    }

    private void createAndSchedulePreloadTask(NearCache nearCache, DataStructureAdapter adapter) {
        if (adapter != null) {
            PreloadTask preloadTask = new PreloadTask(nearCache, adapter);
            ScheduledFuture<?> scheduledFuture = scheduler.schedule(preloadTask, 3, SECONDS);
            preloadTask.scheduledFuture = scheduledFuture;

            preloadTaskFutures.add(scheduledFuture);
        }
    }

    private void createAndScheduleStorageTask() {
        if (storageTaskFutureRef.compareAndSet(null, this)) {
            storageTaskFutureRef.set(scheduler.scheduleWithRepetition(new StorageTask(),
                    0, 1, SECONDS));
        }
    }

    private class PreloadTask implements Runnable {

        private final NearCache nearCache;
        private final DataStructureAdapter adapter;

        private volatile ScheduledFuture<?> scheduledFuture;

        PreloadTask(NearCache nearCache, DataStructureAdapter adapter) {
            this.nearCache = nearCache;
            this.adapter = adapter;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            nearCache.preload(adapter);
            ScheduledFuture<?> future = scheduledFuture;
            if (future != null) {
                preloadTaskFutures.remove(future);
            }
        }
    }

    private class StorageTask implements Runnable {

        private final long started = System.currentTimeMillis();

        @Override
        public void run() {
            long now = System.currentTimeMillis();
            for (NearCache nearCache : nearCacheMap.values()) {
                if (!isScheduled(nearCache, now)) {
                    continue;
                }
                nearCache.storeKeys();
            }
        }

        private boolean isScheduled(NearCache nearCache, long now) {
            NearCachePreloaderConfig preloaderConfig = nearCache.getNearCacheConfig().getPreloaderConfig();
            NearCacheStats nearCacheStats = nearCache.getNearCacheStats();

            if (nearCacheStats.getLastPersistenceTime() == 0) {
                // check initial delay seconds for first persistence
                long runningSeconds = MILLISECONDS.toSeconds(now - started);
                return runningSeconds >= preloaderConfig.getStoreInitialDelaySeconds();
            } else {
                // check interval seconds for all other persistence
                long elapsedSeconds = MILLISECONDS.toSeconds(now - nearCacheStats.getLastPersistenceTime());
                return elapsedSeconds >= preloaderConfig.getStoreIntervalSeconds();
            }
        }
    }

    protected SerializationService getSerializationService() {
        return serializationService;
    }

    protected TaskScheduler getScheduler() {
        return scheduler;
    }

    protected ClassLoader getClassLoader() {
        return classLoader;
    }
}
