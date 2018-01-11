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

package com.hazelcast.internal.nearcache.impl;

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.NearCachePreloaderConfig;
import com.hazelcast.internal.adapter.DataStructureAdapter;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.spi.TaskScheduler;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Collection;
import java.util.HashSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DefaultNearCacheManager implements NearCacheManager {

    protected final SerializationService serializationService;
    protected final TaskScheduler scheduler;
    protected final ClassLoader classLoader;

    private final Queue<ScheduledFuture> preloadTaskFutures = new ConcurrentLinkedQueue<ScheduledFuture>();
    private final ConcurrentMap<String, NearCache> nearCacheMap = new ConcurrentHashMap<String, NearCache>();
    private final Object mutex = new Object();

    private volatile ScheduledFuture storageTaskFuture;

    public DefaultNearCacheManager(SerializationService ss, TaskScheduler es, ClassLoader classLoader) {
        assert ss != null;
        assert es != null;

        this.serializationService = ss;
        this.scheduler = es;
        this.classLoader = classLoader;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> NearCache<K, V> getNearCache(String name) {
        return nearCacheMap.get(name);
    }

    @Override
    public <K, V> NearCache<K, V> getOrCreateNearCache(String name, NearCacheConfig nearCacheConfig) {
        return getOrCreateNearCache(name, nearCacheConfig, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> NearCache<K, V> getOrCreateNearCache(String name,
                                                       NearCacheConfig nearCacheConfig,
                                                       DataStructureAdapter dataStructureAdapter) {
        NearCache<K, V> nearCache = nearCacheMap.get(name);
        if (nearCache == null) {
            synchronized (mutex) {
                nearCache = nearCacheMap.get(name);
                if (nearCache == null) {
                    nearCache = createNearCache(name, nearCacheConfig);
                    nearCache.initialize();

                    nearCacheMap.put(name, nearCache);

                    if (nearCache.getPreloaderConfig().isEnabled()) {
                        createAndSchedulePreloadTask(nearCache, dataStructureAdapter);
                        createAndScheduleStorageTask();
                    }
                }
            }
        }
        return nearCache;
    }

    protected <K, V> NearCache<K, V> createNearCache(String name, NearCacheConfig nearCacheConfig) {
        return new DefaultNearCache<K, V>(name, nearCacheConfig, serializationService, scheduler, classLoader);
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
        for (NearCache nearCache : new HashSet<NearCache>(nearCacheMap.values())) {
            destroyNearCache(nearCache.getName());
        }
        for (ScheduledFuture preloadTaskFuture : preloadTaskFutures) {
            preloadTaskFuture.cancel(true);
        }

        if (storageTaskFuture != null) {
            storageTaskFuture.cancel(true);
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
        if (storageTaskFuture == null) {
            StorageTask storageTask = new StorageTask();
            storageTaskFuture = scheduler.scheduleWithRepetition(storageTask, 0, 1, SECONDS);
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
            NearCacheStats nearCacheStats = nearCache.getNearCacheStats();
            NearCachePreloaderConfig preloaderConfig = nearCache.getPreloaderConfig();
            if (nearCacheStats.getLastPersistenceTime() == 0) {
                // check initial delay seconds for first persistence
                long runningSeconds = MILLISECONDS.toSeconds(now - started);
                if (runningSeconds < preloaderConfig.getStoreInitialDelaySeconds()) {
                    return false;
                }
            } else {
                // check interval seconds for all other persistences
                long elapsedSeconds = MILLISECONDS.toSeconds(now - nearCacheStats.getLastPersistenceTime());
                if (elapsedSeconds < preloaderConfig.getStoreIntervalSeconds()) {
                    return false;
                }
            }
            return true;
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
