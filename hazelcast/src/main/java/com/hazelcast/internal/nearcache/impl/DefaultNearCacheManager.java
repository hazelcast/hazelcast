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

import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultNearCacheManager implements NearCacheManager {

    protected final SerializationService serializationService;
    protected final ExecutionService executionService;
    protected final ClassLoader classLoader;

    private final ConcurrentMap<String, NearCache> nearCacheMap = new ConcurrentHashMap<String, NearCache>();
    private final Object mutex = new Object();

    public DefaultNearCacheManager(SerializationService ss, ExecutionService es, ClassLoader classLoader) {
        assert ss != null;
        assert es != null;

        this.serializationService = ss;
        this.executionService = es;
        this.classLoader = classLoader;
    }

    @Override
    public <K, V> NearCache<K, V> getNearCache(String name) {
        return nearCacheMap.get(name);
    }

    @Override
    public <K, V> NearCache<K, V> getOrCreateNearCache(String name, NearCacheConfig nearCacheConfig) {
        NearCache<K, V> nearCache = nearCacheMap.get(name);
        if (nearCache == null) {
            synchronized (mutex) {
                nearCache = nearCacheMap.get(name);
                if (nearCache == null) {
                    nearCache = createNearCache(name, nearCacheConfig);
                    nearCache.initialize();

                    nearCacheMap.put(name, nearCache);
                }
            }
        }
        return nearCache;
    }

    protected <K, V> NearCache<K, V> createNearCache(String name, NearCacheConfig nearCacheConfig) {
        return new DefaultNearCache<K, V>(name, nearCacheConfig, serializationService, executionService, classLoader);
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
        NearCache nearCache = nearCacheMap.remove(name);
        if (nearCache != null) {
            nearCache.destroy();
        }
        return nearCache != null;
    }

    @Override
    public void destroyAllNearCaches() {
        for (NearCache nearCache : new HashSet<NearCache>(nearCacheMap.values())) {
            nearCacheMap.remove(nearCache.getName());
            nearCache.destroy();
        }
    }
}
