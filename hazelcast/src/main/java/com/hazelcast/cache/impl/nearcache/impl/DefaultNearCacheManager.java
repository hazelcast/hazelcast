/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.nearcache.impl;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.config.NearCacheConfig;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultNearCacheManager implements NearCacheManager {

    private final ConcurrentMap<String, NearCache> nearCacheMap =
            new ConcurrentHashMap<String, NearCache>();

    private final Object mutex = new Object();

    @Override
    public <K, V> NearCache<K, V> getNearCache(String name) {
        return nearCacheMap.get(name);
    }

    @Override
    public <K, V> NearCache<K, V> getOrCreateNearCache(String name, NearCacheConfig nearCacheConfig,
                                                       NearCacheContext nearCacheContext) {
        NearCache<K, V> nearCache = nearCacheMap.get(name);
        if (nearCache == null) {
            synchronized (mutex) {
                nearCache = nearCacheMap.get(name);
                if (nearCache == null) {
                    nearCache = createNearCache(name, nearCacheConfig, nearCacheContext);
                    nearCacheMap.put(name, nearCache);
                }
            }
        }
        return nearCache;
    }

    protected <K, V> NearCache<K, V> createNearCache(String name, NearCacheConfig nearCacheConfig,
                                                     NearCacheContext nearCacheContext) {
        return new DefaultNearCache<K, V>(name, nearCacheConfig, nearCacheContext);
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
