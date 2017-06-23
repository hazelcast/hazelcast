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

package com.hazelcast.spring.cache;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author mdogan 4/3/12
 */
public class HazelcastCacheManager implements CacheManager {

    private final ConcurrentMap<String, Cache> caches = new ConcurrentHashMap<String, Cache>();
    private HazelcastInstance hazelcastInstance;
    /**
     * Default cache value retrieval timeout. Apply to all caches.
     * Can be overridden setting a cache specific value to readTimeoutMap.
     */
    private long defaultReadTimeout;
    /**
     * Holds cache specific value retrieval timeouts. Override defaultReadTimeout for specified caches.
     */
    private Map<String, Long> readTimeoutMap;

    public HazelcastCacheManager() {
    }

    public HazelcastCacheManager(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public Cache getCache(String name) {
        Cache cache = caches.get(name);
        if (cache == null) {
            final IMap<Object, Object> map = hazelcastInstance.getMap(name);
            cache = new HazelcastCache(map);
            long cacheTimeout = calculateCacheReadTimeout(name);
            ((HazelcastCache) cache).setReadTimeout(cacheTimeout);
            final Cache currentCache = caches.putIfAbsent(name, cache);
            if (currentCache != null) {
                cache = currentCache;
            }
        }
        return cache;
    }

    @Override
    public Collection<String> getCacheNames() {
        Set<String> cacheNames = new HashSet<String>();
        final Collection<DistributedObject> distributedObjects = hazelcastInstance.getDistributedObjects();
        for (DistributedObject distributedObject : distributedObjects) {
            if (distributedObject instanceof IMap) {
                final IMap<?, ?> map = (IMap) distributedObject;
                cacheNames.add(map.getName());
            }
        }
        return cacheNames;
    }

    private long calculateCacheReadTimeout(String name) {
        long timeout = 0;
        if (getReadTimeoutMap().containsKey(name)) {
            Long v = getReadTimeoutMap().get(name);
            if (v != null) {
                timeout = v;
            }
        } else {
            timeout = defaultReadTimeout;
        }
        return timeout;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public void setHazelcastInstance(final HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    /**
     * Return default cache value retrieval timeout in milliseconds.
     */
    public long getDefaultReadTimeout() {
        return defaultReadTimeout;
    }

    /**
     * Set default cache value retrieval timeout. Applies to all caches, if not defined a cache specific timeout.
     * @param defaultReadTimeout default cache retrieval timeout in milliseconds. 0 or negative values disable timeout.
     */
    public void setDefaultReadTimeout(long defaultReadTimeout) {
        this.defaultReadTimeout = defaultReadTimeout;
    }

    /**
     * Return cache-specific value retrieval timeouts. Map keys are cache names, values are cache retrieval timeouts in
     * milliseconds.
     */
    public Map<String, Long> getReadTimeoutMap() {
        if (readTimeoutMap == null) {
            readTimeoutMap = Collections.emptyMap();
        }
        return readTimeoutMap;
    }

    /**
     * Set cache specific value retrieval timeouts
     * @param readTimeoutMap cache specific value retrieval timeouts. Map keys are cache names, values are cache
     *                       retrieval timeouts in milliseconds.
     */
    public void setReadTimeoutMap(Map<String, Long> readTimeoutMap) {
        this.readTimeoutMap = readTimeoutMap;
    }
}
