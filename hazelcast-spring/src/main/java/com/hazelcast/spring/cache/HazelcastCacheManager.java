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

package com.hazelcast.spring.cache;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.springframework.util.Assert.isTrue;

/**
 * Sprint related {@link HazelcastCacheManager} implementation for Hazelcast.
 */
@SuppressWarnings("WeakerAccess")
public class HazelcastCacheManager implements CacheManager {

    /**
     * Property name for hazelcast spring-cache related properties.
     */
    public static final String CACHE_PROP = "hazelcast.spring.cache.prop";

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
    private Map<String, Long> readTimeoutMap = new HashMap<String, Long>();

    public HazelcastCacheManager() {
    }

    public HazelcastCacheManager(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public Cache getCache(String name) {
        Cache cache = caches.get(name);
        if (cache == null) {
            IMap<Object, Object> map = hazelcastInstance.getMap(name);
            cache = new HazelcastCache(map);
            long cacheTimeout = calculateCacheReadTimeout(name);
            ((HazelcastCache) cache).setReadTimeout(cacheTimeout);
            Cache currentCache = caches.putIfAbsent(name, cache);
            if (currentCache != null) {
                cache = currentCache;
            }
        }
        return cache;
    }

    @Override
    public Collection<String> getCacheNames() {
        Set<String> cacheNames = new HashSet<String>();
        Collection<DistributedObject> distributedObjects = hazelcastInstance.getDistributedObjects();
        for (DistributedObject distributedObject : distributedObjects) {
            if (distributedObject instanceof IMap) {
                IMap<?, ?> map = (IMap) distributedObject;
                cacheNames.add(map.getName());
            }
        }
        return cacheNames;
    }

    private long calculateCacheReadTimeout(String name) {
        Long timeout = getReadTimeoutMap().get(name);
        return timeout == null ? defaultReadTimeout : timeout;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    private void parseOptions(String options) {
        if (null == options || options.trim().isEmpty()) {
            return;
        }
        for (String option : options.split(",")) {
            parseOption(option);
        }
    }

    private void parseOption(String option) {
        String[] keyValue = option.split("=");
        isTrue(keyValue.length != 0, "blank key-value pair");
        isTrue(keyValue.length <= 2, String.format("key-value pair %s with more than one equals sign", option));

        String key = keyValue[0].trim();
        String value = (keyValue.length == 1) ? null : keyValue[1].trim();

        isTrue(value != null && !value.isEmpty(), String.format("value for %s must not be null or empty", key));

        if ("defaultReadTimeout".equals(key)) {
            defaultReadTimeout = Long.parseLong(value);
        } else {
            readTimeoutMap.put(key, Long.parseLong(value));
        }
    }

    /**
     * Return default cache value retrieval timeout in milliseconds.
     */
    public long getDefaultReadTimeout() {
        return defaultReadTimeout;
    }

    /**
     * Set default cache value retrieval timeout. Applies to all caches, if not defined a cache specific timeout.
     *
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
        return readTimeoutMap;
    }

    /**
     * Set the cache ead-timeout params
     *
     * @param options cache read-timeout params, auto-wired by Spring automatically by getting value of
     *                hazelcast.spring.cache.prop parameter
     */
    @Autowired
    public void setCacheOptions(@Value("${" + CACHE_PROP + ":}") String options) {
        parseOptions(options);
    }
}
