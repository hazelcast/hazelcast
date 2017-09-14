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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.util.ConstructorFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.map.impl.querycache.subscriber.NullQueryCache.NULL_QUERY_CACHE;

/**
 * Provides construction of whole {@link com.hazelcast.map.QueryCache QueryCache}
 * sub-system. As a result of that construction, we can have a ready to use {@link com.hazelcast.map.QueryCache QueryCache}.
 */
public class QueryCacheEndToEndProvider<K, V> {

    private static final int MUTEX_COUNT = 16;
    private static final int MASK = MUTEX_COUNT - 1;

    private final Object[] mutexes;
    private final Map<String, Map<String, InternalQueryCache<K, V>>> mapNameToQueryCaches;

    public QueryCacheEndToEndProvider() {
        mutexes = createMutexes();
        mapNameToQueryCaches = new HashMap<String, Map<String, InternalQueryCache<K, V>>>();
    }

    public InternalQueryCache<K, V> getOrCreateQueryCache(String mapName, String cacheId,
                                                          ConstructorFunction<String, InternalQueryCache<K, V>> constructor) {
        synchronized (getMutex(mapName)) {
            Map<String, InternalQueryCache<K, V>> cacheIdToQueryCache = mapNameToQueryCaches.get(mapName);
            if (cacheIdToQueryCache == null) {
                cacheIdToQueryCache = new ConcurrentHashMap<String, InternalQueryCache<K, V>>();
                mapNameToQueryCaches.put(mapName, cacheIdToQueryCache);
            }

            InternalQueryCache<K, V> queryCache = cacheIdToQueryCache.get(cacheId);
            if (queryCache == null) {
                queryCache = constructor.createNew(cacheId);
                if (queryCache == NULL_QUERY_CACHE) {
                    queryCache = null;
                } else {
                    cacheIdToQueryCache.put(cacheId, queryCache);
                }
            }

            return queryCache;
        }
    }

    public void removeSingleQueryCache(String mapName, String cacheId) {
        synchronized (getMutex(mapName)) {
            Map<String, InternalQueryCache<K, V>> cacheIdToQueryCache = mapNameToQueryCaches.get(mapName);
            if (cacheIdToQueryCache != null) {
                cacheIdToQueryCache.remove(cacheId);
            }
        }
    }

    public void destroyAllQueryCaches(String mapName) {
        synchronized (getMutex(mapName)) {
            Map<String, InternalQueryCache<K, V>> cacheIdToQueryCache = mapNameToQueryCaches.remove(mapName);
            if (cacheIdToQueryCache != null) {
                for (InternalQueryCache<K, V> queryCache : cacheIdToQueryCache.values()) {
                    queryCache.destroy();
                }
            }
        }
    }

    // only used in tests
    public int getQueryCacheCount(String mapName) {
        synchronized (getMutex(mapName)) {
            Map<String, InternalQueryCache<K, V>> cacheIdToQueryCache = mapNameToQueryCaches.get(mapName);
            if (cacheIdToQueryCache == null) {
                return 0;
            }
            return cacheIdToQueryCache.size();
        }
    }

    private Object[] createMutexes() {
        Object[] mutexes = new Object[MUTEX_COUNT];
        for (int i = 0; i < MUTEX_COUNT; i++) {
            mutexes[i] = new Object();
        }
        return mutexes;
    }

    private Object getMutex(String name) {
        int hashCode = name.hashCode();
        if (hashCode == Integer.MIN_VALUE) {
            hashCode = 0;
        }
        hashCode = Math.abs(hashCode);
        return mutexes[hashCode & MASK];
    }
}
