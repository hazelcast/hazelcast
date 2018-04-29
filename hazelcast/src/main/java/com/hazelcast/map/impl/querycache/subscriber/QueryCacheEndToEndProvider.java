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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ContextMutexFactory;
import com.hazelcast.util.UuidUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.map.impl.querycache.subscriber.NullQueryCache.NULL_QUERY_CACHE;
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * Provides construction of whole {@link com.hazelcast.map.QueryCache QueryCache}
 * sub-system. As a result of that construction, we can have a ready to use {@link com.hazelcast.map.QueryCache QueryCache}.
 */
public class QueryCacheEndToEndProvider<K, V> {

    private final ContextMutexFactory mutexFactory;
    private final ConcurrentMap<String, ConcurrentMap<String, InternalQueryCache<K, V>>> queryCacheRegistryPerMap;
    private final ConstructorFunction<String, ConcurrentMap<String, InternalQueryCache<K, V>>> queryCacheRegistryConstructor
            = new ConstructorFunction<String, ConcurrentMap<String, InternalQueryCache<K, V>>>() {
        @Override
        public ConcurrentMap<String, InternalQueryCache<K, V>> createNew(String arg) {
            return new ConcurrentHashMap<String, InternalQueryCache<K, V>>();
        }
    };

    public QueryCacheEndToEndProvider(ContextMutexFactory mutexFactory) {
        this.mutexFactory = mutexFactory;
        this.queryCacheRegistryPerMap = new ConcurrentHashMap<String, ConcurrentMap<String, InternalQueryCache<K, V>>>();
    }

    public InternalQueryCache<K, V> getOrCreateQueryCache(String mapName, String cacheName,
                                                          ConstructorFunction<String, InternalQueryCache<K, V>> constructor) {

        InternalQueryCache<K, V> existingQueryCache = getExistingQueryCacheOrNull(mapName, cacheName);
        if (existingQueryCache != null) {
            return existingQueryCache;
        }

        ContextMutexFactory.Mutex mutex = mutexFactory.mutexFor(mapName);
        try {
            synchronized (mutex) {
                ConcurrentMap<String, InternalQueryCache<K, V>> queryCacheRegistry
                        = getOrPutIfAbsent(queryCacheRegistryPerMap, mapName, queryCacheRegistryConstructor);

                InternalQueryCache<K, V> queryCache = queryCacheRegistry.get(cacheName);
                if (queryCache != null) {
                    return queryCache;
                }

                String cacheId = UuidUtil.newUnsecureUuidString();
                queryCache = constructor.createNew(cacheId);
                if (queryCache != NULL_QUERY_CACHE) {
                    queryCacheRegistry.put(cacheName, queryCache);
                    return queryCache;
                }

                return null;
            }
        } finally {
            closeResource(mutex);
        }
    }

    private InternalQueryCache<K, V> getExistingQueryCacheOrNull(String mapName, String cacheName) {
        ConcurrentMap<String, InternalQueryCache<K, V>> queryCacheRegistry = queryCacheRegistryPerMap.get(mapName);
        if (queryCacheRegistry != null) {
            InternalQueryCache<K, V> queryCache = queryCacheRegistry.get(cacheName);
            if (queryCache != null) {
                return queryCache;
            }
        }
        return null;
    }

    public void removeSingleQueryCache(String mapName, String cacheName) {
        ContextMutexFactory.Mutex mutex = mutexFactory.mutexFor(mapName);
        try {
            synchronized (mutex) {
                Map<String, InternalQueryCache<K, V>> queryCacheRegistry = queryCacheRegistryPerMap.get(mapName);
                if (queryCacheRegistry != null) {
                    queryCacheRegistry.remove(cacheName);
                }
            }
        } finally {
            closeResource(mutex);
        }
    }

    public void destroyAllQueryCaches(String mapName) {
        ContextMutexFactory.Mutex mutex = mutexFactory.mutexFor(mapName);
        try {
            synchronized (mutex) {
                Map<String, InternalQueryCache<K, V>> queryCacheRegistry = queryCacheRegistryPerMap.remove(mapName);
                if (queryCacheRegistry != null) {
                    for (InternalQueryCache<K, V> queryCache : queryCacheRegistry.values()) {
                        queryCache.destroy();
                    }
                }
            }
        } finally {
            closeResource(mutex);
        }
    }

    // only used in tests
    public int getQueryCacheCount(String mapName) {
        Map<String, InternalQueryCache<K, V>> queryCacheRegistry = queryCacheRegistryPerMap.get(mapName);
        if (queryCacheRegistry == null) {
            return 0;
        }
        return queryCacheRegistry.size();
    }
}
