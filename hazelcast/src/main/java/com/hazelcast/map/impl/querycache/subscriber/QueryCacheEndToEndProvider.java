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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ContextMutexFactory;
import com.hazelcast.internal.util.UuidUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.map.impl.querycache.subscriber.NullQueryCache.NULL_QUERY_CACHE;

/**
 * Provides construction of whole {@link com.hazelcast.map.QueryCache
 * QueryCache} sub-system. As a result of that construction, we can
 * have a ready to use {@link com.hazelcast.map.QueryCache QueryCache}.
 */
public class QueryCacheEndToEndProvider<K, V> {

    private final ContextMutexFactory lifecycleMutexFactory;
    private final ConcurrentMap<String, ConcurrentMap<String, InternalQueryCache<K, V>>> queryCacheRegistryPerMap;
    private final ConstructorFunction<String, ConcurrentMap<String, InternalQueryCache<K, V>>> queryCacheRegistryConstructor
            = arg -> new ConcurrentHashMap<>();

    public QueryCacheEndToEndProvider(ContextMutexFactory lifecycleMutexFactory) {
        this.lifecycleMutexFactory = lifecycleMutexFactory;
        this.queryCacheRegistryPerMap = new ConcurrentHashMap<>();
    }

    public InternalQueryCache<K, V> getOrCreateQueryCache(String mapName, String cacheName,
                                                          ConstructorFunction<String, InternalQueryCache<K, V>> constructor) {

        InternalQueryCache<K, V> existingQueryCache = getExistingQueryCacheOrNull(mapName, cacheName);
        if (existingQueryCache != null) {
            return existingQueryCache;
        }

        return tryCreateQueryCache(mapName, cacheName, constructor);
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

    /**
     * Idempotent query cache create mechanism.
     */
    public InternalQueryCache<K, V> tryCreateQueryCache(String mapName, String cacheName,
                                                        ConstructorFunction<String, InternalQueryCache<K, V>> constructor) {

        ContextMutexFactory.Mutex mutex = lifecycleMutexFactory.mutexFor(mapName);
        try {
            synchronized (mutex) {
                ConcurrentMap<String, InternalQueryCache<K, V>> queryCacheRegistry
                        = getOrPutIfAbsent(queryCacheRegistryPerMap, mapName, queryCacheRegistryConstructor);

                InternalQueryCache<K, V> queryCache = queryCacheRegistry.get(cacheName);
                // if this is a recreation we expect to have a Uuid otherwise we
                // need to generate one for the first creation of query cache.
                String cacheId = queryCache != null
                        ? queryCache.getCacheId() : UuidUtil.newUnsecureUuidString();

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

    public void removeSingleQueryCache(String mapName, String cacheName) {
        ContextMutexFactory.Mutex mutex = lifecycleMutexFactory.mutexFor(mapName);
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
        ContextMutexFactory.Mutex mutex = lifecycleMutexFactory.mutexFor(mapName);
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
