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
import com.hazelcast.util.ContextMutexFactory;

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
    private final ConcurrentMap<String, ConcurrentMap<String, InternalQueryCache<K, V>>> mapNameToQueryCaches;
    private final ConstructorFunction<String, ConcurrentMap<String, InternalQueryCache<K, V>>> ctor
            = new ConstructorFunction<String, ConcurrentMap<String, InternalQueryCache<K, V>>>() {
        @Override
        public ConcurrentMap<String, InternalQueryCache<K, V>> createNew(String arg) {
            return new ConcurrentHashMap<String, InternalQueryCache<K, V>>();
        }
    };

    public QueryCacheEndToEndProvider(ContextMutexFactory mutexFactory) {
        this.mutexFactory = mutexFactory;
        this.mapNameToQueryCaches = new ConcurrentHashMap<String, ConcurrentMap<String, InternalQueryCache<K, V>>>();
    }

    public InternalQueryCache<K, V> getOrCreateQueryCache(String mapName, String cacheId,
                                                          ConstructorFunction<String, InternalQueryCache<K, V>> constructor) {
        ContextMutexFactory.Mutex mutex = mutexFactory.mutexFor(mapName);
        try {
            synchronized (mutex) {
                ConcurrentMap<String, InternalQueryCache<K, V>> cacheIdToQueryCache
                        = getOrPutIfAbsent(mapNameToQueryCaches, mapName, ctor);
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
        } finally {
            closeResource(mutex);
        }
    }

    public void removeSingleQueryCache(String mapName, String cacheId) {
        ContextMutexFactory.Mutex mutex = mutexFactory.mutexFor(mapName);
        try {
            synchronized (mutex) {
                Map<String, InternalQueryCache<K, V>> cacheIdToQueryCache = mapNameToQueryCaches.get(mapName);
                if (cacheIdToQueryCache != null) {
                    cacheIdToQueryCache.remove(cacheId);
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
                Map<String, InternalQueryCache<K, V>> cacheIdToQueryCache = mapNameToQueryCaches.remove(mapName);
                if (cacheIdToQueryCache != null) {
                    for (InternalQueryCache<K, V> queryCache : cacheIdToQueryCache.values()) {
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
        Map<String, InternalQueryCache<K, V>> cacheIdToQueryCache = mapNameToQueryCaches.get(mapName);
        if (cacheIdToQueryCache == null) {
            return 0;
        }
        return cacheIdToQueryCache.size();
    }
}
