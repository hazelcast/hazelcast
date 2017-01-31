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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.map.impl.querycache.subscriber.NullQueryCache.NULL_QUERY_CACHE;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;

/**
 * Provides construction of whole {@link com.hazelcast.map.QueryCache QueryCache}
 * sub-system. As a result of that construction we can have a ready to use {@link com.hazelcast.map.QueryCache QueryCache}.
 */
public class QueryCacheEndToEndProvider<K, V> {

    private static final int MUTEX_COUNT = 16;
    private static final int MASK = MUTEX_COUNT - 1;

    private final ConstructorFunction<String, ConcurrentMap<String, InternalQueryCache<K, V>>> constructorFunction
            = new ConstructorFunction<String, ConcurrentMap<String, InternalQueryCache<K, V>>>() {
        @Override
        public ConcurrentMap<String, InternalQueryCache<K, V>> createNew(String arg) {
            return new ConcurrentHashMap<String, InternalQueryCache<K, V>>();
        }
    };

    private final Object[] mutexes;
    private final ConcurrentMap<String, ConcurrentMap<String, InternalQueryCache<K, V>>> queryCaches;

    public QueryCacheEndToEndProvider() {
        mutexes = createMutexes();
        queryCaches = new ConcurrentHashMap<String, ConcurrentMap<String, InternalQueryCache<K, V>>>();
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

    public InternalQueryCache<K, V> getOrCreateQueryCache(String mapName, String cacheName,
                                                          ConstructorFunction<String, InternalQueryCache<K, V>> constructor) {
        ConcurrentMap<String, InternalQueryCache<K, V>> queryCachesOfMap
                = getOrPutIfAbsent(queryCaches, mapName, constructorFunction);
        Object mutex = getMutex(cacheName);
        synchronized (mutex) {
            InternalQueryCache<K, V> cache = getOrPutSynchronized(queryCachesOfMap, cacheName, mutex, constructor);
            if (cache == NULL_QUERY_CACHE) {
                remove(mapName, cacheName);
                return null;
            }
            return cache;
        }
    }

    public InternalQueryCache<K, V> remove(String mapName, String cacheName) {
        ConcurrentMap<String, InternalQueryCache<K, V>> queryCachesOfMap = queryCaches.get(mapName);
        return queryCachesOfMap.remove(cacheName);
    }
}
