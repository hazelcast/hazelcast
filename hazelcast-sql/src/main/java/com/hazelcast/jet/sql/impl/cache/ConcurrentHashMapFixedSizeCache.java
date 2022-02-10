/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.jet.sql.impl.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of fixed size cache based on {@link ConcurrentHashMap}. The final size of the inner map may be bigger
 * than maxCapacity if there are multiple concurrent computeIfAbsent executions.
 * <p>
 * The cache has no eviction policy, once the element is putted into the cache, it stays there as long as the cache
 * exists.
 */
public class ConcurrentHashMapFixedSizeCache<K, V> implements Cache<K, V> {
    final Map<K, V> cache;

    private final int maxCapacity;

    public ConcurrentHashMapFixedSizeCache(int maxCapacity) {
        if (maxCapacity <= 0) {
            throw new IllegalArgumentException("maxCapacity <= 0");
        }

        this.maxCapacity = maxCapacity;
        this.cache = new ConcurrentHashMap<>(maxCapacity);
    }

    @Override
    public V computeIfAbsent(K key, Function<K, V> valueFunction) {
        V valueFromCache = cache.get(key);
        if (valueFromCache == null) {
            V calculatedValue = valueFunction.apply(key);
            if (cache.size() < maxCapacity) {
                cache.put(key, calculatedValue);
            }
            return calculatedValue;
        }
        return valueFromCache;
    }
}
