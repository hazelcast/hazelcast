/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.internal.util.Preconditions;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of fixed-capacity cache based on {@link ConcurrentHashMap}
 * caching the initial set of keys.
 * <p>
 * The cache has no eviction policy, once an element is put into it, it stays
 * there so long as the cache exists. Once the cache is full, no new items are
 * cached.
 * <p>
 * It's designed for caching in scenarios where we can assume that there's a low
 * number of keys that typically fit into the cache, and if not, that the keys
 * come in arbitrary order. If the number of keys is larger than capacity, we
 * assume that those that are more common are more likely to be observed at the
 * beginning than those that are not, and we're likely to cache those items. If
 * the number of expressions exceeds the capacity many times, we'll cache
 * arbitrary few of them and the rest will be recalculated each time without
 * caching - a similar behavior to what an LRU cache will provide, but without
 * the overhead of usage tracking. Degenerate case is when items are sorted by
 * the cache key - after the initial phase the cache will have zero hit rate.
 * <p>
 * The above assumptions are common for right-hand operand of SQL LIKE operator,
 * JsonPath, XPath or regular expression, in the context of a single query
 * execution and of a single operator evaluating them.
 * <p>
 * Note: The size of the inner map may become bigger than maxCapacity if there
 * are multiple concurrent computeIfAbsent executions. We don't address this for
 * the purpose of optimizing the read performance. The amount the size can
 * exceed the limit is bounded by the number of concurrent writers.
 */
public class ConcurrentInitialSetCache<K, V> {
    // package-visible for tests
    final Map<K, V> cache;
    private final int capacity;

    public ConcurrentInitialSetCache(int capacity) {
        Preconditions.checkPositive("capacity", capacity);
        this.capacity = capacity;
        this.cache = new ConcurrentHashMap<>(capacity);
    }

    public V computeIfAbsent(K key, Function<? super K, ? extends V> valueFunction) {
        V value = cache.get(key);
        if (value == null) {
            if (cache.size() < capacity) {
                // use CHM.computeIfAbsent to avoid duplicate calculation of a single key
                value = cache.computeIfAbsent(key, valueFunction);
            } else {
                value = valueFunction.apply(key);
            }
        }
        return value;
    }
}
