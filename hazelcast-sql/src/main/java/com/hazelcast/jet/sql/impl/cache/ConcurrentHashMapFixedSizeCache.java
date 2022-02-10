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
