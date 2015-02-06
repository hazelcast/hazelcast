package com.hazelcast.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for Maps
 */
public final class MapUtil {

    private static final double HASHMAP_DEFAULT_LOAD_FACTOR = 0.75;

    private MapUtil() { }

    /**
     * Utility method that creates an {@link java.util.HashMap} with its initialCapacity calculated
     * to minimize rehash operations
     */
    public static <K, V> Map<K, V> createHashMap(int expectedMapSize) {
        int initialCapacity = (int) (expectedMapSize / HASHMAP_DEFAULT_LOAD_FACTOR) + 1;
        return new HashMap<K, V>(initialCapacity);
    }

}
