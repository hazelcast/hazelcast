/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import com.hazelcast.mapreduce.impl.HashMapAdapter;
import com.hazelcast.util.collection.Int2ObjectHashMap;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Utility class for Maps
 */
public final class MapUtil {

    private static final float HASHMAP_DEFAULT_LOAD_FACTOR = 0.75f;

    private MapUtil() { }

    /**
     * Utility method that creates an {@link java.util.HashMap} with its initialCapacity calculated
     * to minimize rehash operations
     */
    public static <K, V> Map<K, V> createHashMap(int expectedMapSize) {
        final int initialCapacity = calculateInitialCapacity(expectedMapSize);
        return new HashMap<K, V>(initialCapacity, HASHMAP_DEFAULT_LOAD_FACTOR);
    }

    /**
     * Utility method that creates an {@link HashMapAdapter} with its initialCapacity calculated
     * to minimize rehash operations
     */
    public static <K, V> Map<K, V> createHashMapAdapter(int expectedMapSize) {
        final int initialCapacity = calculateInitialCapacity(expectedMapSize);
        return new HashMapAdapter<K, V>(initialCapacity, HASHMAP_DEFAULT_LOAD_FACTOR);
    }

    /**
     * Utility method that creates an {@link java.util.LinkedHashMap} with its initialCapacity calculated
     * to minimize rehash operations
     */
    public static <K, V> Map<K, V> createLinkedHashMap(int expectedMapSize) {
        final int initialCapacity = calculateInitialCapacity(expectedMapSize);
        return new LinkedHashMap<K, V>(initialCapacity, HASHMAP_DEFAULT_LOAD_FACTOR);
    }

    /**
     * Utility method that creates an {@link java.util.LinkedHashMap} with its initialCapacity calculated
     * to minimize rehash operations
     */
    public static <K, V> ConcurrentMap<K, V> createConcurrentHashMap(int expectedMapSize) {
        //concurrent hash map will size itself to accomodate this many elements
        return new ConcurrentHashMap<K, V>(expectedMapSize);
    }

    /**
     * Utility method that creates an {@link Int2ObjectHashMap} with its initialCapacity calculated
     * to minimize rehash operations
     */
    public static <V> Int2ObjectHashMap<V> createInt2ObjectHashMap(int expectedMapSize) {
        final int initialCapacity = (int) (expectedMapSize / Int2ObjectHashMap.DEFAULT_LOAD_FACTOR) + 1;
        return new Int2ObjectHashMap<V>(initialCapacity, Int2ObjectHashMap.DEFAULT_LOAD_FACTOR);
    }

    /**
     * Returns the initial hash map capacity needed for the expected map size.
     * To avoid resizing the map, the initial capacity should be different than
     * the expected size, depending on the load factor.
     *
     * @param expectedMapSize the expected map size
     * @return the necessary initial capacity
     * @see HashMap
     */
    public static int calculateInitialCapacity(int expectedMapSize) {
        return (int) (expectedMapSize / HASHMAP_DEFAULT_LOAD_FACTOR) + 1;
    }

    /**
     * Test the given map and return {@code true} if the map is null or empty.
     * @param map the map to test
     * @return    {@code true} if {@code map} is null or empty, otherwise {@code false}.
     */
    public static boolean isNullOrEmpty(Map map) {
        return map == null || map.isEmpty();
    }

    /**
     * Converts <code>long</code> map size to <code>int</code>.
     * If <code>size</code> is greater than <code>Integer.MAX_VALUE</code>
     * then <code>Integer.MAX_VALUE</code> is returned.
     *
     * @param size map size
     * @return map size in <code>int</code> type
     */
    public static int toIntSize(long size) {
        assert size >= 0 : "Invalid size value: " + size;
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) size;
    }
}
