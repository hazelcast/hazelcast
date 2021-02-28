/*
 * Original work Copyright 2015 Real Logic Ltd.
 * Modified work Copyright (c) 2015-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.collection;

import java.util.function.Consumer;
import java.util.Map;

/**
 * Map that takes two part int key and associates with an object.
 * <p>
 * The underlying implementation use as {@link Long2ObjectHashMap} and combines both int keys into a long key.
 *
 * @param <V> type of the object stored in the map.
 */
public class BiInt2ObjectMap<V> {

    @SuppressWarnings("checkstyle:magicnumber")
    private static final long LOWER_INT_MASK = (1L << Integer.SIZE) - 1;

    /**
     * Handler for a map entry
     *
     * @param <V> type of the value
     */
    public interface EntryConsumer<V> {
        /**
         * A map entry
         *
         * @param keyPartA for the key
         * @param keyPartB for the key
         * @param value    for the entry
         */
        void accept(int keyPartA, int keyPartB, V value);
    }

    private final Long2ObjectHashMap<V> map;

    /**
     * Construct an empty map
     */
    public BiInt2ObjectMap() {
        map = new Long2ObjectHashMap<V>();
    }

    /**
     * See {@link Long2ObjectHashMap#Long2ObjectHashMap(int, double)}.
     *
     * @param initialCapacity for the underlying hash map
     * @param loadFactor      for the underlying hash map
     */
    public BiInt2ObjectMap(final int initialCapacity, final double loadFactor) {
        map = new Long2ObjectHashMap<V>(initialCapacity, loadFactor);
    }

    /**
     * Get the total capacity for the map to which the load factor with be a fraction of.
     *
     * @return the total capacity for the map.
     */
    public int capacity() {
        return map.capacity();
    }

    /**
     * Get the load factor beyond which the map will increase size.
     *
     * @return load factor for when the map should increase size.
     */
    public double loadFactor() {
        return map.loadFactor();
    }

    /**
     * Put a value into the map.
     *
     * @param keyPartA for the key
     * @param keyPartB for the key
     * @param value    to put into the map
     * @return the previous value if found otherwise null
     */
    public V put(final int keyPartA, final int keyPartB, final V value) {
        final long key = compoundKey(keyPartA, keyPartB);

        return map.put(key, value);
    }

    /**
     * Retrieve a value from the map.
     *
     * @param keyPartA for the key
     * @param keyPartB for the key
     * @return value matching the key if found or null if not found.
     */
    public V get(final int keyPartA, final int keyPartB) {
        final long key = compoundKey(keyPartA, keyPartB);

        return map.get(key);
    }

    /**
     * Remove a value from the map and return the value.
     *
     * @param keyPartA for the key
     * @param keyPartB for the key
     * @return the previous value if found otherwise null
     */
    public V remove(final int keyPartA, final int keyPartB) {
        final long key = compoundKey(keyPartA, keyPartB);

        return map.remove(key);
    }

    /**
     * Iterate over the entries of the map
     *
     * @param consumer to apply to each entry in the map
     */
    public void forEach(final EntryConsumer<V> consumer) {
        for (Map.Entry<Long, V> entry : map.entrySet()) {
            Long compoundKey = entry.getKey();
            final int keyPartA = (int) (compoundKey >>> Integer.SIZE);
            final int keyPartB = (int) (compoundKey & LOWER_INT_MASK);
            consumer.accept(keyPartA, keyPartB, entry.getValue());
        }
    }

    /**
     * Iterate over the values in the map
     *
     * @param consumer to apply to each value in the map
     */
    public void forEach(final Consumer<V> consumer) {
        for (Map.Entry<Long, V> entry : map.entrySet()) {
            consumer.accept(entry.getValue());
        }
    }

    /**
     * Return the number of unique entries in the map.
     *
     * @return number of unique entries in the map.
     */
    public int size() {
        return map.size();
    }

    /**
     * Is map empty or not.
     *
     * @return boolean indicating empty map or not
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    private static long compoundKey(final int keyPartA, final int keyPartB) {
        return ((long) keyPartA << Integer.SIZE) | keyPartB;
    }
}
