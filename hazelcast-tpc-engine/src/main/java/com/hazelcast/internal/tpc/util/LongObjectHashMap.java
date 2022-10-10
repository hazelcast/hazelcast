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

package com.hazelcast.internal.tpc.util;


import java.util.Arrays;
import java.util.Map;

import static com.hazelcast.internal.tpc.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpc.util.BitUtil.longHash;


/**
 * {@link Map} implementation specialised for {@code long} keys using open addressing and
 * linear probing for cache efficient access.
 * <p>
 * NOTE: This map doesn't support {@code null} keys and values.
 *
 * @param <V> values stored in the {@link Map}
 */
public class LongObjectHashMap<V> {

    /**
     * The default load factor for constructors not explicitly supplying it
     */
    public static final double DEFAULT_LOAD_FACTOR = 0.6;
    /**
     * The default initial capacity for constructors not explicitly supplying it
     */
    public static final int DEFAULT_INITIAL_CAPACITY = 8;

    private final double loadFactor;
    private int resizeThreshold;
    private int capacity;
    private int mask;
    private int size;

    private long[] keys;
    private Object[] values;

    // cached to avoid allocation
    public LongObjectHashMap() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    public LongObjectHashMap(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Construct a new map allowing a configuration for initial capacity and load factor.
     *
     * @param initialCapacity for the backing array
     * @param loadFactor      limit for resizing on puts
     */
    public LongObjectHashMap(final int initialCapacity, final double loadFactor) {
        this.loadFactor = loadFactor;
        capacity = BitUtil.nextPowerOfTwo(initialCapacity);
        mask = capacity - 1;
        resizeThreshold = (int) (capacity * loadFactor);

        keys = new long[capacity];
        values = new Object[capacity];
    }

    /**
     * Get the load factor beyond which the map will increase size.
     *
     * @return load factor for when the map should increase size.
     */
    public double loadFactor() {
        return loadFactor;
    }

    /**
     * Get the total capacity for the map to which the load factor with be a fraction of.
     *
     * @return the total capacity for the map.
     */
    public int capacity() {
        return capacity;
    }

    /**
     * Get the actual threshold which when reached the map resize.
     * This is a function of the current capacity and load factor.
     *
     * @return the threshold when the map will resize.
     */
    public int resizeThreshold() {
        return resizeThreshold;
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return 0 == size;
    }

    /**
     * Overloaded version of {@link Map#containsKey(Object)} that takes a primitive long key.
     *
     * @param key for indexing the {@link Map}
     * @return true if the key is found otherwise false.
     */
    public boolean containsKey(final long key) {
        int index = longHash(key, mask);
        while (null != values[index]) {
            if (key == keys[index]) {
                return true;
            }
            index = ++index & mask;
        }
        return false;
    }

    /**
     * Overloaded version of {@link Map#get(Object)} that takes a primitive long key.
     *
     * @param key for indexing the {@link Map}
     * @return the value if found otherwise null
     */
    @SuppressWarnings("unchecked")
    public V get(final long key) {
        int index = longHash(key, mask);
        Object value;
        while (null != (value = values[index])) {
            if (key == keys[index]) {
                return (V) value;
            }
            index = ++index & mask;
        }
        return null;
    }


    /**
     * Overloaded version of {@link Map#put(Object, Object)} that takes a primitive long key.
     *
     * @param key   for indexing the {@link Map}
     * @param value to be inserted in the {@link Map}
     * @return the previous value if found otherwise null
     */
    @SuppressWarnings("unchecked")
    public V put(final long key, final V value) {
        checkNotNull(value, "Value cannot be null");
        V oldValue = null;
        int index = longHash(key, mask);
        while (null != values[index]) {
            if (key == keys[index]) {
                oldValue = (V) values[index];
                break;
            }
            index = ++index & mask;
        }
        if (null == oldValue) {
            ++size;
            keys[index] = key;
        }
        values[index] = value;
        if (size > resizeThreshold) {
            increaseCapacity();
        }
        return oldValue;
    }

    /**
     * Overloaded version of {@link Map#remove(Object)} that takes a primitive long key.
     *
     * @param key for indexing the {@link Map}
     * @return the value if found otherwise null
     */
    @SuppressWarnings("unchecked")
    public V remove(final long key) {
        int index = longHash(key, mask);
        Object value;
        while (null != (value = values[index])) {
            if (key == keys[index]) {
                values[index] = null;
                --size;
                compactChain(index);
                return (V) value;
            }
            index = ++index & mask;
        }
        return null;
    }

    public void clear() {
        size = 0;
        Arrays.fill(values, null);
    }

    /**
     * Compact the {@link Map} backing arrays by rehashing with a capacity just larger than current size
     * and giving consideration to the load factor.
     */
    public void compact() {
        final int idealCapacity = (int) Math.round(size() * (1.0d / loadFactor));
        rehash(BitUtil.nextPowerOfTwo(idealCapacity));
    }

    private void increaseCapacity() {
        final int newCapacity = capacity << 1;
        if (newCapacity < 0) {
            throw new IllegalStateException("Max capacity reached at size=" + size);
        }
        rehash(newCapacity);
    }

    private void rehash(final int newCapacity) {
        if (1 != Integer.bitCount(newCapacity)) {
            throw new IllegalStateException("New capacity must be a power of two");
        }
        capacity = newCapacity;
        mask = newCapacity - 1;
        resizeThreshold = (int) (newCapacity * loadFactor);
        final long[] tempKeys = new long[capacity];
        final Object[] tempValues = new Object[capacity];
        for (int i = 0, size = values.length; i < size; i++) {
            final Object value = values[i];
            if (null != value) {
                final long key = keys[i];
                int newHash = longHash(key, mask);
                while (null != tempValues[newHash]) {
                    newHash = ++newHash & mask;
                }
                tempKeys[newHash] = key;
                tempValues[newHash] = value;
            }
        }
        keys = tempKeys;
        values = tempValues;
    }

    private void compactChain(int deleteIndex) {
        int index = deleteIndex;
        while (true) {
            index = ++index & mask;
            if (null == values[index]) {
                return;
            }
            final int hash = longHash(keys[index], mask);
            if ((index < hash && (hash <= deleteIndex || deleteIndex <= index))
                    || (hash <= deleteIndex && deleteIndex <= index)) {
                keys[deleteIndex] = keys[index];
                values[deleteIndex] = values[index];
                values[index] = null;
                deleteIndex = index;
            }
        }
    }


}
