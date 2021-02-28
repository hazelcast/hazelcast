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

import com.hazelcast.internal.util.QuickMath;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.LongFunction;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.collection.Hashing.longHash;

/**
 * {@link java.util.Map} implementation specialised for {@code long} keys using open addressing and
 * linear probing for cache efficient access.
 * <p>
 * NOTE: This map doesn't support {@code null} keys and values.
 *
 * @param <V> values stored in the {@link java.util.Map}
 */
public class Long2ObjectHashMap<V> implements Map<Long, V> {

    /** The default load factor for constructors not explicitly supplying it */
    public static final double DEFAULT_LOAD_FACTOR = 0.6;
    /** The default initial capacity for constructors not explicitly supplying it */
    public static final int DEFAULT_INITIAL_CAPACITY = 8;

    private final double loadFactor;
    private int resizeThreshold;
    private int capacity;
    private int mask;
    private int size;

    private long[] keys;
    private Object[] values;

    // cached to avoid allocation
    private final ValueCollection valueCollection = new ValueCollection();
    private final KeySet keySet = new KeySet();
    private final EntrySet entrySet = new EntrySet();

    public Long2ObjectHashMap() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    public Long2ObjectHashMap(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Construct a new map allowing a configuration for initial capacity and load factor.
     *
     * @param initialCapacity for the backing array
     * @param loadFactor      limit for resizing on puts
     */
    public Long2ObjectHashMap(final int initialCapacity, final double loadFactor) {
        this.loadFactor = loadFactor;
        capacity = QuickMath.nextPowerOfTwo(initialCapacity);
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

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return 0 == size;
    }

    @Override
    public boolean containsKey(final Object key) {
        checkNotNull(key, "Null keys are not permitted");
        return containsKey(((Long) key).longValue());
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

    @Override
    public boolean containsValue(final Object value) {
        checkNotNull(value, "Null values are not permitted");
        for (final Object v : values) {
            if (null != v && value.equals(v)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public V get(final Object key) {
        return get(((Long) key).longValue());
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
     * Get a value for a given key, or if it does ot exist then default the value via a {@link LongFunction}
     * and put it in the map.
     *
     * @param key             to search on.
     * @param mappingFunction to provide a value if the get returns null.
     * @return the value if found otherwise the default.
     */
    public V computeIfAbsent(final long key, final LongFunction<? extends V> mappingFunction) {
        checkNotNull(mappingFunction, "mappingFunction cannot be null");
        V value = get(key);
        if (value == null) {
            value = mappingFunction.apply(key);
            if (value != null) {
                put(key, value);
            }
        }
        return value;
    }

    @Override
    public V put(final Long key, final V value) {
        return put(key.longValue(), value);
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

    @Override
    public V remove(final Object key) {
        return remove(((Long) key).longValue());
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

    @Override
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
        rehash(QuickMath.nextPowerOfTwo(idealCapacity));
    }

    @Override
    public void putAll(final Map<? extends Long, ? extends V> map) {
        for (final Entry<? extends Long, ? extends V> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public KeySet keySet() {
        return keySet;
    }

    @Override
    public Collection<V> values() {
        return valueCollection;
    }

    /**
     * {@inheritDoc}
     * This set's iterator also implements <code>Map.Entry</code>
     * so the <code>next()</code> method can just return the iterator
     * instance itself with no heap allocation. This characteristic
     * makes the set unusable wherever the returned entries are
     * retained (such as <code>coll.addAll(entrySet)</code>.
     */
    @Override
    public Set<Entry<Long, V>> entrySet() {
        return entrySet;
    }

    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (final Entry<Long, V> entry : entrySet()) {
            sb.append(entry.getKey().longValue());
            sb.append('=');
            sb.append(entry.getValue());
            sb.append(", ");
        }
        if (sb.length() > 1) {
            sb.setLength(sb.length() - 2);
        }
        sb.append('}');
        return sb.toString();
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

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Internal Sets and Collections
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /** Adds non-boxing methods to the standard Set interface. */
    public class KeySet extends AbstractSet<Long> {

        @Override
        public int size() {
            return Long2ObjectHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return Long2ObjectHashMap.this.isEmpty();
        }

        @Override
        public boolean contains(final Object o) {
            return Long2ObjectHashMap.this.containsKey(o);
        }

        /** Non-boxing variant of contains(). */
        public boolean contains(final long key) {
            return Long2ObjectHashMap.this.containsKey(key);
        }

        @Override
        public KeyIterator iterator() {
            return new KeyIterator();
        }

        @Override
        public boolean remove(final Object o) {
            return null != Long2ObjectHashMap.this.remove(o);
        }

        /** Non-boxing variant of remove(). */
        public boolean remove(final long key) {
            return null != Long2ObjectHashMap.this.remove(key);
        }

        @Override
        public void clear() {
            Long2ObjectHashMap.this.clear();
        }
    }

    private class ValueCollection extends AbstractCollection<V> {

        @Override
        public int size() {
            return Long2ObjectHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return Long2ObjectHashMap.this.isEmpty();
        }

        @Override
        public boolean contains(final Object o) {
            return Long2ObjectHashMap.this.containsValue(o);
        }

        @Override
        public Iterator<V> iterator() {
            return new ValueIterator<V>();
        }

        @Override
        public void clear() {
            Long2ObjectHashMap.this.clear();
        }
    }

    private class EntrySet extends AbstractSet<Entry<Long, V>> {

        @Override
        public int size() {
            return Long2ObjectHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return Long2ObjectHashMap.this.isEmpty();
        }

        @Override
        public Iterator<Entry<Long, V>> iterator() {
            return new EntryIterator();
        }

        @Override
        public void clear() {
            Long2ObjectHashMap.this.clear();
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Iterators
    ///////////////////////////////////////////////////////////////////////////////////////////////

    private abstract class AbstractIterator<T> implements Iterator<T> {

        protected final long[] keys = Long2ObjectHashMap.this.keys;
        protected final Object[] values = Long2ObjectHashMap.this.values;
        private int posCounter;
        private int stopCounter;
        private boolean isPositionValid;

        protected AbstractIterator() {
            int i = capacity;
            if (null != values[capacity - 1]) {
                i = 0;
                for (int size = capacity; i < size; i++) {
                    if (null == values[i]) {
                        break;
                    }
                }
            }
            stopCounter = i;
            posCounter = i + capacity;
        }

        protected int getPosition() {
            return posCounter & mask;
        }

        @Override
        public boolean hasNext() {
            for (int i = posCounter - 1; i >= stopCounter; i--) {
                final int index = i & mask;
                if (null != values[index]) {
                    return true;
                }
            }
            return false;
        }

        protected void findNext() {
            isPositionValid = false;
            for (int i = posCounter - 1; i >= stopCounter; i--) {
                final int index = i & mask;
                if (null != values[index]) {
                    posCounter = i;
                    isPositionValid = true;
                    return;
                }
            }
            throw new NoSuchElementException();
        }

        @Override
        public abstract T next();

        @Override
        public void remove() {
            if (isPositionValid) {
                final int position = getPosition();
                values[position] = null;
                --size;
                compactChain(position);
                isPositionValid = false;
            } else {
                throw new IllegalStateException();
            }
        }
    }

    private class ValueIterator<T> extends AbstractIterator<T> {
        @Override
        @SuppressWarnings("unchecked")
        public T next() {
            findNext();
            return (T) values[getPosition()];
        }
    }

    /** Adds an unboxed next() method to the standard Iterator interface. */
    public class KeyIterator extends AbstractIterator<Long> {

        @Override
        public Long next() {
            return nextLong();
        }

        /** Non-boxing variant of next(). */
        public long nextLong() {
            findNext();
            return keys[getPosition()];
        }
    }

    @SuppressWarnings("unchecked")
    @SuppressFBWarnings(value = "PZ_DONT_REUSE_ENTRY_OBJECTS_IN_ITERATORS",
            justification = "deliberate, documented choice")
    private class EntryIterator extends AbstractIterator<Entry<Long, V>> implements Entry<Long, V> {

        @Override
        public Entry<Long, V> next() {
            findNext();
            return this;
        }

        @Override
        public Long getKey() {
            return keys[getPosition()];
        }

        @Override
        public V getValue() {
            return (V) values[getPosition()];
        }

        @Override
        public V setValue(final V value) {
            checkNotNull(value);
            final int pos = getPosition();
            final Object oldValue = values[pos];
            values[pos] = value;
            return (V) oldValue;
        }
    }
}
