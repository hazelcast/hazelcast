/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.storage;

import com.hazelcast.internal.util.QuickMath;

import javax.annotation.Nonnull;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.IntFunction;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.collection.Hashing.intHash;

/**
 * A specialization of {@link com.hazelcast.internal.util.collection.Int2ObjectHashMap}
 * for {@code int} keys and {@code float[]} values.
 */
public class Int2FloatArrayHashMap {

    /** The default load factor for constructors not explicitly supplying it. */
    public static final double DEFAULT_LOAD_FACTOR = 0.6;
    /** The default initial capacity for constructors not explicitly supplying it. */
    public static final int DEFAULT_INITIAL_CAPACITY = 8;

    private final float[] nullValue;

    private final double loadFactor;
    private final int dimensions;
    private int resizeThreshold;
    private int capacity;
    private int mask;
    private int size;

    private int[] keys;
    private float[][] values;

    // cached to avoid allocation
    private final ValueCollection valueCollection = new ValueCollection();
    private final KeySet keySet = new KeySet();
    private final EntrySet entrySet = new EntrySet();

    public Int2FloatArrayHashMap(int dimensions) {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, dimensions);
    }

    public Int2FloatArrayHashMap(int initialCapacity, int dimensions) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, dimensions);
    }

    /**
     * Construct a new map allowing a configuration for initial capacity and load factor.
     *
     * @param initialCapacity for the backing array
     * @param loadFactor      limit for resizing on puts
     */
    public Int2FloatArrayHashMap(final int initialCapacity, final double loadFactor, int dimensions) {
        this.loadFactor = loadFactor;
        this.dimensions = dimensions;
        this.nullValue = new float[dimensions];
        capacity = QuickMath.nextPowerOfTwo(initialCapacity);
        mask = capacity - 1;
        resizeThreshold = (int) (capacity * loadFactor);

        keys = new int[capacity];
        values = new float[capacity][dimensions];
        initializeValues(values);
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
     * Overloaded version of {@link Map#containsKey(Object)} that takes a primitive int key.
     *
     * @param key for indexing the {@link Map}
     * @return true if the key is found otherwise false.
     */
    public boolean containsKey(final int key) {
        int index = intHash(key, mask);
        while (nullValue != values[index]) {
            if (key == keys[index]) {
                return true;
            }
            index = ++index & mask;
        }
        return false;
    }

    /**
     * Overloaded version of {@link Map#get(Object)} that takes a primitive int key.
     *
     * @param key for indexing the {@link Map}
     * @return the value if found otherwise null
     */
    public float[] get(final int key) {
        int index = intHash(key, mask);
        float[] value;
        while (nullValue != (value = values[index])) {
            if (key == keys[index]) {
                return value;
            }
            index = ++index & mask;
        }
        return null;
    }

    /**
     * Get a value for a given key, or if it does ot exist then default the value via a {@link IntFunction}
     * and put it in the map.
     *
     * @param key             to search on.
     * @param mappingFunction to provide a value if the get returns null.
     * @return the value if found otherwise the default.
     */
    public float[] computeIfAbsent(final int key, final IntFunction<float[]> mappingFunction) {
        checkNotNull(mappingFunction, "mappingFunction cannot be null");
        float[] value = get(key);
        if (value == null) {
            value = mappingFunction.apply(key);
            if (value != null) {
                put(key, value);
            }
        }
        return value;
    }

    /**
     * Overloaded version of {@link Map#put(Object, Object)} that takes a primitive int key.
     *
     * @param key   for indexing the {@link Map}
     * @param value to be inserted in the {@link Map}
     * @return the previous value if found otherwise null
     */
    public float[] put(final int key, final float[] value) {
        checkNotNull(value, "Value cannot be null");
        float[] oldValue = null;
        int index = intHash(key, mask);
        while (nullValue != values[index]) {
            if (key == keys[index]) {
                oldValue = values[index];
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
        return (oldValue == nullValue) ? null : oldValue;
    }

    /**
     * Overloaded version of {@link Map#remove(Object)} that takes a primitive int key.
     *
     * @param key for indexing the {@link Map}
     * @return the value if found otherwise null
     */
    public float[] remove(final int key) {
        int index = intHash(key, mask);
        float[] value;
        while (nullValue != (value = values[index])) {
            if (key == keys[index]) {
                values[index] = nullValue;
                --size;
                compactChain(index);
                return value;
            }
            index = ++index & mask;
        }
        return null;
    }

    public void clear() {
        size = 0;
        initializeValues(values);
    }

    /**
     * Compact the {@link Map} backing arrays by rehashing with a capacity just larger than current size
     * and giving consideration to the load factor.
     */
    public void compact() {
        final int idealCapacity = (int) Math.round(size() * (1.0d / loadFactor));
        rehash(QuickMath.nextPowerOfTwo(idealCapacity));
    }

    public KeySet keySet() {
        return keySet;
    }

    public Collection<float[]> values() {
        return valueCollection;
    }

    public boolean containsValue(final Object value) {
        checkNotNull(value, "Null values are not permitted");
        for (final float[] v : values) {
            if (nullValue != v && value.equals(v)) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     * This set's iterator also implements <code>Map.Entry</code>
     * so the <code>next()</code> method can just return the iterator
     * instance itself with no heap allocation. This characteristic
     * makes the set unusable wherever the returned entries are
     * retained (such as <code>coll.addAll(entrySet)</code>.
     */
    public Set<Entry<Integer, float[]>> entrySet() {
        return entrySet;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append('{');
        for (final Entry<Integer, float[]> entry : entrySet()) {
            sb.append(entry.getKey().intValue());
            sb.append('=');
            sb.append(Arrays.toString(entry.getValue()));
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
        final int[] tempKeys = new int[capacity];
        final float[][] tempValues = new float[capacity][dimensions];
        initializeValues(tempValues);
        for (int i = 0, size = values.length; i < size; i++) {
            final float[] value = values[i];
            if (nullValue != value) {
                final int key = keys[i];
                int newHash = intHash(key, mask);
                while (nullValue != tempValues[newHash]) {
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
            if (nullValue == values[index]) {
                return;
            }
            final int hash = intHash(keys[index], mask);
            if ((index < hash && (hash <= deleteIndex || deleteIndex <= index))
                    || (hash <= deleteIndex && deleteIndex <= index)) {
                keys[deleteIndex] = keys[index];
                values[deleteIndex] = values[index];
                values[index] = nullValue;
                deleteIndex = index;
            }
        }
    }

    private void initializeValues(float[][] values) {
        for (int i = 0; i < capacity; i++) {
            values[i] = nullValue;
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Internal Sets and Collections
    ///////////////////////////////////////////////////////////////////////////////////////////////

    /** Adds non-boxing methods to the standard Set interface. */
    public class KeySet extends AbstractSet<Integer> {

        @Override
        public int size() {
            return Int2FloatArrayHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return Int2FloatArrayHashMap.this.isEmpty();
        }

        @Override
        public boolean contains(final Object o) {
            if (o instanceof Integer i) {
                return Int2FloatArrayHashMap.this.containsKey(i.intValue());
            }
            return false;
        }

        /** Non-boxing variant of contains(). */
        public boolean contains(final int key) {
            return Int2FloatArrayHashMap.this.containsKey(key);
        }

        @Override
        @Nonnull
        public KeyIterator iterator() {
            return new KeyIterator();
        }

        @Override
        public boolean remove(final Object o) {
            if (o instanceof Integer i) {
                return null != Int2FloatArrayHashMap.this.remove(i.intValue());
            }
            return false;
        }

        /** Non-boxing variant of remove(). */
        public boolean remove(final int key) {
            return null != Int2FloatArrayHashMap.this.remove(key);
        }

        @Override
        public void clear() {
            Int2FloatArrayHashMap.this.clear();
        }
    }

    private class ValueCollection extends AbstractCollection<float[]> {

        @Override
        public int size() {
            return Int2FloatArrayHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return Int2FloatArrayHashMap.this.isEmpty();
        }

        @Override
        public boolean contains(final Object o) {
            return Int2FloatArrayHashMap.this.containsValue(o);
        }

        @Override
        @Nonnull
        public ValueIterator<float[]> iterator() {
            return new ValueIterator<>();
        }

        @Override
        public void clear() {
            Int2FloatArrayHashMap.this.clear();
        }
    }

    private class EntrySet extends AbstractSet<Entry<Integer, float[]>> {

        @Override
        public int size() {
            return Int2FloatArrayHashMap.this.size();
        }

        @Override
        public boolean isEmpty() {
            return Int2FloatArrayHashMap.this.isEmpty();
        }

        @Override
        @Nonnull
        public Iterator<Entry<Integer, float[]>> iterator() {
            return new EntryIterator();
        }

        @Override
        public void clear() {
            Int2FloatArrayHashMap.this.clear();
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Iterators
    ///////////////////////////////////////////////////////////////////////////////////////////////

    private abstract class AbstractIterator<T> implements Iterator<T> {

        protected final int[] keys = Int2FloatArrayHashMap.this.keys;
        protected final float[][] values = Int2FloatArrayHashMap.this.values;
        private int posCounter;
        private int stopCounter;
        private boolean isPositionValid;

        protected AbstractIterator() {
            int i = capacity;
            if (nullValue != values[capacity - 1]) {
                i = 0;
                for (int size = capacity; i < size; i++) {
                    if (nullValue == values[i]) {
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
                if (nullValue != values[index]) {
                    return true;
                }
            }
            return false;
        }

        protected void findNext() {
            isPositionValid = false;
            for (int i = posCounter - 1; i >= stopCounter; i--) {
                final int index = i & mask;
                if (nullValue != values[index]) {
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
                values[position] = nullValue;
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
        public T next() {
            findNext();
            return (T) values[getPosition()];
        }
    }

    /** Adds an unboxed next() method to the standard Iterator interface. */
    public class KeyIterator extends AbstractIterator<Integer> {

        @Override
        public Integer next() {
            return nextInt();
        }

        /** Non-boxing variant of next(). */
        public int nextInt() {
            findNext();
            return keys[getPosition()];
        }
    }

    private class EntryIterator extends AbstractIterator<Entry<Integer, float[]>> implements Entry<Integer, float[]> {

        @Override
        public Entry<Integer, float[]> next() {
            findNext();
            return this;
        }

        @Override
        public Integer getKey() {
            return keys[getPosition()];
        }

        @Override
        public float[] getValue() {
            return values[getPosition()];
        }

        @Override
        public float[] setValue(final float[] value) {
            checkNotNull(value);
            final int pos = getPosition();
            final float[] oldValue = values[pos];
            values[pos] = value;
            return oldValue;
        }
    }
}
