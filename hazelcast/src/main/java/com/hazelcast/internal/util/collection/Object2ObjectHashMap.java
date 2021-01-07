/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.internal.util.collection;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;
import static com.hazelcast.internal.util.collection.Hashing.evenLongHash;
import static java.util.Objects.requireNonNull;

/**
 * A open addressing with linear probing hash map.
 */
public class Object2ObjectHashMap<K, V> implements Map<K, V>, Serializable {
    static final int MIN_CAPACITY = 8;
    private static final long serialVersionUID = -1549211171492606678L;
    private static final float DEFAULT_LOAD_FACTOR = 0.55f;

    private final float loadFactor;
    private int resizeThreshold;
    private int size;
    private final boolean shouldAvoidAllocation;

    private Object[] entries;
    private KeySet keySet;
    private ValueCollection valueCollection;
    private EntrySet entrySet;

    /**
     * Default constructor, i.e. create a map with {@link
     * #MIN_CAPACITY} and {@link #DEFAULT_LOAD_FACTOR}.
     */
    public Object2ObjectHashMap() {
        this(MIN_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Create a map with initiail capacity and load factor.
     *
     * @param initialCapacity for the map to override {@link #MIN_CAPACITY}
     * @param loadFactor      for the map to override {@link #DEFAULT_LOAD_FACTOR}.
     */
    public Object2ObjectHashMap(final int initialCapacity, final float loadFactor) {
        this(initialCapacity, loadFactor, true);
    }

    /**
     * @param initialCapacity       for the map to override {@link #MIN_CAPACITY}
     * @param loadFactor            for the map to override {@link #DEFAULT_LOAD_FACTOR}.
     * @param shouldAvoidAllocation should allocation be avoided by caching iterators and map entries.
     */
    public Object2ObjectHashMap(final int initialCapacity, final float loadFactor, final boolean shouldAvoidAllocation) {
        this.loadFactor = loadFactor;
        this.shouldAvoidAllocation = shouldAvoidAllocation;

        capacity(nextPowerOfTwo(Math.max(MIN_CAPACITY, initialCapacity)));
    }

    /**
     * Get the load factor applied for resize operations.
     *
     * @return the load factor applied for resize operations.
     */
    public float loadFactor() {
        return loadFactor;
    }

    /**
     * Get the actual threshold which when reached the map will resize.
     * This is a function of the current capacity and load factor.
     *
     * @return the threshold when the map will resize.
     */
    public int resizeThreshold() {
        return resizeThreshold;
    }

    /**
     * Get the total capacity for the map to which the load factor will be a fraction of.
     *
     * @return the total capacity for the map.
     */
    public int capacity() {
        return entries.length >> 1;
    }

    /**
     * {@inheritDoc}
     */
    public int size() {
        return size;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isEmpty() {
        return size == 0;
    }

    public V get(final Object key) {
        return unmapNullValue(getMapped(key));
    }

    @SuppressWarnings("unchecked")
    private V getMapped(final Object key) {
        Objects.requireNonNull(key);

        final int mask = entries.length - 1;
        int index = evenLongHash(key.hashCode(), mask);

        Object value = null;
        while (entries[index + 1] != null) {
            if (entries[index] == key || entries[index].equals(key)) {
                value = entries[index + 1];
                break;
            }

            index = next(index, mask);
        }

        return (V) value;
    }

    /**
     * Put a key value pair into the map.
     *
     * @param key   lookup key
     * @param value new value, must not be null
     * @return current value associated with key, or null if none found
     * @throws IllegalArgumentException if value is null
     */
    public V put(final K key, final V value) {
        final Object val = mapNullValue(value);
        requireNonNull(val, "value cannot be null");

        final int mask = entries.length - 1;
        int index = evenLongHash(key.hashCode(), mask);
        Object oldValue = null;

        while (entries[index + 1] != null) {
            if (entries[index] == key || entries[index].equals(key)) {
                oldValue = entries[index + 1];
                break;
            }

            index = next(index, mask);
        }

        if (oldValue == null) {
            ++size;
            entries[index] = key;
        }

        entries[index + 1] = val;

        increaseCapacity();

        return unmapNullValue(oldValue);
    }

    private void increaseCapacity() {
        if (size > resizeThreshold) {
            // entries.length = 2 * capacity
            final int newCapacity = entries.length;
            rehash(newCapacity);
        }
    }

    private void rehash(final int newCapacity) {
        final Object[] oldEntries = entries;
        final int length = entries.length;

        capacity(newCapacity);

        final Object[] newEntries = entries;
        final int mask = entries.length - 1;

        for (int keyIndex = 0; keyIndex < length; keyIndex += 2) {
            final Object value = oldEntries[keyIndex + 1];
            if (value != null) {
                final Object key = oldEntries[keyIndex];
                int index = evenLongHash(key.hashCode(), mask);

                while (newEntries[index + 1] != null) {
                    index = next(index, mask);
                }

                newEntries[index] = key;
                newEntries[index + 1] = value;
            }
        }
    }

    /**
     * Does the map contain the value.
     *
     * @param value to be tested against contained values.
     * @return true if contained otherwise false.
     */
    public boolean containsValue(final Object value) {
        final Object val = mapNullValue(value);
        boolean found = false;
        if (val != null) {
            final int length = entries.length;

            for (int valueIndex = 1; valueIndex < length; valueIndex += 2) {
                if (val == entries[valueIndex] || val.equals(entries[valueIndex])) {
                    found = true;
                    break;
                }
            }
        }

        return found;
    }

    /**
     * {@inheritDoc}
     */
    public void clear() {
        if (size > 0) {
            Arrays.fill(entries, null);
            size = 0;
        }
    }

    /**
     * Compact the backing arrays by rehashing with a capacity just larger than current size
     * and giving consideration to the load factor.
     */
    public void compact() {
        final int idealCapacity = (int) Math.round(size() * (1.0d / loadFactor));
        rehash(nextPowerOfTwo(Math.max(MIN_CAPACITY, idealCapacity)));
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    public void forEach(final BiConsumer<? super K, ? super V> consumer) {
        int remaining = size;

        for (int i = 1, length = entries.length; remaining > 0 && i < length; i += 2) {
            if (null != entries[i]) {
                consumer.accept((K) entries[i - 1], unmapNullValue(entries[i]));
                --remaining;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean containsKey(final Object key) {
        return getMapped(key) != null;
    }

    /**
     * {@inheritDoc}
     */
    public void putAll(final Map<? extends K, ? extends V> map) {
        for (final Entry<? extends K, ? extends V> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * {@inheritDoc}
     */
    public KeySet keySet() {
        if (null == keySet) {
            keySet = new KeySet();
        }

        return keySet;
    }

    /**
     * {@inheritDoc}
     */
    public ValueCollection values() {
        if (null == valueCollection) {
            valueCollection = new ValueCollection();
        }

        return valueCollection;
    }

    /**
     * {@inheritDoc}
     */
    public EntrySet entrySet() {
        if (null == entrySet) {
            entrySet = new EntrySet();
        }

        return entrySet;
    }

    /**
     * {@inheritDoc}
     */
    public V remove(final Object key) {
        final Object[] entries = this.entries;
        final int mask = entries.length - 1;
        int keyIndex = evenLongHash(key.hashCode(), mask);

        Object oldValue = null;
        while (entries[keyIndex + 1] != null) {
            if (entries[keyIndex] == key || entries[keyIndex].equals(key)) {
                oldValue = entries[keyIndex + 1];
                entries[keyIndex] = null;
                entries[keyIndex + 1] = null;
                size--;

                compactChain(keyIndex);

                break;
            }

            keyIndex = next(keyIndex, mask);
        }

        return unmapNullValue(oldValue);
    }

    @SuppressWarnings("FinalParameters")
    private void compactChain(int deleteKeyIndex) {
        final Object[] entries = this.entries;
        final int mask = entries.length - 1;
        int keyIndex = deleteKeyIndex;

        while (true) {
            keyIndex = next(keyIndex, mask);
            if (entries[keyIndex + 1] == null) {
                break;
            }

            final int hash = evenLongHash(entries[keyIndex].hashCode(), mask);

            if ((keyIndex < hash && (hash <= deleteKeyIndex
                    || deleteKeyIndex <= keyIndex))
                    || (hash <= deleteKeyIndex && deleteKeyIndex <= keyIndex)) {
                entries[deleteKeyIndex] = entries[keyIndex];
                entries[deleteKeyIndex + 1] = entries[keyIndex + 1];

                entries[keyIndex] = null;
                entries[keyIndex + 1] = null;
                deleteKeyIndex = keyIndex;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    public String toString() {
        if (isEmpty()) {
            return "{}";
        }

        final EntryIterator entryIterator = new EntryIterator();
        entryIterator.reset();

        final StringBuilder sb = new StringBuilder().append('{');
        while (true) {
            entryIterator.next();
            sb.append(entryIterator.getKey()).append('=').append(unmapNullValue(entryIterator.getValue()));
            if (!entryIterator.hasNext()) {
                return sb.append('}').toString();
            }
            sb.append(',').append(' ');
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Map)) {
            return false;
        }

        final Map<K, V> that = (Map<K, V>) o;

        return size == that.size() && entrySet().equals(that.entrySet());
    }

    public int hashCode() {
        return entrySet().hashCode();
    }

    /**
     * Handle incoming <em>null</em> value and optionally replace with another non-null counterpart.
     *
     * @param value value to be handled.
     * @return replacement value.
     */
    protected Object mapNullValue(final Object value) {
        return value;
    }

    /**
     * Handle incoming non-null value and optionally replace it with the <em>null</em> value counterpart. This is the
     * opposite of the {@link #mapNullValue(Object)} method.
     *
     * @param value value to be handled.
     * @return replacement value.
     * @see #mapNullValue(Object)
     */
    @SuppressWarnings("unchecked")
    protected V unmapNullValue(final Object value) {
        return (V) value;
    }

    private static int next(final int index, final int mask) {
        return (index + 2) & mask;
    }

    private void capacity(final int newCapacity) {
        final int entriesLength = newCapacity * 2;
        if (entriesLength < 0) {
            throw new IllegalStateException("max capacity reached at size=" + size);
        }

        /*@DoNotSub*/
        resizeThreshold = (int) (newCapacity * loadFactor);
        entries = new Object[entriesLength];
    }

    // ---------------- Utility Classes ----------------

    /**
     * Base iterator impl.
     */
    abstract class AbstractIterator implements Serializable {
        private static final long serialVersionUID = -6722944592177026218L;
        /**
         * Is position valid.
         */
        protected boolean isPositionValid;
        private int remaining;
        private int positionCounter;
        private int stopCounter;

        final void reset() {
            isPositionValid = false;
            remaining = Object2ObjectHashMap.this.size;
            final Object[] entries = Object2ObjectHashMap.this.entries;
            final int capacity = entries.length;

            int keyIndex = capacity;
            if (null != entries[capacity - 1]) {
                for (int i = 1; i < capacity; i += 2) {
                    if (entries[i] == null) {
                        keyIndex = i - 1;
                        break;
                    }
                }
            }

            stopCounter = keyIndex;
            positionCounter = keyIndex + capacity;
        }

        final int keyPosition() {
            return positionCounter & entries.length - 1;
        }

        /**
         * Return number of remaining elements.
         *
         * @return number of remaining elements.
         */
        public int remaining() {
            return remaining;
        }

        /**
         * Check if there is next element to iterate.
         *
         * @return {@code true} if {@code remaining > 0}.
         */
        public boolean hasNext() {
            return remaining > 0;
        }

        /**
         * Find next element.
         *
         * @throws NoSuchElementException if no more elements.
         */
        protected final void findNext() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final Object[] entries = Object2ObjectHashMap.this.entries;
            final int mask = entries.length - 1;

            for (int keyIndex = positionCounter - 2; keyIndex >= stopCounter; keyIndex -= 2) {
                final int index = keyIndex & mask;
                if (entries[index + 1] != null) {
                    isPositionValid = true;
                    positionCounter = keyIndex;
                    --remaining;
                    return;
                }
            }

            isPositionValid = false;
            throw new IllegalStateException();
        }

        /**
         * {@inheritDoc}
         */
        public void remove() {
            if (isPositionValid) {
                final int position = keyPosition();
                entries[position] = null;
                entries[position + 1] = null;
                --size;

                compactChain(position);

                isPositionValid = false;
            } else {
                throw new IllegalStateException();
            }
        }
    }

    /**
     * An iterator over keys.
     */
    @SuppressFBWarnings("SE_INNER_CLASS")
    public final class KeyIterator extends AbstractIterator implements Iterator<K>, Serializable {
        private static final long serialVersionUID = 2381081253326969359L;

        @SuppressWarnings("unchecked")
        public K next() {
            findNext();
            return (K) entries[keyPosition()];
        }
    }

    /**
     * An iterator over values.
     */
    @SuppressFBWarnings("SE_INNER_CLASS")
    public final class ValueIterator extends AbstractIterator implements Iterator<V>, Serializable {
        private static final long serialVersionUID = -9021925571373462472L;

        public V next() {
            findNext();
            return unmapNullValue(entries[keyPosition() + 1]);
        }
    }

    /**
     * An iterator over entries.
     */
    @SuppressWarnings("unchecked")
    @SuppressFBWarnings({"PZ_DONT_REUSE_ENTRY_OBJECTS_IN_ITERATORS", "SE_INNER_CLASS"})
    public final class EntryIterator
            extends AbstractIterator
            implements Iterator<Entry<K, V>>, Entry<K, V>, Serializable {
        private static final long serialVersionUID = 7273758986584625427L;

        @SuppressWarnings("unchecked")
        public K getKey() {
            return (K) entries[keyPosition()];
        }

        public V getValue() {
            return unmapNullValue(entries[keyPosition() + 1]);
        }

        @SuppressWarnings("unchecked")
        public V setValue(final V value) {
            final V val = (V) mapNullValue(value);

            if (!isPositionValid) {
                throw new IllegalStateException();
            }

            if (null == val) {
                throw new IllegalArgumentException();
            }

            final int keyPosition = keyPosition();
            final Object prevValue = entries[keyPosition + 1];
            entries[keyPosition + 1] = val;
            return unmapNullValue(prevValue);
        }

        public Entry<K, V> next() {
            findNext();

            if (shouldAvoidAllocation) {
                return this;
            }

            return allocateDuplicateEntry();
        }

        private Entry<K, V> allocateDuplicateEntry() {
            return new MapEntry(getKey(), getValue());
        }

        /**
         * {@inheritDoc}
         */
        public int hashCode() {
            return getKey().hashCode() ^ getValue().hashCode();
        }

        /**
         * {@inheritDoc}
         */
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Entry)) {
                return false;
            }

            final Entry<?, ?> that = (Entry<?, ?>) o;

            return Objects.equals(getKey(), that.getKey()) && Objects.equals(getValue(), that.getValue());
        }

        /**
         * An {@link java.util.Map.Entry} implementation.
         */
        public final class MapEntry implements Entry<K, V> {
            private final K k;
            private final V v;

            /**
             * @param k key.
             * @param v value.
             */
            public MapEntry(final K k, final V v) {
                this.k = k;
                this.v = v;
            }

            public K getKey() {
                return k;
            }

            public V getValue() {
                return v;
            }

            public V setValue(final V value) {
                return Object2ObjectHashMap.this.put(k, value);
            }

            public int hashCode() {
                final V v = getValue();
                return getKey().hashCode() ^ (v != null ? v.hashCode() : 0);
            }

            public boolean equals(final Object o) {
                if (!(o instanceof Map.Entry)) {
                    return false;
                }

                final Entry<?, ?> e = (Entry<?, ?>) o;

                return (e.getKey() != null && e.getKey().equals(k))
                        && ((e.getValue() == null && v == null) || e.getValue().equals(v));
            }

            public String toString() {
                return k + "=" + v;
            }
        }
    }

    /**
     * A key set implementation.
     */
    @SuppressFBWarnings("SE_INNER_CLASS")
    public final class KeySet extends AbstractSet<K> implements Serializable {
        private static final long serialVersionUID = 9104229702905879053L;
        private final KeyIterator keyIterator = shouldAvoidAllocation ? new KeyIterator() : null;

        /**
         * {@inheritDoc}
         */
        public KeyIterator iterator() {
            KeyIterator keyIterator = this.keyIterator;
            if (null == keyIterator) {
                keyIterator = new KeyIterator();
            }

            keyIterator.reset();
            return keyIterator;
        }

        /**
         * {@inheritDoc}
         */
        public int size() {
            return Object2ObjectHashMap.this.size();
        }

        /**
         * {@inheritDoc}
         */
        public boolean isEmpty() {
            return Object2ObjectHashMap.this.isEmpty();
        }

        /**
         * {@inheritDoc}
         */
        public void clear() {
            Object2ObjectHashMap.this.clear();
        }

        /**
         * {@inheritDoc}
         */
        public boolean contains(final Object o) {
            return containsKey(o);
        }

        @SuppressWarnings("unchecked")
        public void forEach(final Consumer<? super K> action) {
            int remaining = Object2ObjectHashMap.this.size;

            for (int i = 1, length = entries.length; remaining > 0 && i < length; i += 2) {
                if (null != entries[i]) {
                    action.accept((K) entries[i - 1]);
                    --remaining;
                }
            }
        }
    }

    /**
     * A collection of values.
     */
    @SuppressFBWarnings("SE_INNER_CLASS")
    public final class ValueCollection extends AbstractCollection<V> implements Serializable {
        private static final long serialVersionUID = 4700865656461554026L;
        private final ValueIterator valueIterator = shouldAvoidAllocation ? new ValueIterator() : null;

        /**
         * {@inheritDoc}
         */
        public ValueIterator iterator() {
            ValueIterator valueIterator = this.valueIterator;
            if (null == valueIterator) {
                valueIterator = new ValueIterator();
            }

            valueIterator.reset();
            return valueIterator;
        }

        /**
         * {@inheritDoc}
         */
        public int size() {
            return Object2ObjectHashMap.this.size();
        }

        /**
         * {@inheritDoc}
         */
        public boolean contains(final Object o) {
            return containsValue(o);
        }

        public void forEach(final Consumer<? super V> action) {
            int remaining = Object2ObjectHashMap.this.size;

            for (int i = 1, length = entries.length; remaining > 0 && i < length; i += 2) {
                if (null != entries[i]) {
                    action.accept(unmapNullValue(entries[i]));
                    --remaining;
                }
            }
        }
    }

    /**
     * An entry set implementation.
     */
    @SuppressFBWarnings("SE_INNER_CLASS")
    public final class EntrySet extends AbstractSet<Map.Entry<K, V>> implements Serializable {
        private static final long serialVersionUID = 7025830249792699509L;
        private final EntryIterator entryIterator = shouldAvoidAllocation ? new EntryIterator() : null;

        /**
         * {@inheritDoc}
         */
        public EntryIterator iterator() {
            EntryIterator entryIterator = this.entryIterator;
            if (null == entryIterator) {
                entryIterator = new EntryIterator();
            }

            entryIterator.reset();
            return entryIterator;
        }

        /**
         * {@inheritDoc}
         */
        public int size() {
            return Object2ObjectHashMap.this.size();
        }

        /**
         * {@inheritDoc}
         */
        public boolean isEmpty() {
            return Object2ObjectHashMap.this.isEmpty();
        }

        /**
         * {@inheritDoc}
         */
        public void clear() {
            Object2ObjectHashMap.this.clear();
        }

        /**
         * {@inheritDoc}
         */
        public boolean contains(final Object o) {
            if (!(o instanceof Entry)) {
                return false;
            }

            final Entry<?, ?> entry = (Entry<?, ?>) o;
            final V value = getMapped(entry.getKey());
            return value != null && value.equals(mapNullValue(entry.getValue()));
        }

        /**
         * {@inheritDoc}
         */
        public Object[] toArray() {
            return toArray(new Object[size()]);
        }

        /**
         * {@inheritDoc}
         */
        @SuppressWarnings("unchecked")
        public <T> T[] toArray(final T[] a) {
            final T[] array = a.length >= size
                    ? a : (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);
            final EntryIterator it = iterator();

            for (int i = 0; i < array.length; i++) {
                if (it.hasNext()) {
                    it.next();
                    array[i] = (T) it.allocateDuplicateEntry();
                } else {
                    array[i] = null;
                    break;
                }
            }

            return array;
        }
    }
}
