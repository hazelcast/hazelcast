/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import java.util.*;

/**
 * {@link Map} implementation specialised for long values using open addressing and
 * linear probing for cache efficient access. The implementation is mirror copy of {@link Long2ObjectHashMap}
 * and it also relies on missing value concept from {@link Long2LongHashMap}
 *
 * @param <K> type of keys stored in the {@link Map}
 */
public class Object2LongHashMap<K>
    implements Map<K, Long>
{
    private static final float DEFAULT_LOAD_FACTOR = 0.6F;
    private static final int MIN_CAPACITY = 8;

    private final float loadFactor;
    private final long missingValue;
    private int resizeThreshold;
    private int size;
    private final boolean shouldAvoidAllocation;

    private K[] keys;
    private long[] values;

    private ValueCollection valueCollection;
    private KeySet keySet;
    private EntrySet entrySet;

    /**
     * Construct a map with default capacity and load factor.
     *
     * @param missingValue value to be used as a null maker in the map
     */
    public Object2LongHashMap(final long missingValue)
    {
        this(MIN_CAPACITY, DEFAULT_LOAD_FACTOR, missingValue);
    }

    /**
     * Construct a new map allowing a configuration for initial capacity and load factor.
     *
     * @param initialCapacity for the backing array
     * @param loadFactor      limit for resizing on puts
     * @param missingValue    value to be used as a null marker in the map
     */
    public Object2LongHashMap(
        final int initialCapacity,
        final float loadFactor,
        final long missingValue)
    {
        this(initialCapacity, loadFactor, missingValue, true);
    }

    /**
     * Construct a new map allowing a configuration for initial capacity and load factor.
     * @param initialCapacity       for the backing array
     * @param loadFactor            limit for resizing on puts
     * @param missingValue          value to be used as a null marker in the map
     * @param shouldAvoidAllocation should allocation be avoided by caching iterators and map entries.
     */
    @SuppressWarnings("unchecked")
    public Object2LongHashMap(
        final int initialCapacity,
        final float loadFactor,
        final long missingValue,
        final boolean shouldAvoidAllocation)
    {
        this.loadFactor = loadFactor;
        final int capacity = QuickMath.nextPowerOfTwo(Math.max(MIN_CAPACITY, initialCapacity));
        resizeThreshold = (int)(capacity * loadFactor);

        this.missingValue = missingValue;
        this.shouldAvoidAllocation = shouldAvoidAllocation;
        keys = (K[])new Object[capacity];
        values = new long[capacity];
        Arrays.fill(values, missingValue);
    }

    /**
     * Copy construct a new map from an existing one.
     *
     * @param mapToCopy for construction.
     */
    public Object2LongHashMap(final Object2LongHashMap<K> mapToCopy)
    {
        this.loadFactor = mapToCopy.loadFactor;
        this.resizeThreshold = mapToCopy.resizeThreshold;
        this.size = mapToCopy.size;
        this.missingValue = mapToCopy.missingValue;
        this.shouldAvoidAllocation = mapToCopy.shouldAvoidAllocation;

        keys = mapToCopy.keys.clone();
        values = mapToCopy.values.clone();
    }

    /**
     * The value to be used as a null marker in the map.
     *
     * @return value to be used as a null marker in the map.
     */
    public long missingValue()
    {
        return missingValue;
    }

    /**
     * Get the load factor beyond which the map will increase size.
     *
     * @return load factor for when the map should increase size.
     */
    public float loadFactor()
    {
        return loadFactor;
    }

    /**
     * Get the total capacity for the map to which the load factor will be a fraction of.
     *
     * @return the total capacity for the map.
     */
    public int capacity()
    {
        return values.length;
    }

    /**
     * Get the actual threshold which when reached the map will resize.
     * This is a function of the current capacity and load factor.
     *
     * @return the threshold when the map will resize.
     */
    public int resizeThreshold()
    {
        return resizeThreshold;
    }

    /**
     * {@inheritDoc}
     */
    public int size()
    {
        return size;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isEmpty()
    {
        return 0 == size;
    }

    /**
     * {@inheritDoc}
     * Overloaded version of {@link Map#containsKey(Object)} that takes a primitive long key.
     *
     * @param key for indexing the {@link Map}
     * @return true if the key is found otherwise false.
     */
    public boolean containsKey(final Object key)
    {
        final int mask = values.length - 1;
        int index = Hashing.hash(key, mask);

        boolean found = false;
        while (missingValue != values[index])
        {
            if (key.equals(keys[index]))
            {
                found = true;
                break;
            }

            index = ++index & mask;
        }

        return found;
    }

    /**
     * {@inheritDoc}
     */
    public boolean containsValue(final Object value)
    {
        return containsValue(((Long)value).longValue());
    }

    public boolean containsValue(final long value)
    {
        if (value == missingValue)
        {
            return false;
        }

        boolean found = false;
        for (final long v : values)
        {
            if (value == v)
            {
                found = true;
                break;
            }
        }

        return found;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    public Long get(final Object key)
    {
        return valOrNull(getValue((K)key));
    }

    /**
     * Overloaded version of {@link Map#get(Object)} that takes a primitive long key.
     * Due to type erasure have to rename the method
     *
     * @param key for indexing the {@link Map}
     * @return the value if found otherwise missingValue
     */
    public long getValue(final K key)
    {
        final int mask = values.length - 1;
        int index = Hashing.hash(key, mask);

        long value;
        while (missingValue != (value = values[index]))
        {
            if (key.equals(keys[index]))
            {
                break;
            }

            index = ++index & mask;
        }

        return value;
    }

    /**
     * {@inheritDoc}
     */
    public Long put(final K key, final Long value)
    {
        return valOrNull(put(key, value.longValue()));
    }

    /**
     * Overloaded version of {@link Map#put(Object, Object)} that takes a primitive long key.
     *
     * @param key   for indexing the {@link Map}
     * @param value to be inserted in the {@link Map}
     * @return the previous value if found otherwise missingValue
     */
    public long put(final K key, final long value)
    {
        if (value == missingValue)
        {
            throw new IllegalArgumentException("cannot accept missingValue");
        }

        long oldValue = missingValue;
        final int mask = values.length - 1;
        int index = Hashing.hash(key, mask);

        while (missingValue != values[index])
        {
            if (key.equals(keys[index]))
            {
                oldValue = values[index];
                break;
            }

            index = ++index & mask;
        }

        if (missingValue == oldValue)
        {
            ++size;
            keys[index] = key;
        }

        values[index] = value;

        if (size > resizeThreshold)
        {
            increaseCapacity();
        }

        return oldValue;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    public Long remove(final Object key)
    {
        return valOrNull(removeKey(((K)key)));
    }

    /**
     * Overloaded version of {@link Map#remove(Object)} that takes a primitive long key.
     * Due to type erasure have to rename the method
     *
     * @param key for indexing the {@link Map}
     * @return the value if found otherwise missingValue
     */
    public long removeKey(final K key)
    {
        final int mask = values.length - 1;
        int index = Hashing.hash(key, mask);

        long value;
        while (missingValue != (value = values[index]))
        {
            if (key.equals(keys[index]))
            {
                keys[index] = null;
                values[index] = missingValue;
                --size;

                compactChain(index);
                break;
            }

            index = ++index & mask;
        }

        return value;
    }

    /**
     * {@inheritDoc}
     */
    public void clear()
    {
        if (size > 0)
        {
            Arrays.fill(keys, null);
            Arrays.fill(values, missingValue);
            size = 0;
        }
    }

    /**
     * Compact the {@link Map} backing arrays by rehashing with a capacity just larger than current size
     * and giving consideration to the load factor.
     */
    public void compact()
    {
        final int idealCapacity = (int)Math.round(size() * (1.0d / loadFactor));
        rehash(QuickMath.nextPowerOfTwo(Math.max(MIN_CAPACITY, idealCapacity)));
    }

    /**
     * {@inheritDoc}
     */
    public void putAll(final Map<? extends K, ? extends Long> map)
    {
        for (final Entry<? extends K, ? extends Long> entry : map.entrySet())
        {
            put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * {@inheritDoc}
     */
    public KeySet keySet()
    {
        if (null == keySet)
        {
            keySet = new KeySet();
        }

        return keySet;
    }

    /**
     * {@inheritDoc}
     */
    public ValueCollection values()
    {
        if (null == valueCollection)
        {
            valueCollection = new ValueCollection();
        }

        return valueCollection;
    }

    /**
     * {@inheritDoc}
     */
    public EntrySet entrySet()
    {
        if (null == entrySet)
        {
            entrySet = new EntrySet();
        }

        return entrySet;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        if (isEmpty())
        {
            return "{}";
        }

        final EntryIterator entryIterator = new EntryIterator();
        entryIterator.reset();

        final StringBuilder sb = new StringBuilder().append('{');
        while (true)
        {
            entryIterator.next();
            sb.append(entryIterator.getKey()).append('=').append(entryIterator.getLongValue());
            if (!entryIterator.hasNext())
            {
                return sb.append('}').toString();
            }
            sb.append(',').append(' ');
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (!(o instanceof Map))
        {
            return false;
        }

        final Map<?, ?> that = (Map<?, ?>)o;

        if (size != that.size())
        {
            return false;
        }

        for (int i = 0, length = values.length; i < length; i++)
        {
            final long thisValue = values[i];
            if (missingValue != thisValue)
            {
                final Object thatValueObject = that.get(keys[i]);
                if (!(thatValueObject instanceof Long))
                {
                    return false;
                }

                final long thatValue = (Long)thatValueObject;
                if (missingValue == thatValue || thisValue != thatValue)
                {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        int result = 0;

        for (int i = 0, length = values.length; i < length; i++)
        {
            final long value = values[i];
            if (missingValue != value)
            {
                result += (keys[i].hashCode() ^ Hashing.hashCode(value));
            }
        }

        return result;
    }

    /**
     * Primitive specialised version of {@link #replace(Object, Object)}
     *
     * @param key   key with which the specified value is associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with the specified key, or
     * {@code null} if there was no mapping for the key.
     */
    public long replace(final K key, final long value)
    {
        long curValue = getValue(key);
        if (curValue != missingValue)
        {
            curValue = put(key, value);
        }

        return curValue;
    }

    /**
     * Primitive specialised version of {@link #replace(Object, Object, Object)}
     *
     * @param key      key with which the specified value is associated
     * @param oldValue value expected to be associated with the specified key
     * @param newValue value to be associated with the specified key
     * @return {@code true} if the value was replaced
     */
    public boolean replace(final K key, final long oldValue, final long newValue)
    {
        final long curValue = getValue(key);
        if (curValue == missingValue || curValue != oldValue)
        {
            return false;
        }

        put(key, newValue);

        return true;
    }

    private void increaseCapacity()
    {
        final int newCapacity = values.length << 1;
        if (newCapacity < 0)
        {
            throw new IllegalStateException("max capacity reached at size=" + size);
        }

        rehash(newCapacity);
    }

    private void rehash(final int newCapacity)
    {
        final int mask = newCapacity - 1;
        resizeThreshold = (int)(newCapacity * loadFactor);

        @SuppressWarnings("unchecked")
        final K[] tempKeys = (K[])new Object[newCapacity];
        final long[] tempValues = new long[newCapacity];
        Arrays.fill(tempValues, missingValue);

        for (int i = 0, size = values.length; i < size; i++)
        {
            final long value = values[i];
            if (missingValue != value)
            {
                final K key = keys[i];
                int index = Hashing.hash(key, mask);
                while (missingValue != tempValues[index])
                {
                    index = ++index & mask;
                }

                tempKeys[index] = key;
                tempValues[index] = value;
            }
        }

        keys = tempKeys;
        values = tempValues;
    }

    @SuppressWarnings("FinalParameters")
    private void compactChain(int deleteIndex)
    {
        final int mask = values.length - 1;
        int index = deleteIndex;

        while (true)
        {
            index = ++index & mask;
            if (missingValue == values[index])
            {
                break;
            }

            final int hash = Hashing.hash(keys[index], mask);

            if ((index < hash && (hash <= deleteIndex || deleteIndex <= index)) ||
                (hash <= deleteIndex && deleteIndex <= index))
            {
                keys[deleteIndex] = keys[index];
                values[deleteIndex] = values[index];

                keys[index] = null;
                values[index] = missingValue;
                deleteIndex = index;
            }
        }
    }

    private Long valOrNull(final long value)
    {
        return value == missingValue ? null : value;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Sets and Collections
    ///////////////////////////////////////////////////////////////////////////////////////////////

    public final class KeySet extends AbstractSet<K>
    {
        private final KeyIterator keyIterator = shouldAvoidAllocation ? new KeyIterator() : null;

        /**
         * {@inheritDoc}
         */
        public KeyIterator iterator()
        {
            KeyIterator keyIterator = this.keyIterator;
            if (null == keyIterator)
            {
                keyIterator = new KeyIterator();
            }

            keyIterator.reset();
            return keyIterator;
        }

        public int size()
        {
            return Object2LongHashMap.this.size();
        }

        public boolean contains(final Object o)
        {
            return Object2LongHashMap.this.containsKey(o);
        }

        @SuppressWarnings("unchecked")
        public boolean remove(final Object o)
        {
            return missingValue != Object2LongHashMap.this.removeKey((K)o);
        }

        public void clear()
        {
            Object2LongHashMap.this.clear();
        }
    }

    public final class ValueCollection extends AbstractCollection<Long>
    {
        private final ValueIterator valueIterator = shouldAvoidAllocation ? new ValueIterator() : null;

        /**
         * {@inheritDoc}
         */
        public ValueIterator iterator()
        {
            ValueIterator valueIterator = this.valueIterator;
            if (null == valueIterator)
            {
                valueIterator = new ValueIterator();
            }

            valueIterator.reset();
            return valueIterator;
        }

        public int size()
        {
            return Object2LongHashMap.this.size();
        }

        public boolean contains(final Object o)
        {
            return Object2LongHashMap.this.containsValue(o);
        }

        public void clear()
        {
            Object2LongHashMap.this.clear();
        }
    }

    public final class EntrySet extends AbstractSet<Entry<K, Long>>
    {
        private final EntryIterator entryIterator = shouldAvoidAllocation ? new EntryIterator() : null;

        /**
         * {@inheritDoc}
         */
        public EntryIterator iterator()
        {
            EntryIterator entryIterator = this.entryIterator;
            if (null == entryIterator)
            {
                entryIterator = new EntryIterator();
            }

            entryIterator.reset();
            return entryIterator;
        }

        public int size()
        {
            return Object2LongHashMap.this.size();
        }

        public void clear()
        {
            Object2LongHashMap.this.clear();
        }

        /**
         * {@inheritDoc}
         */
        public boolean contains(final Object o)
        {
            final Entry entry = (Entry)o;
            final Long value = get(entry.getKey());
            return value != null && value.equals(entry.getValue());
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Iterators
    ///////////////////////////////////////////////////////////////////////////////////////////////

    abstract class AbstractIterator<T> implements Iterator<T>
    {
        private int posCounter;
        private int stopCounter;
        private int remaining;
        private boolean isPositionValid = false;

        protected final int position()
        {
            return posCounter & (values.length - 1);
        }

        public boolean hasNext()
        {
            return remaining > 0;
        }

        protected final void findNext()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }

            final long[] values = Object2LongHashMap.this.values;
            final int mask = values.length - 1;

            for (int i = posCounter - 1; i >= stopCounter; i--)
            {
                final int index = i & mask;
                if (missingValue != values[index])
                {
                    posCounter = i;
                    isPositionValid = true;
                    --remaining;

                    return;
                }
            }

            isPositionValid = false;
            throw new IllegalStateException();
        }

        public abstract T next();

        public void remove()
        {
            if (isPositionValid)
            {
                final int position = position();
                values[position] = missingValue;
                keys[position] = null;
                --size;

                compactChain(position);

                isPositionValid = false;
            }
            else
            {
                throw new IllegalStateException();
            }
        }

        final void reset()
        {
            remaining = Object2LongHashMap.this.size;
            final long[] values = Object2LongHashMap.this.values;
            final int capacity = values.length;

            int i = capacity;
            if (missingValue != values[capacity - 1])
            {
                for (i = 0; i < capacity; i++)
                {
                    if (missingValue == values[i])
                    {
                        break;
                    }
                }
            }

            stopCounter = i;
            posCounter = i + capacity;
            isPositionValid = false;
        }
    }

    public final class ValueIterator extends AbstractIterator<Long>
    {
        public Long next()
        {
            return nextLong();
        }

        public long nextLong()
        {
            findNext();

            return values[position()];
        }
    }

    public final class KeyIterator extends AbstractIterator<K>
    {
        public K next()
        {
            findNext();

            return keys[position()];
        }
    }

    @SuppressFBWarnings(value = "PZ_DONT_REUSE_ENTRY_OBJECTS_IN_ITERATORS")
    public final class EntryIterator
        extends AbstractIterator<Entry<K, Long>>
        implements Entry<K, Long>
    {
        public Entry<K, Long> next()
        {
            findNext();
            if (shouldAvoidAllocation)
            {
                return this;
            }

            return allocateDuplicateEntry();
        }

        private Entry<K, Long> allocateDuplicateEntry()
        {
            final K k = getKey();
            final long v = getLongValue();

            return new Entry<K, Long>()
            {
                public K getKey()
                {
                    return k;
                }

                public Long getValue()
                {
                    return v;
                }

                public Long setValue(final Long value)
                {
                    return Object2LongHashMap.this.put(k, value);
                }

                public int hashCode()
                {
                    return getKey().hashCode() ^ Hashing.hashCode(getLongValue());
                }

                public boolean equals(final Object o)
                {
                    if (!(o instanceof Entry))
                    {
                        return false;
                    }

                    final Entry e = (Entry)o;

                    return (e.getKey() != null && e.getValue() != null) &&
                        (e.getKey().equals(k) && e.getValue().equals(v));
                }

                public String toString()
                {
                    return k + "=" + v;
                }
            };
        }

        public K getKey()
        {
            return keys[position()];
        }

        public long getLongValue()
        {
            return values[position()];
        }

        public Long getValue()
        {
            return getLongValue();
        }

        public Long setValue(final Long value)
        {
            return setValue(value.longValue());
        }

        public long setValue(final long value)
        {
            if (value == missingValue)
            {
                throw new IllegalArgumentException("cannot accept missingValue");
            }

            final int pos = position();
            final long oldValue = values[pos];
            values[pos] = value;

            return oldValue;
        }
    }
}
