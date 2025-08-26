/*
 * Original work Copyright 2015 Real Logic Ltd.
 * Modified work Copyright (c) 2015-2025, Hazelcast, Inc. All Rights Reserved.
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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;
import static com.hazelcast.internal.util.collection.Hashing.longHash;

/**
 * Simple fixed-size long hashset.
 */
@SuppressWarnings("checkstyle:methodcount")
public final class LongHashSet implements Set<Long> {
    /** Maximum supported capacity */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int MAX_CAPACITY = 1 << 29;
    private final long[] values;
    private final LongIterator iterator;
    private final int capacity;
    private final int mask;
    private final long missingValue;

    private int size;

    public LongHashSet(final int capacity, final long missingValue) {
        checkTrue(capacity <= MAX_CAPACITY, "Maximum capacity is 2^29");
        this.capacity = capacity;
        size = 0;
        this.missingValue = missingValue;
        final int arraySize = nextPowerOfTwo(2 * capacity);
        mask = arraySize - 1;
        values = new long[arraySize];
        Arrays.fill(values, missingValue);

        // NB: references values in the constructor, so must be assigned after values
        iterator = new LongIterator(missingValue, values);
    }

    public LongHashSet(long[] items, long missingValue) {
        this(items.length, missingValue);
        for (long item : items) {
            add(item);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean add(final Long value) {
        return add(value.longValue());
    }

    /**
     * Primitive specialised overload of {this#add(Long)}
     *
     * @param value the value to add
     * @return true if the collection has changed, false otherwise
     */
    public boolean add(final long value) {
        if (size == capacity) {
            throw new IllegalStateException("This LongHashSet of capacity " + capacity + " is full");
        }
        int index = longHash(value, mask);

        while (values[index] != missingValue) {
            if (values[index] == value) {
                return false;
            }

            index = next(index);
        }

        values[index] = value;
        size++;

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(final Object value) {
        return value instanceof Long l && remove(l.longValue());
    }

    /**
     * A long specialised version of {this#remove(Object)}.
     *
     * @param value the value to remove
     * @return true if the value was present, false otherwise
     */
    public boolean remove(final long value) {
        int index = longHash(value, mask);

        while (values[index] != missingValue) {
            if (values[index] == value) {
                values[index] = missingValue;
                compactChain(index);
                size--;
                return true;
            }

            index = next(index);
        }

        return false;
    }

    private int next(int index) {
        return (index + 1) & mask;
    }

    private void compactChain(int deleteIndex) {
        final long[] values = this.values;
        int index = deleteIndex;
        while (true) {
            index = next(index);
            if (values[index] == missingValue) {
                return;
            }
            final int hash = longHash(values[index], mask);
            if ((index < hash && (hash <= deleteIndex || deleteIndex <= index))
                    || (hash <= deleteIndex && deleteIndex <= index)
            ) {
                values[deleteIndex] = values[index];
                values[index] = missingValue;
                deleteIndex = index;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(final Object value) {
        return value instanceof Long l && contains(l.longValue());
    }

    public boolean contains(final long value) {
        int index = longHash(value, mask);

        while (values[index] != missingValue) {
            if (values[index] == value) {
                return true;
            }

            index = next(index);
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clear() {
        final long[] values = this.values;
        final int length = values.length;
        for (int i = 0; i < length; i++) {
            values[i] = missingValue;
        }
        size = 0;
    }

    /**
     * {@inheritDoc}
     */
    public boolean addAll(final Collection<? extends Long> coll) {
        return addAllCapture(coll);
    }

    private <E extends Long> boolean addAllCapture(final Collection<E> coll) {
        final Predicate<E> p = this::add;
        return conjunction(coll, p);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsAll(final Collection<?> coll) {
        return containsAllCapture(coll);
    }

    private <E> boolean containsAllCapture(Collection<E> coll) {
        return conjunction(coll, this::contains);
    }

    /**
     * LongHashSet specialised variant of {this#containsAll(Collection)}.
     *
     * @param other the long hashset to compare against.
     * @return true if every element in other is in this.
     */
    public boolean containsAll(final LongHashSet other) {
        final LongIterator iterator = other.iterator();
        while (iterator.hasNext()) {
            if (!contains(iterator.nextValue())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Fast Path set difference for comparison with another LongHashSet.
     * <p>
     * NB: garbage free in the identical case, allocates otherwise.
     *
     * @param collection the other set to subtract
     * @return null if identical, otherwise the set of differences
     */
    public LongHashSet difference(final LongHashSet collection) {
        checkNotNull(collection);

        LongHashSet difference = null;

        final LongIterator it = iterator();

        while (it.hasNext()) {
            final long value = it.nextValue();
            if (!collection.contains(value)) {
                if (difference == null) {
                    difference = new LongHashSet(size, missingValue);
                }

                difference.add(value);
            }
        }

        return difference;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean removeAll(final Collection<?> coll) {
        return removeAllCapture(coll);
    }

    private <E> boolean removeAllCapture(final Collection<E> coll) {
        return conjunction(coll, this::remove);
    }

    private static <T> boolean conjunction(final Collection<T> collection, final Predicate<T> predicate) {
        checkNotNull(collection);

        boolean acc = false;
        for (final T t : collection) {
            // Deliberate strict evaluation
            acc |= predicate.test(t);
        }

        return acc;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LongIterator iterator() {
        iterator.reset();
        return iterator;
    }

    public void copy(final LongHashSet obj) {
        // NB: mask also implies the length is the same
        if (this.mask != obj.mask) {
            throw new IllegalArgumentException("Cannot copy object: masks not equal");
        }

        if (this.missingValue != obj.missingValue) {
            throw new IllegalArgumentException("Cannot copy object: missingValues not equal");
        }

        System.arraycopy(obj.values, 0, this.values, 0, this.values.length);
        this.size = obj.size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        final StringBuilder b = new StringBuilder(size() * 3 + 2);
        b.append('{');
        String separator = "";
        for (long i : values) {
            if (i == missingValue) {
                continue;
            }
            b.append(separator).append(i);
            separator = ",";
        }
        return b.append('}').toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object[] toArray() {
        final long[] values = this.values;
        final Object[] array = new Object[this.size];
        int i = 0;
        for (long value : values) {
            if (value != missingValue) {
                array[i++] = value;
            }
        }
        return array;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] into) {
        checkNotNull(into);
        final Class<?> aryType = into.getClass().getComponentType();
        if (!aryType.isAssignableFrom(Long.class)) {
            throw new ArrayStoreException("Cannot store Longs in array of type " + aryType);
        }
        final long[] values = this.values;
        final Object[] ret = into.length >= this.size ? into : (T[]) Array.newInstance(aryType, this.size);
        int i = 0;
        for (long value : values) {
            if (value != missingValue) {
                ret[i++] = value;
            }
        }
        if (ret.length > this.size) {
            ret[values.length] = null;
        }
        return (T[]) ret;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object other) {
        if (other == this) {
            return true;
        }

        if (other instanceof LongHashSet otherSet) {
            return otherSet.missingValue == missingValue && otherSet.size() == size() && containsAll(otherSet);
        }

        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final LongIterator iterator = iterator();
        int total = 0;
        while (iterator.hasNext()) {
            // Cast exists for substitutions
            total += (long) iterator.nextValue();
        }
        return total;
    }

    // --- Unimplemented below here

    @Override
    public boolean retainAll(final Collection<?> coll) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
