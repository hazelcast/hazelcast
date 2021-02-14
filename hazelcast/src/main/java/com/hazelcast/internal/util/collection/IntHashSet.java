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

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;
import static com.hazelcast.internal.util.collection.Hashing.intHash;

/**
 * Simple fixed-size int hashset.
 */
@SuppressWarnings("checkstyle:methodcount")
public final class IntHashSet implements Set<Integer> {
    /** Maximum supported capacity */
    @SuppressWarnings("checkstyle:magicnumber")
    public static final int MAX_CAPACITY = 1 << 29;
    private final int[] values;
    private final IntIterator iterator;
    private final int capacity;
    private final int mask;
    private final int missingValue;

    private int size;

    public IntHashSet(final int capacity, final int missingValue) {
        checkTrue(capacity <= MAX_CAPACITY, "Maximum capacity is 2^29");
        this.capacity = capacity;
        size = 0;
        this.missingValue = missingValue;
        final int arraySize = nextPowerOfTwo(2 * capacity);
        mask = arraySize - 1;
        values = new int[arraySize];
        Arrays.fill(values, missingValue);

        // NB: references values in the constructor, so must be assigned after values
        iterator = new IntIterator(missingValue, values);
    }

    @Override
    public boolean add(final Integer value) {
        return add(value.intValue());
    }

    /**
     * Primitive specialised overload of {this#add(Integer)}
     *
     * @param value the value to add
     * @return true if the collection has changed, false otherwise
     */
    public boolean add(final int value) {
        if (size == capacity) {
            throw new IllegalStateException("This IntHashSet of capacity " + capacity + " is full");
        }
        int index = intHash(value, mask);

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

    @Override
    public boolean remove(final Object value) {
        return value instanceof Integer && remove(((Integer) value).intValue());
    }

    /**
     * An int specialised version of {this#remove(Object)}.
     *
     * @param value the value to remove
     * @return true if the value was present, false otherwise
     */
    public boolean remove(final int value) {
        int index = intHash(value, mask);

        while (values[index] != missingValue) {
            if (values[index] == value) {
                values[index] = missingValue;
                compactChain(index);
                return true;
            }

            index = next(index);
        }

        return false;
    }

    private int next(int index) {
        index = ++index & mask;
        return index;
    }

    private void compactChain(int deleteIndex) {
        final int[] values = this.values;
        int index = deleteIndex;
        while (true) {
            index = next(index);
            if (values[index] == missingValue) {
                return;
            }
            final int hash = intHash(values[index], mask);
            if ((index < hash && (hash <= deleteIndex || deleteIndex <= index))
                    || (hash <= deleteIndex && deleteIndex <= index)
            ) {
                values[deleteIndex] = values[index];
                values[index] = missingValue;
                deleteIndex = index;
            }
        }
    }

    @Override
    public boolean contains(final Object value) {
        return value instanceof Integer && contains(((Integer) value).intValue());
    }

    public boolean contains(final int value) {
        int index = intHash(value, mask);

        while (values[index] != missingValue) {
            if (values[index] == value) {
                return true;
            }

            index = next(index);
        }

        return false;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public void clear() {
        final int[] values = this.values;
        final int length = values.length;
        for (int i = 0; i < length; i++) {
            values[i] = missingValue;
        }
        size = 0;
    }

    @Override
    public boolean addAll(final Collection<? extends Integer> coll) {
        return addAllCapture(coll);
    }

    private <E extends Integer> boolean addAllCapture(final Collection<E> coll) {
        final Predicate<E> p = new Predicate<E>() {
            @Override
            public boolean test(E x) {
                return add(x);
            }
        };
        return conjunction(coll, p);
    }

    @Override
    public boolean containsAll(final Collection<?> coll) {
        return containsAllCapture(coll);
    }

    private <E> boolean containsAllCapture(Collection<E> coll) {
        return conjunction(coll, new Predicate<E>() {
            @Override public boolean test(E value) {
                return contains(value);
            }
        });
    }

    /**
     * IntHashSet specialised variant of {this#containsAll(Collection)}.
     *
     * @param other the int hashset to compare against.
     * @return true if every element in other is in this.
     */
    public boolean containsAll(final IntHashSet other) {
        final IntIterator iterator = other.iterator();
        while (iterator.hasNext()) {
            if (!contains(iterator.nextValue())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Fast Path set difference for comparison with another IntHashSet.
     * <p>
     * NB: garbage free in the identical case, allocates otherwise.
     *
     * @param collection the other set to subtract
     * @return null if identical, otherwise the set of differences
     */
    public IntHashSet difference(final IntHashSet collection) {
        checkNotNull(collection, "Collection must not be null");

        IntHashSet difference = null;

        final IntIterator it = iterator();

        while (it.hasNext()) {
            final int value = it.nextValue();
            if (!collection.contains(value)) {
                if (difference == null) {
                    difference = new IntHashSet(size, missingValue);
                }

                difference.add(value);
            }
        }

        return difference;
    }

    @Override
    public boolean removeAll(final Collection<?> coll) {
        return removeAllCapture(coll);
    }

    private <E> boolean removeAllCapture(final Collection<E> coll) {
        return conjunction(coll, new Predicate<E>() {
            @Override public boolean test(E value) {
                return remove(value);
            }
        });
    }

    private static <E> boolean conjunction(final Collection<E> collection, final Predicate<E> predicate) {
        checkNotNull(collection);

        boolean acc = false;
        for (final E e : collection) {
            // Deliberate strict evaluation
            acc |= predicate.test(e);
        }

        return acc;
    }

    @Override
    public IntIterator iterator() {
        iterator.reset();
        return iterator;
    }

    public void copy(final IntHashSet obj) {
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

    @Override
    public String toString() {
        final StringBuilder b = new StringBuilder(size() * 3 + 2);
        b.append('{');
        String separator = "";
        for (int i : values) {
            b.append(i).append(separator);
            separator = ",";
        }
        return b.append('}').toString();
    }

    @Override
    public Object[] toArray() {
        final int[] values = this.values;
        final Object[] array = new Object[this.size];
        int i = 0;
        for (int value : values) {
            if (value != missingValue) {
                array[i++] = value;
            }
        }
        return array;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] into) {
        checkNotNull(into);
        final Class<?> aryType = into.getClass().getComponentType();
        if (!aryType.isAssignableFrom(Integer.class)) {
            throw new ArrayStoreException("Cannot store Integers in array of type " + aryType);
        }
        final int[] values = this.values;
        final Object[] ret = into.length >= this.size ? into : (T[]) Array.newInstance(aryType, this.size);
        int i = 0;
        for (int value : values) {
            if (value != missingValue) {
                ret[i++] = value;
            }
        }
        if (ret.length > this.size) {
            ret[values.length] = null;
        }
        return (T[]) ret;
    }

    public boolean equals(final Object other) {
        if (other == this) {
            return true;
        }

        if (other instanceof IntHashSet) {
            final IntHashSet otherSet = (IntHashSet) other;
            return otherSet.missingValue == missingValue && otherSet.size() == size() && containsAll(otherSet);
        }

        return false;
    }

    public int hashCode() {
        final IntIterator iterator = iterator();
        int total = 0;
        while (iterator.hasNext()) {
            // Cast exists for substitutions
            total += (int) iterator.nextValue();
        }
        return total;
    }

    // --- Unimplemented below here

    @Override
    public boolean retainAll(final Collection<?> coll) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
