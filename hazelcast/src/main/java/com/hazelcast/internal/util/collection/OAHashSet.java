/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.hazelcast.internal.nio.Bits.FLOAT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Not thread-safe open-addressing hash {@link Set} implementation with linear
 * probing for CPU cache efficiency. This implementation caches the hashes
 * of the elements stored in the set. This caching enables avoiding
 * expensive {@link #hashCode()} calls when rehashing at the cost of the
 * increased memory consumption.
 * <p>
 * Besides avoiding {@link #hashCode()} calls on rehashing, this
 * implementation offers methods that accept the hash together with the
 * element if it is already known on the caller side.
 * See {@link #add(Object, int)}, {@link  #contains(Object, int)}, {@link #remove(Object, int)}.
 * <p>
 * This {@link Set} implementation does not permit null elements.
 * <p>
 * This {@link Set} implementation does not permit concurrent modifications
 * during iteration.
 * <p>
 * Please note that this {@link Set} implementation does not shrink when
 * elements are removed.
 *
 * @param <E> The type of the elements stored in the set
 */
public class OAHashSet<E> extends AbstractSet<E> {
    private static final int DEFAULT_INITIAL_CAPACITY = 16;
    private static final float DEFAULT_LOAD_FACTOR = 0.6F;

    private final float loadFactor;

    private int[] hashes;
    private Object[] table;
    private int resizeThreshold;
    private int capacity;
    private int mask;
    private int size;
    /**
     * The version of this set. Used to detect concurrent modification when
     * iterating over the elements in the set with {@link ElementIterator}
     */
    private int version;

    /**
     * Constructs an {@link OAHashSet} instance with default initial
     * capacity and default load factor
     *
     * @see #DEFAULT_INITIAL_CAPACITY
     * @see #DEFAULT_LOAD_FACTOR
     */
    public OAHashSet() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Constructs an {@link OAHashSet} instance with the specified
     * initial capacity and with the default load factor
     *
     * @param initialCapacity the initial capacity of the set to be created
     * @see #DEFAULT_LOAD_FACTOR
     */
    public OAHashSet(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Constructs an {@link OAHashSet} instance with the specified
     * initial capacity and load factor
     *
     * @param initialCapacity the initial capacity of the set to be created
     * @param loadFactor      the load factor of the set to be created
     */
    public OAHashSet(int initialCapacity, float loadFactor) {
        // the parameter checks below are intentionally not done via Preconditions
        // the error messages provided to the preconditions are created unconditionally
        // which creates plenty StringBuilders and for building the error message
        // if many instances are created in a loop this increases the GC pressure significantly
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Illegal initial capacity: " + initialCapacity);
        }

        if (loadFactor <= 0 || loadFactor >= 1 || Float.isNaN(loadFactor)) {
            throw new IllegalArgumentException("Illegal load factor: " + loadFactor);
        }

        this.capacity = QuickMath.nextPowerOfTwo(initialCapacity);
        this.loadFactor = loadFactor;
        this.resizeThreshold = (int) (capacity * loadFactor);
        this.mask = capacity - 1;
        this.hashes = new int[capacity];
        this.table = new Object[capacity];
    }

    @Override
    public boolean add(E element) {
        return add(element, element.hashCode());
    }

    /**
     * Adds the specified element to this set if it is not already present.
     * <p>
     * This variant of {@link #add(Object)} acts as an optimisation to
     * enable avoiding {@link #hashCode()} calls if the hash is already
     * known on the caller side.
     *
     * @param elementToAdd element to be added to this set
     * @param hash         the hash of the element to be added
     * @return <tt>true</tt> if this set did not already contain the specified
     * element
     * @see #add(Object)
     */
    public boolean add(E elementToAdd, int hash) {
        checkNotNull(elementToAdd);

        int index = hash & mask;

        // using the hashes array for looping and comparison if possible, hence we're cache friendly
        while (hashes[index] != 0 || table[index] != null) {
            if (hash == hashes[index] && elementToAdd.equals(table[index])) {
                return false;
            }
            index = ++index & mask;
        }

        size++;
        version++;

        table[index] = elementToAdd;
        hashes[index] = hash;

        if (size > resizeThreshold) {
            increaseCapacity();
        }

        return true;
    }

    @Override
    public boolean contains(Object objectToCheck) {
        return contains(objectToCheck, objectToCheck.hashCode());
    }

    /**
     * Returns <tt>true</tt> if this set contains the specified element
     * with the hash provided in parameter.
     * <p>
     * This variant of {@link #contains(Object)} acts as an optimisation to
     * enable avoiding {@link #hashCode()} calls if the hash is already
     * known on the caller side.
     *
     * @param objectToCheck element whose presence in this set is to be tested
     * @param hash          the hash of the element to be tested
     * @return <tt>true</tt> if this set contains the specified element
     * @see #contains(Object)
     */
    public boolean contains(Object objectToCheck, int hash) {
        checkNotNull(objectToCheck);

        int index = hash & mask;

        // using the hashes array for looping and comparison if possible, hence we're cache friendly
        while (hashes[index] != 0 || table[index] != null) {
            if (hash == hashes[index] && objectToCheck.equals(table[index])) {
                return true;
            }
            index = ++index & mask;
        }

        return false;
    }

    @Override
    public boolean remove(Object objectToRemove) {
        return remove(objectToRemove, objectToRemove.hashCode());
    }

    /**
     * Removes the specified element from this set if it is present with
     * the hash provided in parameter.
     * <p>
     * This variant of {@link #remove(Object)} acts as an optimisation to
     * enable avoiding {@link #hashCode()} calls if the hash is already
     * known on the caller side.
     *
     * @param objectToRemove object to be removed from this set, if present
     * @param hash           the hash of the element to be removed
     * @return <tt>true</tt> if this set contained the specified element
     * @see #remove(Object)
     */
    public boolean remove(Object objectToRemove, int hash) {
        checkNotNull(objectToRemove);

        int index = hash & mask;

        // using the hashes array for looping and comparison if possible, hence we're cache friendly
        while (hashes[index] != 0 || table[index] != null) {
            if (hash == hashes[index] && objectToRemove.equals(table[index])) {
                removeFromIndex(index);

                return true;
            }
            index = ++index & mask;
        }

        return false;
    }

    @Override
    public boolean removeAll(Collection<?> elementsToRemove) {
        boolean setChanged = false;
        for (Object objectToRemove : elementsToRemove) {
            setChanged |= remove(objectToRemove.hashCode());
        }

        return setChanged;
    }

    @Override
    public boolean retainAll(Collection<?> elementsToRetain) {
        boolean setChanged = false;
        final int sizeBeforeRemovals = size;
        int visited = 0;

        for (int index = 0; index < table.length && visited < sizeBeforeRemovals; index++) {
            final Object storedElement = table[index];
            if (storedElement != null) {
                visited++;
                if (!elementsToRetain.contains(storedElement)) {
                    removeFromIndex(index);
                    setChanged = true;
                }
            }
        }

        return setChanged;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Iterator<E> iterator() {
        return new ElementIterator();
    }

    @Override
    public Object[] toArray() {
        return toArray(new Object[size]);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] array) {
        if (array.length < size) {
            array = (T[]) new Object[size];
        }

        int arrIdx = 0;
        for (int i = 0; i < table.length && arrIdx < size; i++) {
            if (table[i] != null) {
                array[arrIdx++] = (T) table[i];
            }
        }
        return array;
    }

    @Override
    public void clear() {
        size = 0;
        Arrays.fill(hashes, 0);
        Arrays.fill(table, null);
        ++version;
    }

    /**
     * Returns the capacity of the set
     *
     * @return the capacity of the set
     */
    public int capacity() {
        return capacity;
    }

    /**
     * Returns the current memory consumption (in bytes)
     *
     * @return the current memory consumption
     */
    @SuppressWarnings("checkstyle:trailingcomment")
    public long footprint() {
        return
                INT_SIZE_IN_BYTES * hashes.length // size of hashes array
                + REFERENCE_COST_IN_BYTES * table.length // size of table array
                + REFERENCE_COST_IN_BYTES // reference to hashes array
                + REFERENCE_COST_IN_BYTES // reference to table array
                + FLOAT_SIZE_IN_BYTES // loadFactor
                + INT_SIZE_IN_BYTES // resizeThreshold
                + INT_SIZE_IN_BYTES // capacity
                + INT_SIZE_IN_BYTES // mask
                + INT_SIZE_IN_BYTES // size
                + INT_SIZE_IN_BYTES; // version
    }

    /**
     * Returns the load factor of the set
     *
     * @return the load factor of the set
     */
    public float loadFactor() {
        return loadFactor;
    }

    @Override
    public int hashCode() {
        int hashCode = 0;
        for (int hash : hashes) {
            hashCode += hash;
        }
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o);
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
        final Object[] newTable = new Object[capacity];
        final int[] newHashes = new int[capacity];

        for (int i = 0; i < table.length; i++) {
            final Object element = table[i];
            if (element != null) {
                int index = hashes[i] & mask;
                while (null != newTable[index]) {
                    index = ++index & mask;
                }
                newTable[index] = element;
                newHashes[index] = hashes[i];
            }
        }

        table = newTable;
        hashes = newHashes;
    }

    private void removeFromIndex(int index) {
        hashes[index] = 0;
        table[index] = null;
        size--;
        version++;

        compactChain(index);
    }

    private void compactChain(final int indexOfRemoved) {
        int deleteIndex = indexOfRemoved;
        int index = deleteIndex;

        while (true) {
            index = ++index & mask;

            if (null == table[index]) {
                return;
            }

            final int hashedIndex = hashes[index] & mask;
            if ((index < hashedIndex && (hashedIndex <= deleteIndex || deleteIndex <= index))
                    || (hashedIndex <= deleteIndex && deleteIndex <= index)) {
                hashes[deleteIndex] = hashes[index];
                table[deleteIndex] = table[index];
                hashes[index] = 0;
                table[index] = null;
                deleteIndex = index;
            }
        }
    }

    private final class ElementIterator implements Iterator<E> {

        /**
         * The version of the set at which the iterator is constructed.
         *
         * @see #version
         */
        private final int expectedVersion;
        private int position;
        private int index;

        private ElementIterator() {
            this.expectedVersion = OAHashSet.this.version;
        }

        @Override
        public boolean hasNext() {
            return position < size;
        }

        @Override
        @SuppressWarnings("unchecked")
        public E next() {
            if (version != expectedVersion) {
                throw new ConcurrentModificationException();
            }

            for (; index < table.length && position < size; index++) {
                if (table[index] != null) {
                    position++;
                    // we need to make sure that the index advances, hence index++
                    return (E) table[index++];
                }
            }

            throw new NoSuchElementException();
        }

        /**
         * {@inheritDoc}
         *
         * <p>
         *
         * Remove is not permitted in this implementation, since removals
         * come with compaction, which may cause some elements to be
         * missed by the iterator.
         */
        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }
    }
}
