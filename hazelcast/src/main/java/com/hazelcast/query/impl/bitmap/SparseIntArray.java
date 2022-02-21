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

package com.hazelcast.query.impl.bitmap;

import static com.hazelcast.query.impl.bitmap.BitmapUtils.capacityDeltaInt;
import static com.hazelcast.query.impl.bitmap.BitmapUtils.capacityDeltaShort;
import static com.hazelcast.query.impl.bitmap.BitmapUtils.denseCapacityDeltaInt;
import static com.hazelcast.query.impl.bitmap.BitmapUtils.denseCapacityDeltaShort;
import static com.hazelcast.query.impl.bitmap.BitmapUtils.toUnsignedInt;
import static com.hazelcast.query.impl.bitmap.BitmapUtils.toUnsignedLong;
import static com.hazelcast.query.impl.bitmap.BitmapUtils.unsignedBinarySearch;
import static java.lang.System.arraycopy;
import static java.util.Arrays.copyOf;

/**
 * Stores values of type {@code E} indexable by {@code int} indexes interpreted
 * as unsigned.
 * <p>
 * Internally, uses top-level storage ({@link Storage32 Storage32}) which goes
 * in two flavors:
 * <ul>
 * <li>{@link ArrayStorage32 ArrayStorage32} which manages arrays of index-value
 * pairs and can operate in two modes: dense mode with directly indexable values
 * array and sparse mode with sorted indexes and values arrays.
 * <li>{@link PrefixStorage32 PrefixStorage32} which splits 32-bit indexes
 * into 16-bit prefixes and 16-bit postfixes. The high 16 bits are stored in
 * sorted array and used to lookup a storage ({@link Storage16 Storage16}) for
 * the low 16 bits.
 * </ul>
 * <p>
 * {@link Storage16 Storage16} also manages arrays of index-value pairs and can
 * operate in two modes: dense and sparse.
 * <p>
 * The implementation switches between various storage flavors once certain
 * thresholds on storage size are reached.
 * <p>
 * Empty storages are never stored by the implementation.
 */
class SparseIntArray<E> {

    /**
     * The size at which ArrayStorage32 is converted to PrefixStorage32.
     * Inferred empirically: at this size we are not penalized too much for
     * doing binary searches.
     */
    public static final int ARRAY_STORAGE_32_MAX_SPARSE_SIZE = 513;

    /**
     * Sets a limit on maximum dense ArrayStorage32 size to avoid stressing GC
     * too much.
     */
    private static final int ARRAY_STORAGE_32_MAX_DENSE_SIZE = 262145;

    private static final int STORAGE_16_MAX_DENSE_SIZE = 64 * 1024;
    private static final int STORAGE_16_MAX_SPARSE_SIZE = 64 * 1024;

    private static final long SHORT_PREFIX_MASK_LONG = 0xFFFF0000L;
    private static final int SHORT_PREFIX_MASK_INT = 0xFFFF0000;

    private Storage32 storage = new ArrayStorage32();

    /**
     * Iterates over values of a sparse array in ascending index order.
     */
    public static class Iterator<T> {

        /**
         * Identifies an iterator end.
         */
        public static final long END = -1;

        // the current position of Storage32
        private int position32;

        // the current Storage16
        private Storage16 storage16;
        // its position
        private int position16;

        private T value;

        /**
         * Returns a value this iterator is currently at. If this iterator is
         * already reached its end, the return value is undefined.
         * <p>
         * Just after the creation, iterators are positioned at their first value.
         */
        public final T getValue() {
            return value;
        }

    }

    /**
     * @return the value stored inside this sparse array at the given index or
     * {@code null} if nothing stored at it.
     */
    @SuppressWarnings("unchecked")
    public E get(int index) {
        return (E) storage.get(index);
    }

    /**
     * Sets or replaces a value at the given index in this sparse array to the
     * new given value.
     *
     * @param index the index to set value at.
     * @param value the value to set.
     */
    public void set(int index, E value) {
        assert value != null;
        Storage32 newStorage = storage.set(index, value);
        if (newStorage != storage) {
            storage = newStorage;
        }
    }

    /**
     * Clears a value stored at the given index in this sparse array.
     *
     * @param index the index to clear a value at.
     * @return {@code true} if this sparse array is emptied, {@code false} if
     * this sparse array still contains some values.
     */
    public boolean clear(int index) {
        Storage32 newStorage = storage.clear(index);
        if (newStorage == null) {
            return true;
        }

        if (newStorage != storage) {
            storage = newStorage;
        }
        return false;
    }

    /**
     * Clears this sparse array entirely.
     */
    public void clear() {
        storage = new ArrayStorage32();
    }

    /**
     * Starts iteration on this sparse array using the given iterator.
     *
     * @param iterator the iterator to iterate with.
     * @return an index at which the iterator is currently at or {@link
     * Iterator#END END} if this sparse array is empty.
     */
    public long iterate(Iterator<E> iterator) {
        return storage.iterate(iterator);
    }

    /**
     * Advances the given iterator on this sparse array.
     *
     * @param iterator the iterator to advance.
     * @return an index at which this iterator was positioned before the
     * advancement or {@link Iterator#END END} if this iterator already was at
     * its end.
     */
    public long advance(int current, Iterator<E> iterator) {
        return storage.advance(current, iterator);
    }

    /**
     * Starts iteration on this sparse array starting at least from the given
     * index using the given iterator.
     *
     * @param index    the index to start the iteration from.
     * @param iterator the iterator to iterate with.
     * @return an index at which the iterator is currently at; or {@link
     * Iterator#END END} if no index greater than or equal to the given index
     * exists in this array.
     */
    public long iterateAtLeastFrom(int index, Iterator<E> iterator) {
        return storage.iterateAtLeastFrom(index, iterator);
    }

    /**
     * Advances the given iterator to the given index; or, if the index is
     * not present in this sparse array, to an index immediately following it
     * and present in this array.
     *
     * @param index    the index to advance at least to. The index must be
     *                 greater than the index this iterator is currently at.
     * @param iterator the iterator to advance.
     * @return an index at which this iterator was advanced to or {@link
     * Iterator#END END} if this iterator reached its end.
     */
    public long advanceAtLeastTo(int index, int current, Iterator<E> iterator) {
        return storage.advanceAtLeastTo(index, current, iterator);
    }

    /**
     * Defines internal contract of storages responsible for working on full
     * 32-bit indexes.
     */
    private interface Storage32 {

        /**
         * Sets or replaces a value at the given index to the new given value.
         *
         * @param index the index to set value at.
         * @param value the value to set.
         * @return a new storage instance if this storage was converted to
         * another storage flavor; this storage otherwise.
         */
        Storage32 set(int index, Object value);

        /**
         * Clears a value stored at the given index in this storage.
         *
         * @param index the index to clear a value at.
         * @return {@code null} if this storage array is emptied, a new storage
         * instance if this storage was converted to another storage flavor;
         * this storage otherwise.
         */
        Storage32 clear(int index);

        /**
         * @return the value stored inside this storage at the given index or
         * {@code null} if nothing stored at it.
         */
        Object get(int index);

        /**
         * Starts iteration on this storage using the given iterator.
         *
         * @param iterator the iterator to iterate with.
         * @return an index at which the iterator is currently at or {@link
         * Iterator#END END} if this storage is empty.
         */
        long iterate(Iterator iterator);

        /**
         * Advances the given iterator on this storage.
         *
         * @param iterator the iterator to advance.
         * @return an index at which this iterator was positioned before the
         * advancement or {@link Iterator#END END} if this iterator already was
         * at its end.
         */
        long advance(int current, Iterator iterator);

        /**
         * Starts iteration on this storage starting at least from the given
         * index using the given iterator.
         *
         * @param index    the index to start the iteration from.
         * @param iterator the iterator to iterate with.
         * @return an index at which the iterator is currently at; or {@link
         * Iterator#END END} if no index greater than or equal to the given
         * index exists in this storage.
         */
        long iterateAtLeastFrom(int index, Iterator iterator);

        /**
         * Advances the given iterator to the given index; or, if the index is
         * not present in this storage, to an index immediately following it
         * and present in this storage.
         *
         * @param index    the index to advance at least to. The index must be
         *                 greater than the index this iterator is currently at.
         * @param iterator the iterator to advance.
         * @return an index at which thee iterator was advanced to or {@link
         * Iterator#END END} if the iterator reached its end.
         */
        long advanceAtLeastTo(int index, int current, Iterator iterator);

    }

    /**
     * Manages ordered arrays of index-value pairs for 32-bit indexes.
     * <p>
     * Works in two modes:
     * <ul>
     * <li>Dense mode with directly indexable values array.
     * <li>Sparse mode with sorted indexes and values arrays.
     * </ul>
     */
    private static final class ArrayStorage32 implements Storage32 {

        private static final int MIN_CAPACITY = 1;

        private int size;
        private int[] indexes;
        private Object[] values = new Object[MIN_CAPACITY];

        @Override
        public Storage32 set(int index, Object value) {
            if (indexes == null) {
                return setDense(index, value);
            } else {
                return setSparse(index, value);
            }
        }

        @SuppressWarnings("checkstyle:npathcomplexity")
        private Storage32 setDense(int index, Object value) {
            long unsignedIndex = toUnsignedLong(index);

            if (unsignedIndex < values.length) {
                // The index is inside the bounds: just set the corresponding
                // array slot.

                if (values[index] == null) {
                    ++size;
                }
                values[index] = value;
                return this;
            }

            // We need to grow the array.

            int delta = denseCapacityDeltaInt(size, values.length);
            int newCapacity = Math.min(ARRAY_STORAGE_32_MAX_DENSE_SIZE, size + delta);
            if (unsignedIndex < newCapacity) {
                // We are good: just grow the array and store the value.

                values = copyOf(values, newCapacity);
                values[index] = value;
                ++size;
                return this;
            }

            // We would be wasting too much space on dense representation:
            // convert to sparse representation.

            if (size >= ARRAY_STORAGE_32_MAX_SPARSE_SIZE) {
                // Too big: convert to prefix storage.
                return new PrefixStorage32(values, size, index, value);
            }

            Object[] oldValues = values;
            if (size == values.length) {
                // No space left: reallocate the values and allocate the
                // indexes array.

                indexes = new int[newCapacity];
                values = new Object[newCapacity];
            } else {
                // Some space left, its size is bellow the sparse
                // representation threshold (we adjusted for it in
                // denseCapacityDeltaInt call): reuse the values array and
                // allocate indexes array.

                indexes = new int[values.length];
            }

            // Iterate dense records one-by-one and represent them as sparse
            // ones.
            int count = 0;
            for (int i = 0; i < values.length; ++i) {
                Object storedValue = oldValues[i];
                if (storedValue != null) {
                    indexes[count] = i;
                    values[count] = storedValue;
                    ++count;
                    if (count == size) {
                        // Everything converted: stop.
                        break;
                    }
                }
            }

            // Store the original value being inserted and grow the size.
            indexes[size] = index;
            values[size] = value;
            ++size;
            return this;
        }

        private Storage32 setSparse(int index, Object value) {
            assert size > 0;
            long unsignedIndex = toUnsignedLong(index);

            int position = unsignedBinarySearch(indexes, size, unsignedIndex);
            if (position >= 0) {
                // Already there: just overwrite it.

                values[position] = value;
                return this;
            }
            position = -(position + 1);

            if (size == indexes.length) {
                // We are full: either grow the array or try to convert to
                // dense representation.

                int delta = capacityDeltaInt(indexes.length);
                long lastIndex = toUnsignedLong(indexes[indexes.length - 1]);
                long denseCapacity = Math.max(unsignedIndex, lastIndex) + 1;
                if (denseCapacity <= size + delta) {
                    // The wasted space is bellow the threshold: convert to
                    // dense representation.

                    Object[] newValues = new Object[(int) denseCapacity];
                    for (int i = 0; i < indexes.length; ++i) {
                        newValues[indexes[i]] = values[i];
                    }
                    newValues[index] = value;

                    indexes = null;
                    values = newValues;
                    ++size;
                    return this;
                }

                // We were unable to convert to dense representation.

                if (size >= ARRAY_STORAGE_32_MAX_SPARSE_SIZE) {
                    // Too big: convert to prefix storage.
                    return new PrefixStorage32(indexes, values, position, index, value);
                }

                // Reallocate the arrays to gain the space for the new value.

                int newCapacity = Math.min(ARRAY_STORAGE_32_MAX_SPARSE_SIZE, size + delta);

                int[] newIndexes = new int[newCapacity];
                arraycopy(indexes, 0, newIndexes, 0, position);
                arraycopy(indexes, position, newIndexes, position + 1, size - position);
                indexes = newIndexes;

                Object[] newValues = new Object[newCapacity];
                arraycopy(values, 0, newValues, 0, position);
                arraycopy(values, position, newValues, position + 1, size - position);
                values = newValues;
            } else {
                // There is some space left: just shift the values to the right.
                arraycopy(indexes, position, indexes, position + 1, size - position);
                arraycopy(values, position, values, position + 1, size - position);
            }

            // Finally insert the value.
            indexes[position] = index;
            values[position] = value;
            ++size;
            return this;
        }

        @Override
        public Storage32 clear(int index) {
            if (indexes == null) {
                return clearDense(index);
            } else {
                return clearSparse(index);
            }
        }

        @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
        private Storage32 clearDense(int index) {
            long unsignedIndex = toUnsignedLong(index);

            if (unsignedIndex >= values.length) {
                // Out of bounds.
                return this;
            }
            if (values[index] == null) {
                // No such record.
                return this;
            }

            values[index] = null;
            --size;
            if (size == 0) {
                // Emptied: just report the storage is empty by returning null.
                return null;
            }

            int delta = capacityDeltaInt(values.length);
            int wasted = values.length - size;
            if (wasted < delta) {
                return this;
            }
            int newCapacity = values.length - delta;
            if (newCapacity < MIN_CAPACITY) {
                // never reached with the default MIN_CAPACITY of 1
                return this;
            }
            assert wasted == delta;
            assert newCapacity == size;

            // Shrink the capacity while trying to keep the representation dense.

            Object[] newValues = new Object[newCapacity];
            int left = size;
            for (int i = values.length - 1; i >= 0; --i) {
                Object value = values[i];
                if (value != null) {
                    if (i >= newCapacity && indexes == null) {
                        // The index-value pair is outside of the dense
                        // representation bounds: convert to sparse by
                        // allocating indexes array.

                        if (size > ARRAY_STORAGE_32_MAX_SPARSE_SIZE) {
                            return new PrefixStorage32(values, size);
                        }
                        indexes = new int[newCapacity];
                    }

                    --left;
                    if (indexes != null) {
                        // store sparse representation index
                        indexes[left] = i;
                    }
                    newValues[left] = value;

                    if (left == 0) {
                        break;
                    }
                }
            }
            assert left == 0;
            values = newValues;
            return this;
        }

        private Storage32 clearSparse(int index) {
            assert size > 0;
            long unsignedIndex = toUnsignedLong(index);

            int position = unsignedBinarySearch(indexes, size, unsignedIndex);
            if (position < 0) {
                return this;
            }

            --size;
            if (size == 0) {
                // Emptied: just null out the value and convert to dense
                // representation by setting indexes to null.
                values[position] = null;
                indexes = null;
                return null;
            }

            int delta = capacityDeltaInt(indexes.length);
            int wasted = indexes.length - size;
            int newCapacity = indexes.length - delta;
            if (wasted >= delta && newCapacity >= MIN_CAPACITY) {
                // We are wasting too much: shrink the arrays.

                int[] newIndexes = new int[newCapacity];
                arraycopy(indexes, 0, newIndexes, 0, position);
                arraycopy(indexes, position + 1, newIndexes, position, size - position);
                indexes = newIndexes;

                Object[] newValues = new Object[newCapacity];
                arraycopy(values, 0, newValues, 0, position);
                arraycopy(values, position + 1, newValues, position, size - position);
                values = newValues;
            } else {
                // shift left to fill the gap
                arraycopy(indexes, position + 1, indexes, position, size - position);
                arraycopy(values, position + 1, values, position, size - position);
                values[size] = null;
            }
            return this;
        }

        @Override
        public Object get(int index) {
            long unsignedIndex = toUnsignedLong(index);

            if (indexes == null) {
                // Dense representation.

                return unsignedIndex < values.length ? values[index] : null;
            } else {
                // Sparse representation. Size is always greater than zero since
                // empty arrays are represented as dense ones.
                assert size > 0;

                int position = unsignedBinarySearch(indexes, size, unsignedIndex);
                return position >= 0 ? values[position] : null;
            }
        }

        @Override
        public long iterate(Iterator iterator) {
            if (size > 0) {
                iterator.position32 = 0;
                long index = advance(0, iterator);
                assert index != Iterator.END;
                return index;
            } else {
                return Iterator.END;
            }
        }

        @Override
        public long advance(int current, Iterator iterator) {
            int position = iterator.position32;

            if (indexes == null) {
                // Dense representation.

                while (position < values.length) {
                    Object value = values[position];
                    if (value != null) {
                        iterator.value = value;
                        iterator.position32 = position + 1;
                        return position;
                    }

                    ++position;
                }

                return Iterator.END;
            } else {
                // Sparse representation. Size is always greater than zero since
                // empty arrays are represented as dense ones.
                assert size > 0;

                if (position < size) {
                    iterator.value = values[position];
                    iterator.position32 = position + 1;
                    return toUnsignedLong(indexes[position]);
                } else {
                    return Iterator.END;
                }
            }
        }

        @Override
        public long iterateAtLeastFrom(int index, Iterator iterator) {
            long unsignedIndex = toUnsignedLong(index);

            if (indexes == null) {
                // Dense representation.

                long position = unsignedIndex;

                while (position < values.length) {
                    Object value = values[(int) position];
                    if (value != null) {
                        iterator.value = value;
                        iterator.position32 = (int) position + 1;
                        return position;
                    }

                    ++position;
                }

                return Iterator.END;
            } else {
                // Sparse representation. Size is always greater than zero since
                // empty arrays are represented as dense ones.
                assert size > 0;

                int position = unsignedBinarySearch(indexes, size, unsignedIndex);

                if (position < 0) {
                    position = -(position + 1);
                    if (position == size) {
                        return Iterator.END;
                    }
                    unsignedIndex = toUnsignedLong(indexes[position]);
                }

                iterator.position32 = position + 1;
                iterator.value = values[position];
                return unsignedIndex;
            }
        }

        @Override
        public long advanceAtLeastTo(int index, int current, Iterator iterator) {
            long unsignedIndex = toUnsignedLong(index);
            assert toUnsignedLong(current) < unsignedIndex;

            if (indexes == null) {
                // Dense representation.

                long position = unsignedIndex;

                while (position < values.length) {
                    Object value = values[(int) position];
                    if (value != null) {
                        iterator.value = value;
                        iterator.position32 = (int) position + 1;
                        return position;
                    }

                    ++position;
                }

                return Iterator.END;
            } else {
                // Sparse representation. Size is always greater than zero since
                // empty arrays are represented as dense ones.
                assert size > 0;

                int position = iterator.position32;
                if (position == size) {
                    return Iterator.END;
                }
                position = unsignedBinarySearch(indexes, position, size, unsignedIndex);

                if (position < 0) {
                    position = -(position + 1);
                    if (position == size) {
                        return Iterator.END;
                    }
                    unsignedIndex = toUnsignedLong(indexes[position]);
                }

                iterator.value = values[position];
                iterator.position32 = position + 1;
                return unsignedIndex;
            }
        }

    }

    /**
     * Manages sorted array of 16-bit prefixes to lookup storages for 16-bit
     * postfixes.
     */
    private static final class PrefixStorage32 implements Storage32 {

        private static final int MIN_CAPACITY = 2;
        private static final int MAX_CAPACITY = 64 * 1024;

        private int size;
        private short[] prefixes;
        private Storage16[] storages;

        // used for caching of the last resolved 16-bit storage
        private int lastPrefix = -1;
        private Storage16 lastStorage;

        /**
         * Constructs a new prefix storage by converting from the given dense
         * values and inserting the new given index-value pair.
         */
        PrefixStorage32(Object[] values, int size, int index, Object value) {
            assert size <= values.length;
            assert index >= values.length;

            this.prefixes = new short[MIN_CAPACITY];
            this.storages = new Storage16[MIN_CAPACITY];

            int count = 0;
            for (int i = 0; i < values.length; ++i) {
                Object storedValue = values[i];
                if (storedValue != null) {
                    append(i, storedValue);
                    ++count;
                    if (count == size) {
                        break;
                    }
                }
            }
            append(index, value);
        }

        /**
         * Constructs a new prefix storage by converting from the given dense
         * values.
         */
        PrefixStorage32(Object[] values, int size) {
            assert size <= values.length;

            this.prefixes = new short[MIN_CAPACITY];
            this.storages = new Storage16[MIN_CAPACITY];

            int count = 0;
            for (int i = 0; i < values.length; ++i) {
                Object storedValue = values[i];
                if (storedValue != null) {
                    append(i, storedValue);
                    ++count;
                    if (count == size) {
                        break;
                    }
                }
            }
        }

        /**
         * Constructs a new prefix storage by converting from the given sparse
         * index-value pairs and inserting the new given index-value pair at the
         * given position.
         */
        PrefixStorage32(int[] indexes, Object[] values, int position, int index, Object value) {
            this.prefixes = new short[MIN_CAPACITY];
            this.storages = new Storage16[MIN_CAPACITY];

            for (int i = 0; i < position; ++i) {
                append(indexes[i], values[i]);
            }
            append(index, value);
            for (int i = position; i < indexes.length; ++i) {
                append(indexes[i], values[i]);
            }
        }

        @Override
        public Storage32 set(int index, Object value) {
            short prefix = (short) (index >>> Short.SIZE);
            int unsignedPrefix = toUnsignedInt(prefix);

            // Try to resolve the corresponding 16-bit postfix storage by its
            // 16-bit prefix.

            if (unsignedPrefix == lastPrefix) {
                // We are lucky: just set the index-value pair in the cached
                // storage.

                lastStorage.set((short) index, value);
                return this;
            }

            int position = size == 0 ? -1 : unsignedBinarySearch(prefixes, size, unsignedPrefix);
            if (position >= 0) {
                // The storage already exists: just add the index-value pair to
                // it.

                Storage16 storage = storages[position];
                storage.set((short) index, value);
                lastPrefix = unsignedPrefix;
                lastStorage = storage;
                return this;
            }
            position = -(position + 1);

            // No storage exists yet: we need to allocate a new postfix storage
            // and insert it into this prefix storage.

            if (size == prefixes.length) {
                // Grow the arrays.

                int newCapacity = Math.min(MAX_CAPACITY, size + capacityDeltaShort(prefixes.length));

                short[] newPrefixes = new short[newCapacity];
                arraycopy(prefixes, 0, newPrefixes, 0, position);
                arraycopy(prefixes, position, newPrefixes, position + 1, size - position);
                prefixes = newPrefixes;

                Storage16[] newStorages = new Storage16[newCapacity];
                arraycopy(storages, 0, newStorages, 0, position);
                arraycopy(storages, position, newStorages, position + 1, size - position);
                storages = newStorages;
            } else {
                // Shift the arrays right to free a slot.

                arraycopy(prefixes, position, prefixes, position + 1, size - position);
                arraycopy(storages, position, storages, position + 1, size - position);
            }

            Storage16 createdStorage = new Storage16((short) index, value);
            prefixes[position] = prefix;
            storages[position] = createdStorage;
            lastPrefix = unsignedPrefix;
            lastStorage = createdStorage;
            ++size;
            return this;
        }

        @Override
        public Storage32 clear(int index) {
            short prefix = (short) (index >>> Short.SIZE);
            int unsignedPrefix = toUnsignedInt(prefix);

            // Try to resolve the corresponding 16-bit postfix storage by its
            // 16-bit prefix.

            int position;

            if (unsignedPrefix == lastPrefix) {
                // We are lucky: just remove the index-value pair from the
                // cached storage.

                Storage16 storage = lastStorage;
                if (!storage.clear((short) index)) {
                    return this;
                }
                // To handle the storage removal we need to know its prefix index.
                position = unsignedBinarySearch(prefixes, size, unsignedPrefix);
                assert position >= 0;
            } else {
                if (size == 0) {
                    // no storages, no problems
                    return this;
                }
                position = unsignedBinarySearch(prefixes, size, unsignedPrefix);
                if (position < 0) {
                    // no storage, no problems
                    return this;
                }

                Storage16 storage = storages[position];
                if (!storage.clear((short) index)) {
                    lastStorage = storage;
                    lastPrefix = unsignedPrefix;
                    return this;
                }
            }

            // The 16-bit postfix storage is emptied this point: remove it from
            // this prefix storage.

            --size;
            lastStorage = null;
            lastPrefix = -1;
            if (size == 0) {
                // this prefix storage is also emptied
                storages[position] = null;
                return null;
            }

            int delta = capacityDeltaShort(prefixes.length);
            int wasted = prefixes.length - size;
            int newCapacity = prefixes.length - delta;
            if (wasted >= delta && newCapacity >= MIN_CAPACITY) {
                // Wasting too much: shrink the arrays.

                short[] newPrefixes = new short[newCapacity];
                arraycopy(prefixes, 0, newPrefixes, 0, position);
                arraycopy(prefixes, position + 1, newPrefixes, position, size - position);
                prefixes = newPrefixes;

                Storage16[] newStorages = new Storage16[newCapacity];
                arraycopy(storages, 0, newStorages, 0, position);
                arraycopy(storages, position + 1, newStorages, position, size - position);
                storages = newStorages;
            } else {
                // Shift the arrays left to fill the gap.

                arraycopy(prefixes, position + 1, prefixes, position, size - position);
                arraycopy(storages, position + 1, storages, position, size - position);
                storages[size] = null;
            }
            return this;
        }

        @Override
        public Object get(int index) {
            short prefix = (short) (index >>> Short.SIZE);
            int unsignedPrefix = toUnsignedInt(prefix);

            // Try to resolve the corresponding 16-bit postfix storage by its
            // 16-bit prefix.

            if (unsignedPrefix == lastPrefix) {
                // We are lucky: just consult the cached postfix storage.
                return lastStorage.get((short) index);
            }

            if (size == 0) {
                // no storages, no problems
                return null;
            }
            int position = unsignedBinarySearch(prefixes, size, unsignedPrefix);
            if (position < 0) {
                // no storage, no problems
                return null;
            }

            Storage16 storage = storages[position];
            lastPrefix = unsignedPrefix;
            lastStorage = storage;
            return storage.get((short) index);
        }

        @Override
        public long iterate(Iterator iterator) {
            if (size > 0) {
                iterator.position32 = 1;
                iterator.storage16 = storages[0];
                return toUnsignedLong(prefixes[0]) << Short.SIZE | iterator.storage16.iterate(iterator);
            } else {
                return Iterator.END;
            }
        }

        @Override
        public long advance(int current, Iterator iterator) {
            // Try advance the current postfix storage.

            int postfix = iterator.storage16.advance(iterator);
            if (postfix != Storage16.END) {
                return toUnsignedLong(current) & SHORT_PREFIX_MASK_LONG | postfix;
            }

            // Try to advance to the next postfix storage and iterate it.

            int index = iterator.position32;
            if (index < size) {
                iterator.storage16 = storages[index];
                iterator.position32 = index + 1;
                postfix = iterator.storage16.iterate(iterator);
                return toUnsignedLong(prefixes[index]) << Short.SIZE | postfix;
            } else {
                return Iterator.END;
            }
        }

        @Override
        public long iterateAtLeastFrom(int index, Iterator iterator) {
            if (size == 0) {
                return Iterator.END;
            }
            return iterateAtLeastFrom(index, 0, iterator);
        }

        @Override
        public long advanceAtLeastTo(int index, int current, Iterator iterator) {
            short prefix = (short) (index >>> Short.SIZE);
            int unsignedPrefix = toUnsignedInt(prefix);
            int currentUnsignedPrefix = (current & SHORT_PREFIX_MASK_INT) >>> Short.SIZE;
            assert currentUnsignedPrefix <= unsignedPrefix;

            if (unsignedPrefix == currentUnsignedPrefix) {
                // The requested index is within the current 16-bit postfix
                // storage.

                int postfix = iterator.storage16.advanceAtLeastTo((short) index, (short) current, iterator);
                if (postfix != Storage16.END) {
                    // found in the current postfix storage
                    return toUnsignedLong(prefix) << Short.SIZE | postfix;
                }

                int position = iterator.position32;
                if (position == size) {
                    // the end
                    return Iterator.END;
                }

                // Iterate the next prefix.

                Storage16 storage = storages[position];
                iterator.storage16 = storage;
                iterator.position32 = position + 1;
                postfix = storage.iterate(iterator);
                assert postfix != Storage16.END;
                return toUnsignedLong(prefixes[position]) << Short.SIZE | postfix;
            }

            // Resolve and iterate the requested prefix.

            int position = iterator.position32;
            if (position == size) {
                return Iterator.END;
            }
            return iterateAtLeastFrom(index, position, iterator);
        }

        private void append(int index, Object value) {
            short prefix = (short) (index >>> Short.SIZE);

            if (size != 0 && prefix == prefixes[size - 1]) {
                storages[size - 1].set((short) index, value);
                return;
            }

            if (size == prefixes.length) {
                int newCapacity = Math.min(MAX_CAPACITY, size + capacityDeltaShort(prefixes.length));
                prefixes = copyOf(prefixes, newCapacity);
                storages = copyOf(storages, newCapacity);
            }

            prefixes[size] = prefix;
            storages[size] = new Storage16((short) index, value);
            ++size;
        }

        private long iterateAtLeastFrom(int index, int startFrom, Iterator iterator) {
            short prefix = (short) (index >>> Short.SIZE);
            int position = unsignedBinarySearch(prefixes, startFrom, size, toUnsignedInt(prefix));

            Storage16 storage;
            int postfix;
            if (position < 0) {
                position = -(position + 1);
                if (position == size) {
                    // no such index
                    return Iterator.END;
                }

                storage = storages[position];
                prefix = prefixes[position];
                postfix = storage.iterate(iterator);
                assert postfix != Storage16.END;
            } else {
                storage = storages[position];
                postfix = storage.iterateAtLeastFrom((short) index, iterator);
            }

            if (postfix != Storage16.END) {
                // The postfix storage corresponding to the requested index is
                // successfully advanced at least to the requested index.

                iterator.storage16 = storage;
                iterator.position32 = position + 1;
                return toUnsignedLong(prefix) << Short.SIZE | postfix;
            } else {
                // The postfix storage corresponding to the requested index
                // doesn't contain the requested index or any indexes greater
                // than it: try to iterate from the next prefix storage.

                ++position;
                if (position == size) {
                    return Iterator.END;
                }

                storage = storages[position];
                iterator.storage16 = storage;
                iterator.position32 = position + 1;
                postfix = storage.iterate(iterator);
                assert postfix != Storage16.END;
                return toUnsignedLong(prefixes[position]) << Short.SIZE | postfix;
            }
        }

    }

    /**
     * Manages ordered arrays of index-value pairs for 16-bit indexes (postfixes).
     * <p>
     * Works in two modes:
     * <ul>
     * <li>Dense mode with directly indexable values array.
     * <li>Sparse mode with sorted indexes and values arrays.
     * </ul>
     */
    private static final class Storage16 {

        /**
         * Identifies an iterator end.
         */
        public static final int END = -1;

        private static final int MIN_CAPACITY = 2;

        private int size;
        private short[] indexes;
        private Object[] values = new Object[MIN_CAPACITY];

        Storage16(short index, Object value) {
            set(index, value);
        }

        /**
         * Sets or replaces a value at the given index to the new given value.
         *
         * @param index the index to set value at.
         * @param value the value to set.
         */
        public void set(short index, Object value) {
            if (indexes == null) {
                setDense(index, value);
            } else {
                setSparse(index, value);
            }
        }

        private void setDense(short index, Object value) {
            int unsignedIndex = toUnsignedInt(index);

            if (unsignedIndex < values.length) {
                // The index is inside the bounds: just set the corresponding
                // array slot.

                if (values[unsignedIndex] == null) {
                    ++size;
                }
                values[unsignedIndex] = value;
                return;
            }

            // We need to grow the array.

            int delta = denseCapacityDeltaShort(size, values.length);
            int newCapacity = Math.min(STORAGE_16_MAX_DENSE_SIZE, size + delta);
            if (unsignedIndex < newCapacity) {
                // We are good: just grow the array and store the value.

                values = copyOf(values, newCapacity);
                values[unsignedIndex] = value;
                ++size;
                return;
            }

            // We would be wasting too much space on dense representation:
            // convert to sparse representation.

            Object[] oldValues = values;
            if (size == values.length) {
                // No space left: reallocate the values and allocate the
                // indexes array.

                indexes = new short[newCapacity];
                values = new Object[newCapacity];
            } else {
                // Some space left, its size is bellow the sparse
                // representation threshold (we adjusted for it in
                // denseCapacityDeltaShort call): reuse the values array and
                // allocate indexes array.

                indexes = new short[values.length];
            }

            // Iterate dense records one-by-one and represent them as sparse
            // ones.
            int count = 0;
            for (int i = 0; i < values.length; ++i) {
                Object storedValue = oldValues[i];
                if (storedValue != null) {
                    indexes[count] = (short) i;
                    values[count] = storedValue;
                    ++count;
                    if (count == size) {
                        break;
                    }
                }
            }

            // Store the original value being inserted and grow the size.
            indexes[size] = index;
            values[size] = value;
            ++size;
        }

        private void setSparse(short index, Object value) {
            int unsignedIndex = toUnsignedInt(index);

            int position = unsignedBinarySearch(indexes, size, unsignedIndex);
            if (position >= 0) {
                // Already there: just overwrite it.

                values[position] = value;
                return;
            }
            position = -(position + 1);

            if (size == indexes.length) {
                // We are full: either grow the array or try to convert to
                // dense representation.

                int delta = capacityDeltaShort(indexes.length);
                int lastIndex = toUnsignedInt(indexes[indexes.length - 1]);
                int denseCapacity = Math.max(unsignedIndex, lastIndex) + 1;
                if (denseCapacity <= size + delta) {
                    // The wasted space is bellow the threshold: convert to
                    // dense representation.

                    Object[] newValues = new Object[denseCapacity];
                    for (int i = 0; i < indexes.length; ++i) {
                        newValues[toUnsignedInt(indexes[i])] = values[i];
                    }
                    newValues[unsignedIndex] = value;

                    indexes = null;
                    values = newValues;
                    ++size;
                    return;
                }

                // We were unable to convert to dense representation: reallocate
                // the arrays to gain the space for the new value.

                int newCapacity = Math.min(STORAGE_16_MAX_SPARSE_SIZE, size + capacityDeltaShort(indexes.length));

                short[] newIndexes = new short[newCapacity];
                arraycopy(indexes, 0, newIndexes, 0, position);
                arraycopy(indexes, position, newIndexes, position + 1, size - position);
                indexes = newIndexes;

                Object[] newValues = new Object[newCapacity];
                arraycopy(values, 0, newValues, 0, position);
                arraycopy(values, position, newValues, position + 1, size - position);
                values = newValues;
            } else {
                // There is some space left: just shift the values to the right.
                arraycopy(indexes, position, indexes, position + 1, size - position);
                arraycopy(values, position, values, position + 1, size - position);
            }

            // Finally insert the value.
            indexes[position] = index;
            values[position] = value;
            ++size;
        }

        /**
         * Clears a value stored at the given index in this storage.
         *
         * @param index the index to clear a value at.
         * @return {@code true} if this storage array is emptied, {@code false}
         * otherwise.
         */
        public boolean clear(short index) {
            if (indexes == null) {
                return clearDense(index);
            } else {
                return clearSparse(index);
            }
        }

        @SuppressWarnings("checkstyle:npathcomplexity")
        private boolean clearDense(short index) {
            int unsignedIndex = toUnsignedInt(index);

            if (unsignedIndex >= values.length) {
                // Out of bounds.
                return false;
            }
            if (values[unsignedIndex] == null) {
                // No such record.
                return false;
            }

            --size;
            if (size == 0) {
                // Emptied: just report the storage is empty by returning null.
                return true;
            }
            values[unsignedIndex] = null;

            int delta = capacityDeltaShort(values.length);
            int wasted = values.length - size;
            if (wasted < delta) {
                return false;
            }
            int newCapacity = values.length - delta;
            if (newCapacity < MIN_CAPACITY) {
                // never reached with the default MIN_CAPACITY of 2
                return false;
            }
            assert wasted == delta;
            assert newCapacity == size;

            // Shrink the capacity while trying to keep the representation dense.

            Object[] newValues = new Object[newCapacity];
            int left = size;
            for (int i = values.length - 1; i >= 0; --i) {
                Object value = values[i];
                if (value != null) {
                    if (i >= newCapacity && indexes == null) {
                        // The index-value pair is outside of the dense
                        // representation bounds: convert to sparse by
                        // allocating indexes array.

                        indexes = new short[newCapacity];
                    }

                    --left;
                    if (indexes != null) {
                        // store sparse representation index
                        indexes[left] = (short) i;
                    }
                    newValues[left] = value;

                    if (left == 0) {
                        break;
                    }
                }
            }
            values = newValues;
            return false;
        }

        private boolean clearSparse(short index) {
            int unsignedIndex = toUnsignedInt(index);

            int position = unsignedBinarySearch(indexes, size, unsignedIndex);
            if (position < 0) {
                return false;
            }

            --size;
            if (size == 0) {
                // Emptied.
                return true;
            }

            int delta = capacityDeltaShort(indexes.length);
            int wasted = indexes.length - size;
            int newCapacity = indexes.length - delta;
            if (wasted >= delta && newCapacity >= MIN_CAPACITY) {
                // We are wasting too much: shrink the arrays.

                short[] newIndexes = new short[newCapacity];
                arraycopy(indexes, 0, newIndexes, 0, position);
                arraycopy(indexes, position + 1, newIndexes, position, size - position);
                indexes = newIndexes;

                Object[] newValues = new Object[newCapacity];
                arraycopy(values, 0, newValues, 0, position);
                arraycopy(values, position + 1, newValues, position, size - position);
                values = newValues;
            } else {
                // shift left to fill the gap

                arraycopy(indexes, position + 1, indexes, position, size - position);
                arraycopy(values, position + 1, values, position, size - position);
                values[size] = null;
            }

            return false;
        }

        /**
         * @return the value stored inside this storage at the given index or
         * {@code null} if nothing stored at it.
         */
        public Object get(short index) {
            int unsignedIndex = toUnsignedInt(index);

            if (indexes == null) {
                return unsignedIndex < values.length ? values[unsignedIndex] : null;
            } else {
                int position = unsignedBinarySearch(indexes, size, unsignedIndex);
                return position >= 0 ? values[position] : null;
            }
        }

        /**
         * Starts iteration on this storage using the given iterator.
         *
         * @param iterator the iterator to iterate with.
         * @return an index at which the iterator is currently at or {@link
         * #END} if this storage is empty.
         */
        public int iterate(Iterator iterator) {
            assert size > 0;
            iterator.position16 = 0;
            int index = advance(iterator);
            assert index != Storage16.END;
            return index;
        }

        /**
         * Advances the given iterator on this storage.
         *
         * @param iterator the iterator to advance.
         * @return an index at which this iterator was positioned before the
         * advancement or {@link #END} if this iterator already was at its end.
         */
        public int advance(Iterator iterator) {
            assert size > 0;
            int position = iterator.position16;

            if (indexes == null) {
                // Dense representation.

                while (position < values.length) {
                    Object value = values[position];
                    if (value != null) {
                        iterator.value = value;
                        iterator.position16 = position + 1;
                        return position;
                    }

                    ++position;
                }

                return Storage16.END;
            } else {
                // Sparse representation.

                if (position < size) {
                    iterator.value = values[position];
                    iterator.position16 = position + 1;
                    return toUnsignedInt(indexes[position]);
                } else {
                    return Storage16.END;
                }
            }
        }

        /**
         * Starts iteration on this storage starting at least from the given
         * index using the given iterator.
         *
         * @param index    the index to start the iteration from.
         * @param iterator the iterator to iterate with.
         * @return an index at which the iterator is currently at; or {@link
         * #END} if no index greater than or equal to the given index exists in
         * this storage.
         */
        public int iterateAtLeastFrom(short index, Iterator iterator) {
            assert size > 0;
            int unsignedIndex = toUnsignedInt(index);

            if (indexes == null) {
                // Dense representation.

                int position = unsignedIndex;
                while (position < values.length) {
                    Object value = values[position];
                    if (value != null) {
                        iterator.value = value;
                        iterator.position16 = position + 1;
                        return position;
                    }

                    ++position;
                }

                return Storage16.END;
            } else {
                // Sparse representation.

                int position = unsignedBinarySearch(indexes, size, unsignedIndex);

                if (position < 0) {
                    position = -(position + 1);
                    if (position == size) {
                        return Storage16.END;
                    }
                    unsignedIndex = toUnsignedInt(indexes[position]);
                }

                iterator.value = values[position];
                iterator.position16 = position + 1;
                return unsignedIndex;
            }
        }

        /**
         * Advances the given iterator to the given index; or, if the index is
         * not present in this storage, to an index immediately following it
         * and present in this storage.
         *
         * @param index    the index to advance at least to. The index must be
         *                 greater than the index this iterator is currently at.
         * @param iterator the iterator to advance.
         * @return an index at which thee iterator was advanced to or {@link
         * #END} if the iterator reached its end.
         */
        public int advanceAtLeastTo(short index, short current, Iterator iterator) {
            assert size > 0;
            int unsignedIndex = toUnsignedInt(index);
            assert toUnsignedInt(current) < unsignedIndex;

            if (indexes == null) {
                // Dense representation.

                int position = unsignedIndex;

                while (position < values.length) {
                    Object value = values[position];
                    if (value != null) {
                        iterator.value = value;
                        iterator.position16 = position + 1;
                        return position;
                    }

                    ++position;
                }

                return Storage16.END;
            } else {
                // Sparse representation.

                int position = iterator.position16;
                if (position == size) {
                    return Storage16.END;
                }
                position = unsignedBinarySearch(indexes, position, size, unsignedIndex);

                if (position < 0) {
                    position = -(position + 1);
                    if (position == size) {
                        return Storage16.END;
                    }
                    unsignedIndex = toUnsignedInt(indexes[position]);
                }

                iterator.value = values[position];
                iterator.position16 = position + 1;
                return unsignedIndex;
            }
        }

    }

}
