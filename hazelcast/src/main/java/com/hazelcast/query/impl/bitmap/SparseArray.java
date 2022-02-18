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

/**
 * Stores values of type {@code E} indexable by non-negative {@code long}
 * indexes.
 * <p>
 * Internally, uses a {@link SparseIntArray} indexed by the high 32 bits (prefix)
 * of an index to resolve another {@link SparseIntArray} indexed by the low 32
 * bits (postfix).
 *
 * @param <E> the element type.
 */
final class SparseArray<E> {

    private static final long INT_PREFIX_MASK = 0xFFFFFFFF00000000L;

    /**
     * Iterates over values of a sparse array in ascending index order.
     */
    public interface Iterator<T> extends AscendingLongIterator {

        /**
         * Returns a value this iterator is currently at. If this iterator is
         * already reached its end, the return value is undefined.
         * <p>
         * Just after the creation, iterators are positioned at their first value.
         */
        T getValue();

    }

    private final SparseIntArray<SparseIntArray<E>> storages = new SparseIntArray<>();

    // used for caching of the last resolved storage
    private int lastPrefix = -1;
    private SparseIntArray<E> lastStorage;

    /**
     * Sets or replaces a value at the given index in this sparse array to the
     * new given value.
     *
     * @param index the index to set value at.
     * @param value the value to set.
     */
    public void set(long index, E value) {
        assert index >= 0;
        assert value != null;
        int prefix = (int) (index >>> Integer.SIZE);

        if (prefix == lastPrefix) {
            lastStorage.set((int) index, value);
        } else {
            lastPrefix = prefix;
            SparseIntArray<E> storage = storages.get(prefix);
            if (storage == null) {
                SparseIntArray<E> createdStorage = new SparseIntArray<>();
                createdStorage.set((int) index, value);
                lastStorage = createdStorage;
                storages.set(prefix, createdStorage);
            } else {
                storage.set((int) index, value);
                lastStorage = storage;
            }
        }
    }

    /**
     * Clears the value at the given index in this sparse array.
     *
     * @param index the index to clear value at.
     */
    public void clear(long index) {
        assert index >= 0;
        int prefix = (int) (index >>> Integer.SIZE);

        if (prefix == lastPrefix) {
            if (lastStorage.clear((int) index)) {
                lastPrefix = -1;
                lastStorage = null;
                // cleanup the empty storage
                storages.clear(prefix);
            }
        } else {
            SparseIntArray<E> storage = storages.get(prefix);
            if (storage != null) {
                if (storage.clear((int) index)) {
                    // cleanup the empty storage
                    storages.clear(prefix);
                } else {
                    lastPrefix = prefix;
                    lastStorage = storage;
                }
            }
        }
    }

    /**
     * Clears this sparse array entirely.
     */
    public void clear() {
        lastPrefix = -1;
        lastStorage = null;
        storages.clear();
    }

    /**
     * @return an iterator that iterates over all the values stored in this
     * sparse array.
     */
    public Iterator<E> iterator() {
        return new IteratorImpl<>(storages);
    }

    private static final class IteratorImpl<T> extends SparseIntArray.Iterator<T> implements Iterator<T> {

        private final SparseIntArray<SparseIntArray<T>> storages;
        private final SparseIntArray.Iterator<SparseIntArray<T>> storageIterator;

        private SparseIntArray<T> storage;
        private long index;

        IteratorImpl(SparseIntArray<SparseIntArray<T>> storages) {
            this.storages = storages;
            this.storageIterator = new SparseIntArray.Iterator<>();

            long prefix = storages.iterate(storageIterator);
            if (prefix != SparseIntArray.Iterator.END) {
                storage = storageIterator.getValue();
                long postfix = storage.iterate(this);
                assert postfix != SparseIntArray.Iterator.END;
                index = prefix << Integer.SIZE | postfix;
            } else {
                index = Iterator.END;
            }
        }

        @Override
        public long getIndex() {
            return index;
        }

        @Override
        public long advance() {
            long current = index;
            if (current == Iterator.END) {
                return Iterator.END;
            }

            // Try to advance the current storage.

            long postfix = storage.advance((int) current, this);
            if (postfix != SparseIntArray.Iterator.END) {
                index = current & INT_PREFIX_MASK | postfix;
                return current;
            }

            // Try to advance to the next storage.

            long prefix = storages.advance((int) (current >>> Integer.SIZE), storageIterator);
            if (prefix != SparseIntArray.Iterator.END) {
                storage = storageIterator.getValue();
                postfix = storage.iterate(this);
                if (postfix != SparseIntArray.Iterator.END) {
                    index = prefix << Integer.SIZE | postfix;
                    return current;
                }
            }

            // The end.

            index = Iterator.END;
            return current;
        }

        @SuppressWarnings("checkstyle:nestedifdepth")
        @Override
        public long advanceAtLeastTo(long member) {
            assert member >= 0;
            long current = index;
            if (current == Iterator.END) {
                return Iterator.END;
            }

            if (current >= member) {
                // Already at or beyond the requested member.

                return current;
            }

            int memberPrefix = (int) (member >>> Integer.SIZE);
            int currentPrefix = (int) (current >>> Integer.SIZE);

            if (memberPrefix == currentPrefix) {
                // Try to advance the current storage.

                long postfix = storage.advanceAtLeastTo((int) member, (int) current, this);
                if (postfix != SparseIntArray.Iterator.END) {
                    index = current & INT_PREFIX_MASK | postfix;
                    return index;
                } else {
                    // Try to advance to the next storage.

                    long prefix = storages.advance(currentPrefix, storageIterator);
                    if (prefix != SparseIntArray.Iterator.END) {
                        storage = storageIterator.getValue();
                        postfix = storage.iterate(this);
                        assert postfix != SparseIntArray.Iterator.END;
                        index = prefix << Integer.SIZE | postfix;
                        return index;
                    }
                }
            } else {
                // Try to advance to the requested storage.

                long prefix = storages.advanceAtLeastTo(memberPrefix, currentPrefix, storageIterator);
                if (prefix != SparseIntArray.Iterator.END) {
                    storage = storageIterator.getValue();
                    if (prefix == memberPrefix) {
                        // We got the storage we have requested.

                        long postfix = storage.iterateAtLeastFrom((int) member, this);
                        if (postfix != SparseIntArray.Iterator.END) {
                            index = prefix << Integer.SIZE | postfix;
                            return index;
                        } else {
                            // The storage we got doesn't contain the requested
                            // postfix or any postfix beyond it: try to advance
                            // to the next storage (storages are guaranteed to
                            // be non-empty).

                            prefix = storages.advance((int) prefix, storageIterator);
                            if (prefix != SparseIntArray.Iterator.END) {
                                storage = storageIterator.getValue();
                                postfix = storage.iterate(this);
                                assert postfix != SparseIntArray.Iterator.END;
                                index = prefix << Integer.SIZE | postfix;
                                return index;
                            }
                        }
                    } else {
                        // We got a storage corresponding to some other prefix
                        // beyond the requested one: just iterate the storage
                        // (storages are guaranteed to be non-empty).

                        long postfix = storage.iterate(this);
                        assert postfix != SparseIntArray.Iterator.END;
                        index = prefix << Integer.SIZE | postfix;
                        return index;
                    }
                }
            }

            // The end.

            index = AscendingLongIterator.END;
            return index;
        }

    }

}
