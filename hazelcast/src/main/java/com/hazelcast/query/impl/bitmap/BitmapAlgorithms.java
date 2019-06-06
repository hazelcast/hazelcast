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

package com.hazelcast.query.impl.bitmap;

/**
 * Provides algorithms crucial for set operations on ordered iterators provided
 * by sparse bit sets.
 */
final class BitmapAlgorithms {

    private BitmapAlgorithms() {
    }

    /**
     * @return an iterator that represents a result of intersection of the given
     * iterators.
     */
    public static AscendingLongIterator and(AscendingLongIterator[] iterators) {
        return new AndIterator(iterators);
    }

    /**
     * @return an iterator that represents a result of union over the given
     * iterators.
     */
    public static AscendingLongIterator or(AscendingLongIterator[] iterators) {
        return new OrIterator(iterators);
    }

    /**
     * @return an iterator that represents a result of negation of the given
     * iterator over the given universe (a set of known elements).
     */
    public static AscendingLongIterator not(AscendingLongIterator iterator, SparseArray<?> universe) {
        return new NotIterator(iterator, universe);
    }

    private static final class AndIterator implements AscendingLongIterator {

        // The idea: iteratively advance all iterators to the current
        // maximum index among all iterators until all iterators are at the
        // same index or the end is reached in at least one of the iterators.

        private final AscendingLongIterator[] iterators;

        private long index;

        AndIterator(AscendingLongIterator[] iterators) {
            this.iterators = iterators;
            advance();
        }

        @Override
        public long getIndex() {
            return index;
        }

        @Override
        public long advance() {
            long current = index;

            long currentMax = iterators[0].getIndex();
            long max = AscendingLongIterator.END;
            while (currentMax != max) {
                max = currentMax;
                for (AscendingLongIterator iterator : iterators) {
                    currentMax = iterator.advanceAtLeastTo(currentMax);
                    if (currentMax == AscendingLongIterator.END) {
                        index = AscendingLongIterator.END;
                        return current;
                    }
                }
            }

            // make a progress
            iterators[0].advance();

            index = max;
            return current;
        }

        @Override
        public long advanceAtLeastTo(long member) {
            if (index >= member) {
                return index;
            }

            for (AscendingLongIterator iterator : iterators) {
                iterator.advanceAtLeastTo(member);
            }
            advance();
            return index;
        }

    }

    private static final class OrIterator implements AscendingLongIterator {

        // The idea: build a min-heap that will order the iterators according
        // to their current indexes. Luckily enough, we need only sift-down
        // operation to accomplish that since heap creation and heap element
        // removal/update can be expressed solely with sift-downs.

        private final AscendingLongIterator[] iterators;

        private int size;

        OrIterator(AscendingLongIterator[] iterators) {
            this.iterators = iterators;
            this.size = iterators.length;

            heapify();
            removeEmptyIterators();
        }

        @Override
        public long getIndex() {
            return size == 0 ? AscendingLongIterator.END : iterators[0].getIndex();
        }

        @Override
        public long advance() {
            if (size == 0) {
                return AscendingLongIterator.END;
            }

            long current = iterators[0].getIndex();
            update();

            // skip duplicates if any
            while (size > 0 && iterators[0].getIndex() == current) {
                update();
            }

            return current;
        }

        @Override
        public long advanceAtLeastTo(long member) {
            if (size == 0) {
                return AscendingLongIterator.END;
            }

            long index;
            while ((index = iterators[0].getIndex()) < member) {
                update();
                if (size == 0) {
                    return AscendingLongIterator.END;
                }
            }
            return index;
        }

        private void removeEmptyIterators() {
            while (size > 0 && iterators[0].getIndex() == AscendingLongIterator.END) {
                --size;
                if (size != 0) {
                    siftDown(0, iterators[size]);
                }
            }
        }

        private void update() {
            assert size > 0;
            AscendingLongIterator iterator = iterators[0];
            long index = iterator.advance();
            long newIndex = iterator.getIndex();

            if (newIndex == AscendingLongIterator.END) {
                --size;
                if (size != 0) {
                    siftDown(0, iterators[size]);
                }
            } else {
                // Indexes are always increasing in each individual iterator,
                // so we may just sift-down according to the index.

                assert newIndex > index;
                siftDown(0, iterator);
            }
        }

        private void heapify() {
            // A textbook implementation of heapify described in Wikipedia,
            // java.util.PriorityQueue uses the same.

            for (int i = (size >>> 1) - 1; i >= 0; i--) {
                siftDown(i, iterators[i]);
            }
        }

        private void siftDown(int index, AscendingLongIterator iterator) {
            // A tail-call-eliminated implementation of sift-down described in
            // Wikipedia, similar to the one used by java.util.PriorityQueue.

            int firstLeafIndex = size >>> 1;
            while (index < firstLeafIndex) {
                int childIndex = (index << 1) + 1;
                AscendingLongIterator child = iterators[childIndex];
                int rightChildIndex = childIndex + 1;
                if (rightChildIndex < size && child.getIndex() > iterators[rightChildIndex].getIndex()) {
                    childIndex = rightChildIndex;
                    child = iterators[childIndex];
                }
                if (iterator.getIndex() <= child.getIndex()) {
                    break;
                }
                iterators[index] = child;
                index = childIndex;
            }
            iterators[index] = iterator;
        }

    }

    private static final class NotIterator implements AscendingLongIterator {

        // The idea: find gaps in the base iterator and iterate indexes/members
        // inside them using the universe iterator which contains all the known
        // indexes.

        private final AscendingLongIterator iterator;
        private final AscendingLongIterator universe;

        private long index;
        private long gapEnd;

        NotIterator(AscendingLongIterator iterator, SparseArray<?> universe) {
            this.iterator = iterator;
            this.universe = universe.iterator();
            gapEnd = iterator.advance();
            advance();
        }

        @Override
        public long getIndex() {
            return index;
        }

        @Override
        public long advance() {
            long current = index;

            long newIndex = universe.advance();
            if (newIndex != gapEnd) {
                // We still not reached the gap end, fast exit.

                index = newIndex;
                return current;
            }

            // Try to find a new gap.

            long newGapEnd = gapEnd;
            while (newIndex == newGapEnd && newIndex != AscendingLongIterator.END) {
                long newGapStart;
                do {
                    // We may overflow here to Long.MIN_VALUE, but we will exit
                    // the loop anyway. In this case, after exiting the loop,
                    // newGapEnd will be equal to BitSetIterator.END.
                    newGapStart = newGapEnd + 1;
                    newGapEnd = iterator.advance();
                } while (newGapEnd == newGapStart);

                if (newGapStart == Long.MIN_VALUE) {
                    // If the overflow happened, nothing left for sure.
                    assert newGapEnd == AscendingLongIterator.END;

                    // Position the universe to Long.MAX_VALUE and consume it.
                    universe.advanceAtLeastTo(Long.MAX_VALUE);
                    universe.advance();
                    break;
                } else {
                    newIndex = universe.advanceAtLeastTo(newGapStart);
                }
            }

            index = universe.advance();
            gapEnd = newGapEnd;
            return current;
        }

        @Override
        public long advanceAtLeastTo(long member) {
            if (index >= member) {
                // We are already at/beyond the member.
                return index;
            }

            if (member > gapEnd) {
                // Find out the new gap end and consume it.
                gapEnd = iterator.advanceAtLeastTo(member);
                iterator.advance();
            }
            // If existing/updated gap end is equal to the member, advance()
            // will handle that. Gap end is either equal to the member or
            // greater than it, since the gap end is part of the universe for
            // sure.

            universe.advanceAtLeastTo(member);
            advance();
            return index;
        }

    }

}
