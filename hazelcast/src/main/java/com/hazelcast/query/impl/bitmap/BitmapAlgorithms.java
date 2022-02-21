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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

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

        // The idea: order iterators by their current index; if the index of the
        // first iterator (minimum) equal to the last one (maximum), we have a
        // match; otherwise advance the first iterator at least to the last
        // iterator index (maximum) and make it the new maximum, make the
        // iterator previously ordered second to be the new minimum, repeat
        // checking for a match.

        private final Node[] nodes;
        private Node first;
        private Node last;

        private long index;

        AndIterator(AscendingLongIterator[] iterators) {
            Node[] nodes = new Node[iterators.length];
            for (int i = 0; i < nodes.length; ++i) {
                nodes[i] = new Node(iterators[i]);
            }
            this.nodes = nodes;

            orderAndLink();
            advance();
        }

        @Override
        public long getIndex() {
            return index;
        }

        @Override
        public long advance() {
            long current = index;

            long min = first.iterator.getIndex();
            long max = last.iterator.getIndex();

            while (min != max && min != AscendingLongIterator.END && max != AscendingLongIterator.END) {
                max = first.iterator.advanceAtLeastTo(max);

                // The first iterator is now guaranteed to be at the maximum or
                // beyond it, make it last and repeat.

                Node second = first.next;
                first.next = null;
                last.next = first;
                last = first;
                first = second;

                // The new first iterator is now our minimum.
                min = first.iterator.getIndex();
            }

            if (max == AscendingLongIterator.END) {
                index = AscendingLongIterator.END;
                return current;
            }

            if (min != AscendingLongIterator.END) {
                last.iterator.advance();
            }
            index = min;
            return current;
        }

        @Override
        public long advanceAtLeastTo(long member) {
            if (index >= member) {
                return index;
            }

            last.iterator.advanceAtLeastTo(member);
            advance();

            return index;
        }

        private void orderAndLink() {
            Arrays.sort(nodes);

            first = nodes[0];
            last = nodes[nodes.length - 1];

            for (int i = 0; i < nodes.length - 1; ++i) {
                nodes[i].next = nodes[i + 1];
            }
            last.next = null;
        }

        @SuppressFBWarnings("EQ_COMPARETO_USE_OBJECT_EQUALS")
        private static final class Node implements Comparable<Node> {

            final AscendingLongIterator iterator;

            Node next;

            Node(AscendingLongIterator iterator) {
                this.iterator = iterator;
            }

            @Override
            public int compareTo(Node that) {
                return Long.compare(this.iterator.getIndex(), that.iterator.getIndex());
            }

            @Override
            public String toString() {
                return Long.toString(iterator.getIndex());
            }

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
                    // newGapEnd will be equal to AscendingLongIterator.END.
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
