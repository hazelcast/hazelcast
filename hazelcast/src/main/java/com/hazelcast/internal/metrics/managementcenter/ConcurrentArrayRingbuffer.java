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

package com.hazelcast.internal.metrics.managementcenter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.System.arraycopy;

public class ConcurrentArrayRingbuffer<E> {

    private static final Object[] EMPTY_ARRAY = {};

    private final E[] ringItems;
    /**
     * Head is the sequence where oldest item is found
     */
    private long head;
    /**
     * Tail is the sequence where next item will be added
     */
    private long tail;
    private final int capacity;

    @SuppressWarnings("unchecked")
    public ConcurrentArrayRingbuffer(int capacity) {
        this.capacity = capacity;
        this.ringItems = (E[]) new Object[capacity];
    }

    /**
     * Appends an item to the ring buffer. If size == capacity, also drops the
     * oldest item.
     */
    public synchronized void add(E item) {
        if (tail - capacity == head) {
            head++;
        }
        ringItems[toIndex(tail++)] = item;
    }

    public synchronized void clear() {
        Arrays.fill(ringItems, null);
        head = tail;
    }

    public synchronized E get(long sequence) {
        checkSequence(sequence);
        return ringItems[toIndex(sequence)];
    }

    /**
     * Copies all the items from the given {@code sequence} up to the
     * tail. If the item at the sequence number is already dropped, start from
     * the oldest item.
     *
     * @throws IllegalArgumentException If the sequence is in the future.
     */
    public synchronized RingbufferSlice<E> copyFrom(long sequence) {
        sequence = Math.max(sequence, head);
        if (sequence == tail) {
            return new RingbufferSlice(EMPTY_ARRAY, tail);
        }
        checkSequence(sequence);
        E[] result = (E[]) new Object[(int) (tail - sequence)];
        int startPoint = toIndex(sequence);
        int endPoint = toIndex(tail);
        if (startPoint >= endPoint) {
            arraycopy(ringItems, startPoint, result, 0, capacity - startPoint);
            arraycopy(ringItems, 0, result, capacity - startPoint, endPoint);
        } else {
            arraycopy(ringItems, startPoint, result, 0, endPoint - startPoint);
        }
        return new RingbufferSlice<>(result, tail);
    }

    public int getCapacity() {
        return capacity;
    }

    public synchronized long size() {
        return tail - head;
    }

    public synchronized boolean isEmpty() {
        return tail == head;
    }

    private void checkSequence(long sequence) {
        if (sequence >= tail) {
            throw new IllegalArgumentException("sequence:" + sequence
                    + " is too large. The current tail is:" + tail);
        }

        if (sequence < head) {
            throw new IllegalArgumentException("sequence:" + sequence
                    + " is too small. The current headSequence is:" + head
                    + " tailSequence is:" + tail);
        }
    }

    private int toIndex(long sequence) {
        return (int) (sequence % ringItems.length);
    }

    public static final class RingbufferSlice<E> {
        // we use Object[] (instead of E[]) because we have only serializer for Object[], not for
        // subtypes.
        private Object[] elements;
        private long nextSequence;

        private RingbufferSlice(E[] elements, long nextSequence) {
            this.elements = elements;
            this.nextSequence = nextSequence;
        }

        public List<E> elements() {
            return (List<E>) Arrays.asList(elements);
        }

        public Stream<E> stream() {
            return (Stream<E>) Arrays.stream(elements);
        }

        public boolean isEmpty() {
            return elements.length == 0;
        }

        /**
         * The tail, this is the sequence where next call to {@link
         * ConcurrentArrayRingbuffer#copyFrom}
         * should start.
         */
        public long nextSequence() {
            return nextSequence;
        }
    }
}
