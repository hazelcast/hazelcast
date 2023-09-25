/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.util;

import java.util.Queue;

import static com.hazelcast.internal.tpcengine.util.BitUtil.nextPowerOfTwo;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.tpcengine.util.Preconditions.checkPositive;

/**
 * A CircularQueue.
 * <p/>
 * This class is not thread-safe.
 *
 * @param <E> the type of elements in this CircularQueue.
 */
public final class CircularQueue<E> {

    private long head;
    private long tail = -1;
    private final E[] array;
    private final int mask;
    private final int capacity;

    /**
     * Creates a CircularQueue with the given capacity.
     *
     * @param capacity the capacity.
     * @throws IllegalArgumentException if capacity not positive.
     */
    public CircularQueue(int capacity) {
        int fixedCapacity = nextPowerOfTwo(checkPositive(capacity, "capacity"));
        this.capacity = fixedCapacity;
        this.array = (E[]) new Object[fixedCapacity];
        this.mask = fixedCapacity - 1;
    }

    /**
     * Checks if the CircularQueue is full.
     *
     * @return true if full.
     */
    public boolean isFull() {
        return tail - head + 1 == capacity;
    }

    /**
     * Returns the number of free spots
     *
     * @return number of remaining spaces.
     */
    public int remaining() {
        return capacity - size();
    }

    /**
     * Returns the capacity of this CircularQueue.
     *
     * @return the capacity.
     */
    public int capacity() {
        return capacity;
    }

    /**
     * Returns the number of items in this CircularQueue.
     *
     * @return the number of items
     */
    public int size() {
        return (int) (tail - head + 1);
    }

    /**
     * Checks if this CircularQueue is empty.
     *
     * @return true if empty.
     */
    public boolean isEmpty() {
        return tail < head;
    }

    /**
     * Adds an item to this CircularQueue.
     *
     * @param item the item to add.
     * @throws NullPointerException  if item is null.
     * @throws IllegalStateException when there is no more space on this CircularQueue.
     */
    public void add(E item) {
        if (!offer(item)) {
            throw new IllegalStateException("CircularQueue is full");
        }
    }

    /**
     * Drains as many items from the src as fit into this CircularQueue.
     *
     * @param src the queue to drain.
     * @return the number of items drained.
     */
    public int drainFrom(Queue<E> src) {
        int remaining = remaining();
        int count = 0;
        for (int k = 0; k < remaining; k++) {
            E item = src.poll();
            if (item == null) {
                break;
            }
            count++;
            long t = tail + 1;
            int index = (int) (t & mask);
            array[index] = item;
            this.tail = t;
        }
        return count;
    }

    /**
     * Peeks in the CircularQueue. So returns the first item without removing it. If the CircularQueue
     * is empty, null is returned.
     *
     * @return the first item or null.
     */
    public E peek() {
        if (tail < head) {
            return null;
        } else {
            long h = head;
            int index = (int) (h & mask);
            return array[index];
        }
    }

    /**
     * Offers an item.
     *
     * @param item the item to offer.
     * @return true if the item was accepted, false otherwise.
     * @throws NullPointerException if item is null.
     */
    public boolean offer(E item) {
        checkNotNull(item, "item");

        if (tail - head + 1 == capacity) {
            return false;
        } else {
            long t = tail + 1;
            int index = (int) (t & mask);
            array[index] = item;
            tail = t;
            return true;
        }
    }

    /**
     * Removes the oldest items or returns null of no such item exists.
     *
     * @return the oldest items.
     */
    public E poll() {
        if (tail < head) {
            return null;
        } else {
            long h = head;
            int index = (int) (h & mask);
            E item = array[index];
            array[index] = null;
            head = h + 1;
            return item;
        }
    }
}
