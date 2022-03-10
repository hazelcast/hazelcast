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

package com.hazelcast.internal.alto.util;

import java.util.Queue;

import static com.hazelcast.internal.util.QuickMath.nextPowerOfTwo;

public final class CircularQueue<E> {

    private long head;
    private long tail = -1;
    private final E[] array;
    private final int mask;
    private final int capacity;

    // Capacity should be power of 2
    public CircularQueue(int capacity) {
        int fixedCapacity = nextPowerOfTwo(capacity);
        this.capacity = fixedCapacity;
        this.array = (E[]) new Object[fixedCapacity];
        this.mask = fixedCapacity - 1;
    }

    public void add(E item) {
        if (!offer(item)) {
            throw new IllegalStateException("CircularQueue is full");
        }
    }

    public int fill(Queue<E> queue) {
        int remaining = remaining();
        int count = 0;
        for (int k = 0; k < remaining; k++) {
            E item = queue.poll();
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

    public boolean isFull() {
        return tail - head + 1 == capacity;
    }

    /**
     * Returns the number of free spots
     *
     * @return
     */
    public int remaining() {
        return capacity - size();
    }

    public int capacity() {
        return capacity;
    }

    public int size() {
        return (int) (tail - head + 1);
    }

    public boolean isEmpty() {
        return tail < head;
    }

    public E peek() {
        if (tail < head) {
            return null;
        }

        long h = head;
        int index = (int) (h & mask);
        return array[index];
    }

    public boolean offer(E item) {
        if (tail - head + 1 == capacity) {
            return false;
        }

        long t = tail + 1;
        int index = (int) (t & mask);
        array[index] = item;
        this.tail = t;
        return true;
    }

    public E poll() {
        if (tail < head) {
            return null;
        }

        long h = head;
        int index = (int) (h & mask);
        E item = array[index];
        array[index] = null;
        this.head = h + 1;
        return item;
    }
}
