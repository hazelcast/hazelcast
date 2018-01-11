/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.querycache.accumulator;

import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.QuickMath.nextPowerOfTwo;

/**
 * Default implementation of {@link CyclicBuffer} interface.
 * This class is not thread-safe and only one thread can access it at a time.
 *
 * @param <E> the element to be placed in this buffer.
 * @see CyclicBuffer
 */
public class DefaultCyclicBuffer<E extends Sequenced> implements CyclicBuffer<E> {

    private static final long UNAVAILABLE = -1L;

    private int capacity;
    private E[] buffer;
    private AtomicLong headSequence;
    private AtomicLong tailSequence;

    public DefaultCyclicBuffer(int capacity) throws IllegalArgumentException {
        checkPositive(capacity, "capacity");

        init(capacity);
    }

    private void init(int maxSize) {
        this.capacity = nextPowerOfTwo(maxSize);
        this.buffer = (E[]) new Sequenced[capacity];
        this.tailSequence = new AtomicLong(UNAVAILABLE);
        this.headSequence = new AtomicLong(UNAVAILABLE);
    }

    @Override
    public void add(E event) {
        checkNotNull(event, "event cannot be null");

        long sequence = event.getSequence();
        int tailIndex = findIndex(sequence);
        buffer[tailIndex] = event;
        tailSequence.set(sequence);

        long head = headSequence.get();
        if (head == UNAVAILABLE) {
            headSequence.set(sequence);
        } else {
            if (head != sequence) {
                int headIndex = findIndex(head);
                if (headIndex == tailIndex) {
                    if (++headIndex == capacity) {
                        headIndex = 0;
                    }
                    E e = buffer[headIndex];
                    if (e != null) {
                        headSequence.set(e.getSequence());
                    } else {
                        headSequence.incrementAndGet();
                    }
                }
            }
        }
    }

    @Override
    public E get(long sequence) {
        checkPositive(sequence, "sequence");

        int index = findIndex(sequence);
        E e = buffer[index];
        if (e != null && e.getSequence() != sequence) {
            return null;
        }
        return e;
    }

    @Override
    public boolean setHead(long sequence) {
        checkPositive(sequence, "sequence");

        E e = get(sequence);
        if (e == null) {
            return false;
        } else {
            headSequence.set(sequence);
            return true;
        }
    }

    @Override
    public E getAndAdvance() {
        long head = headSequence.get();
        long tail = tailSequence.get();

        if (tail == UNAVAILABLE || head > tail) {
            return null;
        }

        int headIndex = findIndex(head);
        E e = buffer[headIndex];
        if (e == null) {
            return null;
        }
        headSequence.incrementAndGet();
        return e;
    }

    @Override
    public void reset() {
        init(this.capacity);
    }


    @Override
    public int size() {
        long head = headSequence.get();
        long tail = tailSequence.get();

        if (tail == UNAVAILABLE) {
            return 0;
        }

        int avail = (int) (tail - head + 1);
        if (avail <= capacity) {
            return avail;
        } else {
            return capacity;
        }
    }

    @Override
    public long getHeadSequence() {
        return headSequence.get();
    }

    private int findIndex(long sequence) {
        return (int) (sequence % capacity);
    }
}
