/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.map.ReachedMaxSizeException;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

/**
 * A bounded queue which throws {@link com.hazelcast.map.ReachedMaxSizeException}
 * when it exceeds max size. Used when non-write-coalescing mode is on.
 * <p/>
 * Note that this {@link WriteBehindQueue} implementation is not thread-safe. When it is in action, thread-safe access
 * will be provided by wrapping it in a {@link SynchronizedWriteBehindQueue}
 *
 * @see SynchronizedWriteBehindQueue
 */
class BoundedWriteBehindQueue<E> implements WriteBehindQueue<E> {

    /**
     * Per node write behind queue item counter.
     */
    private final AtomicInteger writeBehindQueueItemCounter;

    /**
     * Allowed max capacity per node which is used to provide back-pressure.
     */
    private final int maxCapacity;

    private final WriteBehindQueue<E> queue;

    BoundedWriteBehindQueue(int maxCapacity, AtomicInteger writeBehindQueueItemCounter, WriteBehindQueue<E> queue) {
        this.maxCapacity = maxCapacity;
        this.writeBehindQueueItemCounter = writeBehindQueueItemCounter;
        this.queue = queue;
    }

    @Override
    public void addFirst(Collection<E> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        addCapacity(collection.size());
        queue.addFirst(collection);
    }

    @Override
    public void addLast(E e) {
        addCapacity(1);
        queue.addLast(e);
    }

    @Override
    public boolean removeFirstOccurrence(E e) {
        boolean result = queue.removeFirstOccurrence(e);
        if (result) {
            addCapacity(-1);
        }
        return result;
    }

    @Override
    public int drainTo(Collection<E> collection) {
        int size = queue.drainTo(collection);
        addCapacity(-size);
        return size;
    }

    @Override
    public boolean contains(E e) {
        return queue.contains(e);
    }

    @Override
    public int size() {
        return queue.size();
    }

    @Override
    public void clear() {
        int size = size();
        queue.clear();
        addCapacity(-size);
    }

    @Override
    public Sequencer getSequencer() {
        return queue.getSequencer();
    }

    @Override
    public List<E> asList() {
        return queue.asList();
    }

    @Override
    public void getFrontByTime(long time, Collection<E> collection) {
        queue.getFrontByTime(time, collection);
    }

    @Override
    public void getFrontByNumber(int numberOfElements, Collection<E> collection) {
        queue.getFrontByNumber(numberOfElements, collection);
    }

    @Override
    public void getFrontBySequence(long sequence, Collection<DelayedEntry> collection) {
        queue.getFrontBySequence(sequence, collection);
    }

    @Override
    public void getEndBySequence(long sequence, Collection<DelayedEntry> collection) {
        queue.getEndBySequence(sequence, collection);
    }

    /**
     * Increments or decrements node-wide {@link WriteBehindQueue} capacity according to the given value.
     * Throws {@link ReachedMaxSizeException} when node-wide maximum capacity which is stated by the variable
     * {@link #maxCapacity} is exceeded.
     *
     * @param capacity capacity to be added or subtracted.
     * @throws ReachedMaxSizeException
     */
    private void addCapacity(int capacity) {
        int maxCapacity = this.maxCapacity;
        AtomicInteger writeBehindQueueItemCounter = this.writeBehindQueueItemCounter;

        int currentCapacity = writeBehindQueueItemCounter.get();
        int newCapacity = currentCapacity + capacity;

        if (newCapacity < 0) {
            return;
        }

        if (maxCapacity < newCapacity) {
            throwException(currentCapacity, maxCapacity, capacity);
        }

        while (!writeBehindQueueItemCounter.compareAndSet(currentCapacity, newCapacity)) {
            currentCapacity = writeBehindQueueItemCounter.get();
            newCapacity = currentCapacity + capacity;

            if (newCapacity < 0) {
                return;
            }

            if (maxCapacity < newCapacity) {
                throwException(currentCapacity, maxCapacity, capacity);
            }
        }
    }

    private void throwException(int currentCapacity, int maxSize, int requiredCapacity) {
        final String msg = format("Reached node-wide max capacity for write-behind-stores. Max allowed capacity = [%d],"
                        + " current capacity = [%d], required capacity = [%d]",
                maxSize, currentCapacity, requiredCapacity);
        throw new ReachedMaxSizeException(msg);
    }

}
