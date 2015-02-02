package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.map.ReachedMaxSizeException;

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

    /**
     * Add this collection to the front of the queue.
     *
     * @param collection collection of elements to be added in front of this queue.
     */
    @Override
    public void addFirst(Collection<E> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        addCapacity(collection.size());
        queue.addFirst(collection);
    }

    /**
     * Inserts to the end of this queue.
     *
     * @param e element to be offered
     */
    @Override
    public void addLast(E e) {
        addCapacity(1);
        queue.addLast(e);
    }

    /**
     * Removes the first occurrence of the specified element in this queue
     * when searching it by starting from the head of this queue.
     *
     * @param e element to be removed.
     * @return <code>true</code> if removed successfully, <code>false</code> otherwise
     */
    @Override
    public boolean removeFirstOccurrence(E e) {
        boolean result = queue.removeFirstOccurrence(e);
        if (result) {
            addCapacity(-1);
        }
        return result;
    }

    /**
     * Removes all elements from this queue and adds them
     * to the given collection.
     *
     * @return number of removed items from this queue.
     */
    @Override
    public int drainTo(Collection<E> collection) {
        int size = queue.drainTo(collection);
        addCapacity(-size);
        return size;
    }

    /**
     * Checks whether an element exist in this queue.
     *
     * @param e item to be checked
     * @return <code>true</code> if exists, <code>false</code> otherwise
     */
    @Override
    public boolean contains(E e) {
        return queue.contains(e);
    }

    /**
     * Returns the number of elements in this {@link WriteBehindQueue}.
     *
     * @return the number of elements in this {@link WriteBehindQueue}.
     */
    @Override
    public int size() {
        return queue.size();
    }

    /**
     * Removes all of the elements in this  {@link WriteBehindQueue}
     * Queue will be empty after this method returns.
     */
    @Override
    public void clear() {
        int size = size();
        queue.clear();
        addCapacity(-size);
    }

    /**
     * Returns unmodifiable list representation of this queue.
     *
     * @return read-only list representation of this queue.
     */
    @Override
    public List<E> asList() {
        return queue.asList();
    }

    /**
     * Adds all elements to the supplied collection which are smaller than or equal to the given time.
     *
     * @param time       given time.
     * @param collection to add filtered elements.
     */
    @Override
    public void getFrontByTime(long time, Collection<E> collection) {
        queue.getFrontByTime(time, collection);
    }

    /**
     * Adds some number of elements to the supplied collection starting from the head of this queue.
     *
     * @param numberOfElements number of elements to add.
     * @param collection       to add filtered elements.
     */
    @Override
    public void getFrontByNumber(int numberOfElements, Collection<E> collection) {
        queue.getFrontByNumber(numberOfElements, collection);
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
