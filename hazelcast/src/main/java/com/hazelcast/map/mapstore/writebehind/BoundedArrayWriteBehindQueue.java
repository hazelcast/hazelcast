package com.hazelcast.map.mapstore.writebehind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A bounded queue which throws {@link com.hazelcast.map.mapstore.writebehind.ReachedMaxSizeException}
 * when it reaches max size.
 *
 * @param <T> Type of entry to be queued.
 */
class BoundedArrayWriteBehindQueue<T> extends ArrayWriteBehindQueue<T> {

    /**
     * Per node write behind queue item counter.
     */
    private final AtomicInteger writeBehindQueueItemCounter;

    /**
     * Allowed max size per node which is used to provide back-pressure.
     */
    private final int maxSize;

    BoundedArrayWriteBehindQueue(int maxSize, AtomicInteger writeBehindQueueItemCounter) {
        super();
        this.maxSize = maxSize;
        this.writeBehindQueueItemCounter = writeBehindQueueItemCounter;
    }

    BoundedArrayWriteBehindQueue(List<T> list, int maxSize, AtomicInteger writeBehindQueueItemCounter) {
        super(list);
        this.maxSize = maxSize;
        this.writeBehindQueueItemCounter = writeBehindQueueItemCounter;
    }

    @Override
    public boolean offer(T t) {
        final int currentPerNodeCount = currentPerNodeCount();
        if (hasReachedMaxSize(currentPerNodeCount)) {
            throw new ReachedMaxSizeException("Queue already reached per node max capacity [" + maxSize + "]");
        }
        incrementPerNodeMaxSize();
        return super.offer(t);
    }

    @Override
    public void removeFirst() {
        super.removeFirst();
        decrementPerNodeMaxSize();
    }

    @Override
    public T remove(int index) {
        final T removed = super.remove(index);
        decrementPerNodeMaxSize();
        return removed;
    }

    @Override
    public List<T> removeAll() {
        final List<T> removes = super.removeAll();
        final int size = removes.size();
        decrementPerNodeMaxSize(size);
        return removes;
    }

    @Override
    public void clear() {
        final int size = size();
        super.clear();
        decrementPerNodeMaxSize(size);
    }

    @Override
    public WriteBehindQueue<T> getSnapShot() {
        if (list == null || list.isEmpty()) {
            return WriteBehindQueues.emptyWriteBehindQueue();
        }
        return new BoundedArrayWriteBehindQueue<T>(new ArrayList<T>(list), maxSize, writeBehindQueueItemCounter);
    }

    @Override
    public void addFront(Collection<T> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        final int currentPerNodeCount = currentPerNodeCount();
        final int size = collection.size();
        final int desiredSize = currentPerNodeCount + size;
        if (hasReachedMaxSize(desiredSize)) {
            throw new ReachedMaxSizeException("Remaining per node space is not enough for this collection."
                    + " Remaining = [" + (maxSize - currentPerNodeCount) + "]");
        }
        incrementPerNodeMaxSize(size);
        super.addFront(collection);
    }

    @Override
    public void addEnd(Collection<T> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        final int currentPerNodeCount = currentPerNodeCount();
        final int size = collection.size();
        final int desiredSize = currentPerNodeCount + size;
        if (hasReachedMaxSize(desiredSize)) {
            throw new ReachedMaxSizeException("Remaining per node space is not enough for this collection."
                    + " Remaining = [" + (maxSize - currentPerNodeCount) + "]");
        }
        incrementPerNodeMaxSize(size);
        super.addEnd(collection);
    }

    private boolean hasReachedMaxSize(int size) {
        return size >= maxSize;
    }

    private int currentPerNodeCount() {
        return writeBehindQueueItemCounter.intValue();
    }

    private void incrementPerNodeMaxSize() {
        writeBehindQueueItemCounter.incrementAndGet();
    }

    private void incrementPerNodeMaxSize(int count) {
        writeBehindQueueItemCounter.addAndGet(count);
    }

    private void decrementPerNodeMaxSize() {
        writeBehindQueueItemCounter.decrementAndGet();
    }

    private void decrementPerNodeMaxSize(int size) {
        writeBehindQueueItemCounter.addAndGet(-size);
    }
}
