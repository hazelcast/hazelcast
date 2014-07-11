package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.nio.serialization.Data;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class BoundedCoalescedWriteBehindQueue extends CoalescedWriteBehindQueue {

    /**
     * Per node write behind queue item counter.
     */
    private final AtomicInteger writeBehindQueueItemCounter;

    /**
     * Allowed max size per node which is used to provide back-pressure.
     */
    private final int maxSize;

    BoundedCoalescedWriteBehindQueue(int maxSize, AtomicInteger writeBehindQueueItemCounter) {
        super();
        this.maxSize = maxSize;
        this.writeBehindQueueItemCounter = writeBehindQueueItemCounter;
    }

    BoundedCoalescedWriteBehindQueue(Map map, int maxSize, AtomicInteger writeBehindQueueItemCounter) {
        super(map);
        this.maxSize = maxSize;
        this.writeBehindQueueItemCounter = writeBehindQueueItemCounter;
    }

    @Override
    public boolean offer(DelayedEntry entry) {
        if (entry == null) {
            return false;
        }
        final int currentPerNodeCount = currentPerNodeCount();
        if (hasReachedMaxSize(currentPerNodeCount)) {
            throw new ReachedMaxSizeException("Queue already reached per node max capacity [" + maxSize + "]");
        }
        incrementPerNodeMaxSize();
        return super.offer(entry);
    }

    @Override
    public void removeFirst() {
        super.removeFirst();
        decrementPerNodeMaxSize();
    }

    @Override
    public List<DelayedEntry<Data, Object>> removeAll() {
        final List<DelayedEntry<Data, Object>> removes = super.removeAll();
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
    public WriteBehindQueue<DelayedEntry<Data, Object>> getSnapShot() {
        if (queue == null || queue.isEmpty()) {
            return WriteBehindQueues.emptyWriteBehindQueue();
        }
        return new BoundedCoalescedWriteBehindQueue(queue, maxSize, writeBehindQueueItemCounter);
    }

    @Override
    public void addFront(Collection<DelayedEntry<Data, Object>> collection) {
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
    public void addEnd(Collection<DelayedEntry<Data, Object>> collection) {
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

    @Override
    public void removeAll(Collection<DelayedEntry<Data, Object>> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        super.removeAll(collection);
        decrementPerNodeMaxSize(collection.size());
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
