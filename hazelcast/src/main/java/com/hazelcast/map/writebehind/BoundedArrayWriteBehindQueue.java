package com.hazelcast.map.writebehind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A bounded queue which throws {@link ReachedMaxSizeException} when it reaches max size.
 *
 * @param <T> Type of entry to be queued.
 */
class BoundedArrayWriteBehindQueue<T> extends ArrayWriteBehindQueue<T> {

    /**
     * upper bound which is used to provide back-pressure.
     */
    private static final int DEFAULT_MAX_SIZE = 2048;

    private final int maxSize;

    BoundedArrayWriteBehindQueue() {
        this(DEFAULT_MAX_SIZE);
    }

    BoundedArrayWriteBehindQueue(List<T> list, int maxSize) {
        super(list);
        this.maxSize = maxSize;
    }

    BoundedArrayWriteBehindQueue(int maxSize) {
        super();
        if (maxSize < 1) {
            throw new IllegalArgumentException("Queue max size should be greater than 0 but found [" + maxSize + "]");
        }
        this.maxSize = maxSize;
    }


    @Override
    public boolean offer(T t) {
        final int currentSize = size();
        if (hasReachedMaxSize(currentSize)) {
            throw new ReachedMaxSizeException("Queue already reached max capacity [" + maxSize + "]");
        }
        return super.offer(t);
    }

    @Override
    public WriteBehindQueue<T> getSnapShot() {
        if (list == null || list.isEmpty()) {
            return WriteBehindQueues.createEmptyWriteBehindQueue();
        }
        return new BoundedArrayWriteBehindQueue<T>(new ArrayList<T>(list), maxSize);
    }

    @Override
    public void addFront(Collection<T> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        final int currentSize = size();
        final int desiredSize = currentSize + collection.size();
        if (hasReachedMaxSize(desiredSize)) {
            throw new ReachedMaxSizeException("Remaining empty slots are not appropriate for this collection."
                    + " Remaining = [" + (maxSize - currentSize) + "]");
        }
        super.addFront(collection);
    }

    @Override
    public void addEnd(Collection<T> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        final int currentSize = size();
        final int desiredSize = currentSize + collection.size();
        if (hasReachedMaxSize(desiredSize)) {
            throw new ReachedMaxSizeException("Remaining empty slots are not appropriate for this collection."
                    + " Remaining = [" + (maxSize - currentSize) + "]");
        }
        super.addEnd(collection);
    }

    private boolean hasReachedMaxSize(int size) {
        return size >= maxSize;
    }
}
