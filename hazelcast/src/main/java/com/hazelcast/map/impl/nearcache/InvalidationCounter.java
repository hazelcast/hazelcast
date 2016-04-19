package com.hazelcast.map.impl.nearcache;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.HashUtil;

import java.util.concurrent.atomic.AtomicLongArray;

public class InvalidationCounter {

    private final int partitionCount;
    public final AtomicLongArray counters;

    public InvalidationCounter(int partitionCount) {
        this.partitionCount = partitionCount;
        this.counters = new AtomicLongArray(partitionCount);
    }

    public long increase(Data key) {
        int slot = getSlot(key);
        return counters.incrementAndGet(slot);
    }

    public long getCount(Data key) {
        int slot = getSlot(key);
        return counters.get(slot);
    }

    public boolean maybeStale(Data key, long previousCount) {
        int slot = getSlot(key);
        return !counters.compareAndSet(slot, previousCount, previousCount + 1);
    }

    private int getSlot(Data key) {
        int hash = key.getPartitionHash();
        return HashUtil.hashToIndex(hash, partitionCount);
    }
}
