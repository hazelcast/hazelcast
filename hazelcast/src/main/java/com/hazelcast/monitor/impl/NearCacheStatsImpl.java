package com.hazelcast.monitor.impl;

import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * User: eminn
 * Date: 03/12/13
 * Time: 10:58 AM
 */
public class NearCacheStatsImpl implements NearCacheStats {

    private long ownedEntryCount;
    private long ownedEntryMemoryCost;
    private long creationTime;
    private AtomicInteger hits = new AtomicInteger(0);
    private AtomicInteger misses = new AtomicInteger(0);

    public NearCacheStatsImpl() {
        this.creationTime = Clock.currentTimeMillis();
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getOwnedEntryCount() {
        return ownedEntryCount;
    }

    public void setOwnedEntryCount(long ownedEntryCount) {
        this.ownedEntryCount = ownedEntryCount;
    }

    @Override
    public long getOwnedEntryMemoryCost() {
        return ownedEntryMemoryCost;
    }

    @Override
    public long getHits() {
        return hits.get();
    }

    @Override
    public long getMisses() {
        return misses.get();
    }

    public void setHits(int hits) {
        this.hits.set(hits);
    }

    public void setOwnedEntryMemoryCost(long ownedEntryMemoryCost) {

        this.ownedEntryMemoryCost = ownedEntryMemoryCost;
    }

    public void incrementMisses()
    {
        misses.incrementAndGet();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(ownedEntryCount);
        out.writeLong(ownedEntryMemoryCost);
        out.writeInt(hits.get());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.ownedEntryCount = in.readLong();
        this.ownedEntryMemoryCost = in.readLong();
        this.hits.set(in.readInt());
    }

    @Override
    public String toString() {
        return "NearCacheStatsImpl{" +
                "ownedEntryCount=" + ownedEntryCount +
                ", ownedEntryMemoryCost=" + ownedEntryMemoryCost +
                ", creationTime=" + creationTime +
                ", hits=" + hits +
                '}';
    }
}
