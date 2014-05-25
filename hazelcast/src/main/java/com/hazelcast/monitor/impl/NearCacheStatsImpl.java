package com.hazelcast.monitor.impl;

import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public class NearCacheStatsImpl
        implements NearCacheStats {

    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> HITS_UPDATER = AtomicLongFieldUpdater
            .newUpdater(NearCacheStatsImpl.class, "hits");
    private static final AtomicLongFieldUpdater<NearCacheStatsImpl> MISSES_UPDATER = AtomicLongFieldUpdater
            .newUpdater(NearCacheStatsImpl.class, "misses");

    private long ownedEntryCount;
    private long ownedEntryMemoryCost;
    private long creationTime;

    // These fields are only accessed through the updaters
    private volatile long hits;
    private volatile long misses;

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
        return hits;
    }

    @Override
    public long getMisses() {
        return misses;
    }

    public void setHits(long hits) {
        HITS_UPDATER.set(this, hits);
    }

    @Override
    public double getRatio() {
        return (double) hits / misses;
    }

    public void setOwnedEntryMemoryCost(long ownedEntryMemoryCost) {

        this.ownedEntryMemoryCost = ownedEntryMemoryCost;
    }

    public void incrementMisses() {
        MISSES_UPDATER.incrementAndGet(this);
    }

    public void incrementHits() {
        HITS_UPDATER.incrementAndGet(this);
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeLong(ownedEntryCount);
        out.writeLong(ownedEntryMemoryCost);
        out.writeLong(hits);
        out.writeLong(misses);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        this.ownedEntryCount = in.readLong();
        this.ownedEntryMemoryCost = in.readLong();
        HITS_UPDATER.set(this, in.readLong());
        MISSES_UPDATER.set(this, in.readLong());
    }

    @Override
    public String toString() {
        return "NearCacheStatsImpl{"
                + "ownedEntryCount=" + ownedEntryCount
                + ", ownedEntryMemoryCost=" + ownedEntryMemoryCost
                + ", creationTime=" + creationTime
                + ", hits=" + hits
                + ", misses=" + misses
                + ", ratio=" + getRatio()
                + '}';
    }

}
