package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.util.JsonUtil.getLong;

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
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("ownedEntryCount", ownedEntryCount);
        root.add("ownedEntryMemoryCost", ownedEntryMemoryCost);
        root.add("creationTime", creationTime);
        root.add("hits", hits);
        root.add("misses", misses);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        ownedEntryCount = getLong(json, "ownedEntryCount", -1L);
        ownedEntryMemoryCost = getLong(json, "ownedEntryMemoryCost", -1L);
        creationTime = getLong(json, "creationTime", -1L);
        HITS_UPDATER.set(this, getLong(json, "hits", -1L));
        MISSES_UPDATER.set(this, getLong(json, "misses", -1L));
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
