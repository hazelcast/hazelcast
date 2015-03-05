package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.management.JsonSerializable;
import com.hazelcast.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.util.JsonUtil.getLong;

/**
 * Internal API
 */
public class InstantNearCacheStats
        implements JsonSerializable {

    private static final AtomicLongFieldUpdater<InstantNearCacheStats> HITS_UPDATER = AtomicLongFieldUpdater
            .newUpdater(InstantNearCacheStats.class, "hits");
    private static final AtomicLongFieldUpdater<InstantNearCacheStats> MISSES_UPDATER = AtomicLongFieldUpdater
            .newUpdater(InstantNearCacheStats.class, "misses");

    private volatile long creationTime;

    // These fields are only accessed through the updaters
    private volatile long hits;
    private volatile long misses;

    public InstantNearCacheStats() {
        this.creationTime = Clock.currentTimeMillis();
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getHits() {
        return hits;
    }

    public long getMisses() {
        return misses;
    }

    public void setHits(long hits) {
        HITS_UPDATER.set(this, hits);
    }

    public double getRatio() {
        return (double) hits / misses;
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
        root.add("creationTime", creationTime);
        root.add("hits", hits);
        root.add("misses", misses);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        creationTime = getLong(json, "creationTime", -1L);
        HITS_UPDATER.set(this, getLong(json, "hits", -1L));
        MISSES_UPDATER.set(this, getLong(json, "misses", -1L));
    }

    @Override
    public String toString() {
        return "NearCacheStatsImpl{"
                + "creationTime=" + creationTime
                + ", hits=" + hits
                + ", misses=" + misses
                + ", ratio=" + getRatio()
                + '}';
    }

}
