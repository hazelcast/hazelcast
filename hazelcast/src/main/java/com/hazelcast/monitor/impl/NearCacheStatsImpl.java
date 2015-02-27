package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.monitor.NearCacheStats;

import static com.hazelcast.util.JsonUtil.getLong;

/**
 * Default implementation of {@link com.hazelcast.monitor.NearCacheStats}
 */
public class NearCacheStatsImpl
        implements NearCacheStats {


    private InstantNearCacheStats instantNearCacheStats;

    private long ownedEntryCount;
    private long ownedEntryMemoryCost;

    public NearCacheStatsImpl() {
        this(new InstantNearCacheStats());
    }

    public NearCacheStatsImpl(InstantNearCacheStats instantNearCacheStats) {
        this.instantNearCacheStats = instantNearCacheStats;
    }

    @Override
    public long getCreationTime() {
        return instantNearCacheStats.getCreationTime();
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
        return instantNearCacheStats.getHits();
    }

    @Override
    public long getMisses() {
        return instantNearCacheStats.getMisses();
    }

    public void setHits(long hits) {
        instantNearCacheStats.setHits(hits);
    }

    @Override
    public double getRatio() {
        return instantNearCacheStats.getRatio();
    }

    public void setOwnedEntryMemoryCost(long ownedEntryMemoryCost) {

        this.ownedEntryMemoryCost = ownedEntryMemoryCost;
    }

    public void incrementMisses() {
        instantNearCacheStats.incrementMisses();
    }

    public void incrementHits() {
        instantNearCacheStats.incrementHits();
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = instantNearCacheStats.toJson();
        root.add("ownedEntryCount", ownedEntryCount);
        root.add("ownedEntryMemoryCost", ownedEntryMemoryCost);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        instantNearCacheStats.fromJson(json);
        ownedEntryCount = getLong(json, "ownedEntryCount", -1L);
        ownedEntryMemoryCost = getLong(json, "ownedEntryMemoryCost", -1L);
    }

    @Override
    public String toString() {
        return "NearCacheStatsImpl{"
                + "ownedEntryCount=" + ownedEntryCount
                + ", ownedEntryMemoryCost=" + ownedEntryMemoryCost
                + ", ratio=" + getRatio()
                + '}';
    }

}
