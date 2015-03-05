package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.NearCacheStats;

import static com.hazelcast.util.JsonUtil.getInt;
import static com.hazelcast.util.JsonUtil.getLong;

/**
 * Default implementation of {@link LocalMapStats}
 */
public class LocalMapStatsImpl
        implements LocalMapStats {

    private InstantLocalMapStats instantLocalMapStats;

    // fields below are calculated on-demand

    private long ownedEntryCount;
    private long backupEntryCount;
    private long ownedEntryMemoryCost;
    private long backupEntryMemoryCost;
    /**
     * Holds total heap cost of map & near-cache & backups.
     */
    private long heapCost;
    private long lockedEntryCount;
    private long dirtyEntryCount;
    private int backupCount;

    private long hits;

    private NearCacheStats nearCacheStats;

    public LocalMapStatsImpl() {
        this(new InstantLocalMapStats());
    }

    public LocalMapStatsImpl(InstantLocalMapStats instantLocalMapStats) {
        this.instantLocalMapStats = instantLocalMapStats;
    }

    @Override
    public long getOwnedEntryCount() {
        return ownedEntryCount;
    }

    public void setOwnedEntryCount(long ownedEntryCount) {
        this.ownedEntryCount = ownedEntryCount;
    }

    public void incrementOwnedEntryCount(long ownedEntryCount) {
        this.ownedEntryCount += ownedEntryCount;
    }

    @Override
    public long getBackupEntryCount() {
        return backupEntryCount;
    }

    public void setBackupEntryCount(long backupEntryCount) {
        this.backupEntryCount = backupEntryCount;
    }

    public void incrementBackupEntryCount(long backupEntryCount) {
        this.backupEntryCount += backupEntryCount;
    }

    @Override
    public int getBackupCount() {
        return backupCount;
    }

    public void setBackupCount(int backupCount) {
        this.backupCount = backupCount;
    }

    @Override
    public long getOwnedEntryMemoryCost() {
        return ownedEntryMemoryCost;
    }

    public void incrementOwnedEntryMemoryCost(long ownedEntryMemoryCost) {
        this.ownedEntryMemoryCost += ownedEntryMemoryCost;
    }

    @Override
    public long getBackupEntryMemoryCost() {
        return backupEntryMemoryCost;
    }

    public void incrementBackupEntryMemoryCost(long backupEntryMemoryCost) {
        this.backupEntryMemoryCost += backupEntryMemoryCost;
    }

    @Override
    public long getCreationTime() {
        return instantLocalMapStats.getCreationTime();
    }

    @Override
    public long getLastAccessTime() {
        return instantLocalMapStats.getLastAccessTime();
    }

    public void setLastAccessTime(long lastAccessTime) {
        instantLocalMapStats.setLastAccessTime(lastAccessTime);
    }

    @Override
    public long getLastUpdateTime() {
        return instantLocalMapStats.getLastUpdateTime();
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        instantLocalMapStats.setLastUpdateTime(lastUpdateTime);
    }

    @Override
    public long getHits() {
        return hits;
    }

    public void setHits(long hits) {
        this.hits = hits;
    }

    public void incrementHits(long hits) {
        this.hits += hits;
    }

    @Override
    public long getLockedEntryCount() {
        return lockedEntryCount;
    }

    public void setLockedEntryCount(long lockedEntryCount) {
        this.lockedEntryCount = lockedEntryCount;
    }

    public void incrementLockedEntryCount(long lockedEntryCount) {
        this.lockedEntryCount += lockedEntryCount;
    }

    @Override
    public long getDirtyEntryCount() {
        return dirtyEntryCount;
    }

    public void setDirtyEntryCount(long dirtyEntryCount) {
        this.dirtyEntryCount = dirtyEntryCount;
    }

    public void incrementDirtyEntryCount(long dirtyEntryCount) {
        this.dirtyEntryCount += dirtyEntryCount;
    }

    @Override
    public long total() {
        return instantLocalMapStats.total();
    }

    @Override
    public long getPutOperationCount() {
        return instantLocalMapStats.getPutOperationCount();
    }

    public void incrementPuts(long latency) {
        instantLocalMapStats.incrementPuts(latency);
    }

    @Override
    public long getGetOperationCount() {
        return instantLocalMapStats.getGetOperationCount();
    }

    public void incrementGets(long latency) {
        instantLocalMapStats.incrementGets(latency);
    }

    @Override
    public long getRemoveOperationCount() {
        return instantLocalMapStats.getRemoveOperationCount();
    }

    public void incrementRemoves(long latency) {
        instantLocalMapStats.incrementRemoves(latency);
    }

    @Override
    public long getTotalPutLatency() {
        return instantLocalMapStats.getTotalPutLatency();
    }

    @Override
    public long getTotalGetLatency() {
        return instantLocalMapStats.getTotalGetLatency();
    }

    @Override
    public long getTotalRemoveLatency() {
        return instantLocalMapStats.getTotalRemoveLatency();
    }

    @Override
    public long getMaxPutLatency() {
        return instantLocalMapStats.getMaxPutLatency();
    }

    @Override
    public long getMaxGetLatency() {
        return instantLocalMapStats.getMaxGetLatency();
    }

    @Override
    public long getMaxRemoveLatency() {
        return instantLocalMapStats.getMaxRemoveLatency();
    }

    @Override
    public long getOtherOperationCount() {
        return instantLocalMapStats.getOtherOperationCount();
    }

    public void incrementOtherOperations() {
        instantLocalMapStats.incrementOtherOperations();
    }

    @Override
    public long getEventOperationCount() {
        return instantLocalMapStats.getEventOperationCount();
    }

    public void incrementReceivedEvents() {
        instantLocalMapStats.incrementReceivedEvents();
    }

    public void incrementHeapCost(long heapCost) {
        this.heapCost += heapCost;
    }

    @Override
    public long getHeapCost() {
        return heapCost;
    }

    @Override
    public NearCacheStats getNearCacheStats() {
        return nearCacheStats;
    }

    public void setNearCacheStats(NearCacheStats nearCacheStats) {
        this.nearCacheStats = nearCacheStats;
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = instantLocalMapStats.toJson();

        root.add("lockedEntryCount", lockedEntryCount);
        root.add("dirtyEntryCount", dirtyEntryCount);
        root.add("ownedEntryCount", ownedEntryCount);
        root.add("backupEntryCount", backupEntryCount);
        root.add("backupCount", backupCount);
        root.add("heapCost", heapCost);
        root.add("ownedEntryMemoryCost", ownedEntryMemoryCost);
        root.add("backupEntryMemoryCost", backupEntryMemoryCost);
        root.add("hits", hits);
        if (nearCacheStats != null) {
            root.add("nearCacheStats", nearCacheStats.toJson());
        }

        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        instantLocalMapStats.fromJson(json);
        ownedEntryCount = getLong(json, "ownedEntryCount", -1L);
        backupEntryCount = getLong(json, "backupEntryCount", -1L);
        backupCount = getInt(json, "backupCount", -1);
        ownedEntryMemoryCost = getLong(json, "ownedEntryMemoryCost", -1L);
        backupEntryMemoryCost = getLong(json, "backupEntryMemoryCost", -1L);
        lockedEntryCount = getLong(json, "lockedEntryCount", -1L);
        dirtyEntryCount = getLong(json, "dirtyEntryCount", -1L);
        heapCost = getLong(json, "heapCost", -1L);
        hits = getLong(json, "hits", -1L);
        final JsonValue jsonNearCacheStats = json.get("nearCacheStats");
        if (jsonNearCacheStats != null) {
            nearCacheStats = new NearCacheStatsImpl();
            nearCacheStats.fromJson(jsonNearCacheStats.asObject());
        }
    }
}
