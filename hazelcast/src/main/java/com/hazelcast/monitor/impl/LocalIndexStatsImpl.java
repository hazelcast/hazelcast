package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.monitor.LocalIndexStats;

/**
 * Implementation of local index stats that backs the stats exposed through the
 * public API.
 */
public class LocalIndexStatsImpl implements LocalIndexStats {

    @Probe
    private volatile long creationTime = 0;

    @Probe
    private volatile long entryCount = 0;

    @Probe
    private volatile long queryCount = 0;

    @Probe
    private volatile long hitCount = 0;

    @Probe
    private volatile long averageHitLatency = 0;

    @Probe
    private volatile double averageHitSelectivity = 0.0;

    @Probe
    private volatile long insertCount = 0;

    @Probe
    private volatile long totalInsertLatency = 0;

    @Probe
    private volatile long updateCount = 0;

    @Probe
    private volatile long totalUpdateLatency = 0;

    @Probe
    private volatile long removeCount = 0;

    @Probe
    private volatile long totalRemoveLatency = 0;

    @Probe
    private volatile long onHeapMemoryCost = 0;

    @Probe
    private volatile long offHeapMemoryCost = 0;

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Sets the creation of this stats to the given creation time.
     *
     * @param creationTime the creation time to set.
     */
    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    @Override
    public long getEntryCount() {
        return entryCount;
    }

    /**
     * Sets the entry count of this stats to the given entry count.
     *
     * @param entryCount the entry count to set.
     */
    public void setEntryCount(long entryCount) {
        this.entryCount = entryCount;
    }

    @Override
    public long getQueryCount() {
        return queryCount;
    }

    /**
     * Sets the query count of this stats to the given query count.
     *
     * @param queryCount the query count to set.
     */
    public void setQueryCount(long queryCount) {
        this.queryCount = queryCount;
    }

    @Override
    public long getHitCount() {
        return hitCount;
    }

    /**
     * Sets the hit count of this stats to the given hit count.
     *
     * @param hitCount the hit count to set.
     */
    public void setHitCount(long hitCount) {
        this.hitCount = hitCount;
    }

    @Override
    public long getAverageHitLatency() {
        return averageHitLatency;
    }

    /**
     * Sets the average hit latency of this stats to the given average hit
     * latency.
     *
     * @param averageHitLatency the average hit latency to set.
     */
    public void setAverageHitLatency(long averageHitLatency) {
        this.averageHitLatency = averageHitLatency;
    }

    @Override
    public double getAverageHitSelectivity() {
        return averageHitSelectivity;
    }

    /**
     * Sets the average hit selectivity of this stats to the given average hit
     * selectivity.
     *
     * @param averageHitSelectivity the average hit selectivity to set.
     */
    public void setAverageHitSelectivity(double averageHitSelectivity) {
        this.averageHitSelectivity = averageHitSelectivity;
    }

    @Override
    public long getInsertCount() {
        return insertCount;
    }

    /**
     * Sets the insert count of this stats to the given insert count.
     *
     * @param insertCount the insert count to set.
     */
    public void setInsertCount(long insertCount) {
        this.insertCount = insertCount;
    }

    @Override
    public long getTotalInsertLatency() {
        return totalInsertLatency;
    }

    /**
     * Sets the total insert latency of this stats to the given total insert
     * latency.
     *
     * @param totalInsertLatency the total insert latency to set.
     */
    public void setTotalInsertLatency(long totalInsertLatency) {
        this.totalInsertLatency = totalInsertLatency;
    }

    @Override
    public long getUpdateCount() {
        return updateCount;
    }

    /**
     * Sets the update count of this stats to the given update count.
     *
     * @param updateCount the update count to set.
     */
    public void setUpdateCount(long updateCount) {
        this.updateCount = updateCount;
    }

    @Override
    public long getTotalUpdateLatency() {
        return totalUpdateLatency;
    }

    /**
     * Sets the total update latency of this stats to the given total update
     * latency.
     *
     * @param totalUpdateLatency the total update latency to set.
     */
    public void setTotalUpdateLatency(long totalUpdateLatency) {
        this.totalUpdateLatency = totalUpdateLatency;
    }

    @Override
    public long getRemoveCount() {
        return removeCount;
    }

    /**
     * Sets the remove count of this stats to the given remove count.
     *
     * @param removeCount the remove count to set.
     */
    public void setRemoveCount(long removeCount) {
        this.removeCount = removeCount;
    }

    @Override
    public long getTotalRemoveLatency() {
        return totalRemoveLatency;
    }

    /**
     * Sets the total remove latency of this stats to the given total remove
     * latency.
     *
     * @param totalRemoveLatency the total remove latency to set.
     */
    public void setTotalRemoveLatency(long totalRemoveLatency) {
        this.totalRemoveLatency = totalRemoveLatency;
    }

    @Override
    public long getOnHeapMemoryCost() {
        return onHeapMemoryCost;
    }

    /**
     * Sets the on-heap memory cost of this stats to the given on-heap memory
     * cost value.
     *
     * @param onHeapMemoryCost the on-heap memory cost value to set.
     */
    public void setOnHeapMemoryCost(long onHeapMemoryCost) {
        this.onHeapMemoryCost = onHeapMemoryCost;
    }

    @Override
    public long getOffHeapMemoryCost() {
        return offHeapMemoryCost;
    }

    /**
     * Sets the off-heap memory cost of this stats to the given off-heap memory
     * cost value.
     *
     * @param offHeapMemoryCost the off-heap memory cost value to set.
     */
    public void setOffHeapMemoryCost(long offHeapMemoryCost) {
        this.offHeapMemoryCost = offHeapMemoryCost;
    }

    /**
     * Sets all the values in this stats to the corresponding values in the
     * given on-demand stats.
     *
     * @param onDemandStats the on-demand stats to fetch the values to set from.
     */
    public void setAllFrom(OnDemandIndexStats onDemandStats) {
        this.creationTime = onDemandStats.getCreationTime();
        this.hitCount = onDemandStats.getHitCount();
        this.queryCount = onDemandStats.getQueryCount();
        this.entryCount = onDemandStats.getEntryCount();
        this.averageHitSelectivity = onDemandStats.getAverageHitSelectivity();
        this.averageHitLatency = onDemandStats.getAverageHitLatency();
        this.insertCount = onDemandStats.getInsertCount();
        this.totalInsertLatency = onDemandStats.getTotalInsertLatency();
        this.updateCount = onDemandStats.getUpdateCount();
        this.totalUpdateLatency = onDemandStats.getTotalUpdateLatency();
        this.removeCount = onDemandStats.getRemoveCount();
        this.totalRemoveLatency = onDemandStats.getTotalRemoveLatency();
        this.onHeapMemoryCost = onDemandStats.getOnHeapMemoryCost();
        this.offHeapMemoryCost = onDemandStats.getOffHeapMemoryCost();
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.add("creationTime", creationTime);
        json.add("hitCount", hitCount);
        json.add("queryCount", queryCount);
        json.add("entryCount", entryCount);
        json.add("averageHitSelectivity", averageHitSelectivity);
        json.add("averageHitLatency", averageHitLatency);
        json.add("insertCount", insertCount);
        json.add("totalInsertLatency", totalInsertLatency);
        json.add("updateCount", updateCount);
        json.add("totalUpdateLatency", totalUpdateLatency);
        json.add("removeCount", removeCount);
        json.add("totalRemoveLatency", totalRemoveLatency);
        json.add("onHeapMemoryCost", onHeapMemoryCost);
        json.add("offHeapMemoryCost", offHeapMemoryCost);
        return json;
    }

    @Override
    public void fromJson(JsonObject json) {
        creationTime = json.getLong("creationTime", -1);
        hitCount = json.getLong("hitCount", -1);
        queryCount = json.getLong("queryCount", -1);
        entryCount = json.getLong("entryCount", -1);
        averageHitSelectivity = json.getDouble("averageHitSelectivity", -1.0);
        averageHitLatency = json.getLong("averageHitLatency", -1);
        insertCount = json.getLong("insertCount", -1);
        totalInsertLatency = json.getLong("totalInsertLatency", -1);
        updateCount = json.getLong("updateCount", -1);
        totalUpdateLatency = json.getLong("totalUpdateLatency", -1);
        removeCount = json.getLong("removeCount", -1);
        totalRemoveLatency = json.getLong("totalRemoveLatency", -1);
        onHeapMemoryCost = json.getLong("onHeapMemoryCost", -1);
        offHeapMemoryCost = json.getLong("offHeapMemoryCost", -1);
    }

    @Override
    public String toString() {
        return "LocalIndexStatsImpl{" + "creationTime=" + creationTime + ", hitCount=" + hitCount + ", entryCount=" + entryCount
                + ", queryCount=" + queryCount + ", averageHitSelectivity=" + averageHitSelectivity + ", averageHitLatency="
                + averageHitLatency + ", insertCount=" + insertCount + ", totalInsertLatency=" + totalInsertLatency
                + ", updateCount=" + updateCount + ", totalUpdateLatency=" + totalUpdateLatency + ", removeCount=" + removeCount
                + ", totalRemoveLatency=" + totalRemoveLatency + ", onHeapMemoryCost=" + onHeapMemoryCost + ", offHeapMemoryCost="
                + offHeapMemoryCost + '}';
    }

}
