/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.query.LocalIndexStats;

/**
 * Implementation of local index stats that backs the stats exposed through the
 * public API.
 */
@SuppressWarnings("checkstyle:methodcount")
public class LocalIndexStatsImpl implements LocalIndexStats {

    @Probe
    private volatile long creationTime;

    @Probe
    private volatile long queryCount;

    @Probe
    private volatile long hitCount;

    @Probe
    private volatile long averageHitLatency;

    @Probe
    private volatile double averageHitSelectivity;

    @Probe
    private volatile long insertCount;

    @Probe
    private volatile long totalInsertLatency;

    @Probe
    private volatile long updateCount;

    @Probe
    private volatile long totalUpdateLatency;

    @Probe
    private volatile long removeCount;

    @Probe
    private volatile long totalRemoveLatency;

    @Probe
    private volatile long memoryCost;

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
    public long getMemoryCost() {
        return memoryCost;
    }

    /**
     * Sets the memory cost of this stats to the given memory cost value.
     *
     * @param memoryCost the memory cost value to set.
     */
    public void setMemoryCost(long memoryCost) {
        this.memoryCost = memoryCost;
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
        this.averageHitSelectivity = onDemandStats.getAverageHitSelectivity();
        this.averageHitLatency = onDemandStats.getAverageHitLatency();
        this.insertCount = onDemandStats.getInsertCount();
        this.totalInsertLatency = onDemandStats.getTotalInsertLatency();
        this.updateCount = onDemandStats.getUpdateCount();
        this.totalUpdateLatency = onDemandStats.getTotalUpdateLatency();
        this.removeCount = onDemandStats.getRemoveCount();
        this.totalRemoveLatency = onDemandStats.getTotalRemoveLatency();
        this.memoryCost = onDemandStats.getMemoryCost();
    }

    @Override
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.add("creationTime", creationTime);
        json.add("hitCount", hitCount);
        json.add("queryCount", queryCount);
        json.add("averageHitSelectivity", averageHitSelectivity);
        json.add("averageHitLatency", averageHitLatency);
        json.add("insertCount", insertCount);
        json.add("totalInsertLatency", totalInsertLatency);
        json.add("updateCount", updateCount);
        json.add("totalUpdateLatency", totalUpdateLatency);
        json.add("removeCount", removeCount);
        json.add("totalRemoveLatency", totalRemoveLatency);
        json.add("memoryCost", memoryCost);
        return json;
    }

    @Override
    public void fromJson(JsonObject json) {
        creationTime = json.getLong("creationTime", -1);
        hitCount = json.getLong("hitCount", -1);
        queryCount = json.getLong("queryCount", -1);
        averageHitSelectivity = json.getDouble("averageHitSelectivity", -1.0);
        averageHitLatency = json.getLong("averageHitLatency", -1);
        insertCount = json.getLong("insertCount", -1);
        totalInsertLatency = json.getLong("totalInsertLatency", -1);
        updateCount = json.getLong("updateCount", -1);
        totalUpdateLatency = json.getLong("totalUpdateLatency", -1);
        removeCount = json.getLong("removeCount", -1);
        totalRemoveLatency = json.getLong("totalRemoveLatency", -1);
        memoryCost = json.getLong("memoryCost", -1);
    }

    @Override
    public String toString() {
        return "LocalIndexStatsImpl{" + "creationTime=" + creationTime + ", hitCount=" + hitCount + ", queryCount=" + queryCount
                + ", averageHitSelectivity=" + averageHitSelectivity + ", averageHitLatency=" + averageHitLatency
                + ", insertCount=" + insertCount + ", totalInsertLatency=" + totalInsertLatency + ", updateCount=" + updateCount
                + ", totalUpdateLatency=" + totalUpdateLatency + ", removeCount=" + removeCount + ", totalRemoveLatency="
                + totalRemoveLatency + ", memoryCost=" + memoryCost + '}';
    }

}
