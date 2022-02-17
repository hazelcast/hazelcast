/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Holds the intermediate results while combining the partitioned index stats
 * to produce the final per-index stats.
 */
@SuppressWarnings("checkstyle:methodcount")
public class OnDemandIndexStats {

    private long creationTime;

    private long entryCount;

    private long queryCount;

    private long hitCount;

    private long averageHitLatency;

    private double averageHitSelectivity;

    private long insertCount;

    private long totalInsertLatency;

    private long updateCount;

    private long totalUpdateLatency;

    private long removeCount;

    private long totalRemoveLatency;

    private long memoryCost;

    private long totalHitCount;

    /**
     * Returns the creation time.
     */
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Sets the creation time the given value.
     *
     * @param creationTime the creation time to set.
     */
    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    /**
     * Returns the query count.
     */
    public long getQueryCount() {
        return queryCount;
    }

    /**
     * Sets the query count to the given value.
     *
     * @param queryCount the query count value to set.
     */
    public void setQueryCount(long queryCount) {
        this.queryCount = queryCount;
    }

    /**
     * Returns the hit count.
     */
    public long getHitCount() {
        return hitCount;
    }

    /**
     * Sets the hit count to the given value.
     *
     * @param hitCount the hit count value to set.
     */
    public void setHitCount(long hitCount) {
        this.hitCount = hitCount;
    }

    /**
     * Returns the average hit latency.
     */
    public long getAverageHitLatency() {
        return averageHitLatency;
    }

    /**
     * Sets the average hit latency to the given value.
     *
     * @param averageHitLatency the average hit latency value to set.
     */
    public void setAverageHitLatency(long averageHitLatency) {
        this.averageHitLatency = averageHitLatency;
    }

    /**
     * Returns the average hit selectivity.
     */
    public double getAverageHitSelectivity() {
        return averageHitSelectivity;
    }

    /**
     * Sets the average hit selectivity to the given value.
     *
     * @param averageHitSelectivity the average hit selectivity value to set.
     */
    public void setAverageHitSelectivity(double averageHitSelectivity) {
        this.averageHitSelectivity = averageHitSelectivity;
    }

    /**
     * Returns the insert count.
     */
    public long getInsertCount() {
        return insertCount;
    }

    /**
     * Sets the insert count to the given value.
     *
     * @param insertCount the insert count value to set.
     */
    public void setInsertCount(long insertCount) {
        this.insertCount = insertCount;
    }

    /**
     * Returns the total insert latency.
     */
    public long getTotalInsertLatency() {
        return totalInsertLatency;
    }

    /**
     * Sets the total insert latency to the given value.
     *
     * @param totalInsertLatency the total insert latency value to set.
     */
    public void setTotalInsertLatency(long totalInsertLatency) {
        this.totalInsertLatency = totalInsertLatency;
    }

    /**
     * Returns the update count.
     */
    public long getUpdateCount() {
        return updateCount;
    }

    /**
     * Sets the update count to the given value.
     *
     * @param updateCount the update count value to set.
     */
    public void setUpdateCount(long updateCount) {
        this.updateCount = updateCount;
    }

    /**
     * Returns the total update latency.
     */
    public long getTotalUpdateLatency() {
        return totalUpdateLatency;
    }

    /**
     * Sets the total update latency to the given value.
     *
     * @param totalUpdateLatency the total update latency value to set.
     */
    public void setTotalUpdateLatency(long totalUpdateLatency) {
        this.totalUpdateLatency = totalUpdateLatency;
    }

    /**
     * Returns the remove count.
     */
    public long getRemoveCount() {
        return removeCount;
    }

    /**
     * Sets the remove count to the given value.
     *
     * @param removeCount the remove count value to set.
     */
    public void setRemoveCount(long removeCount) {
        this.removeCount = removeCount;
    }

    /**
     * Returns the total remove latency.
     */
    public long getTotalRemoveLatency() {
        return totalRemoveLatency;
    }

    /**
     * Sets the total remove latency to the given value.
     *
     * @param totalRemoveLatency the total remove latency value to set.
     */
    public void setTotalRemoveLatency(long totalRemoveLatency) {
        this.totalRemoveLatency = totalRemoveLatency;
    }

    /**
     * Returns the memory cost.
     */
    public long getMemoryCost() {
        return memoryCost;
    }

    /**
     * Sets the memory cost to the given value.
     *
     * @param memoryCost the memory cost value to set.
     */
    public void setMemoryCost(long memoryCost) {
        this.memoryCost = memoryCost;
    }

    /**
     * Returns the total hit count.
     */
    public long getTotalHitCount() {
        return totalHitCount;
    }

    /**
     * Sets the total hit count to the given value.
     *
     * @param totalHitCount the total hit count value to set.
     */
    public void setTotalHitCount(long totalHitCount) {
        this.totalHitCount = totalHitCount;
    }

    @Override
    public String toString() {
        return "LocalIndexStatsImpl{" + "creationTime=" + creationTime + ", hitCount=" + hitCount + ", entryCount=" + entryCount
                + ", queryCount=" + queryCount + ", averageHitSelectivity=" + averageHitSelectivity + ", averageHitLatency="
                + averageHitLatency + ", insertCount=" + insertCount + ", totalInsertLatency=" + totalInsertLatency
                + ", updateCount=" + updateCount + ", totalUpdateLatency=" + totalUpdateLatency + ", removeCount=" + removeCount
                + ", totalRemoveLatency=" + totalRemoveLatency + ", memoryCost=" + memoryCost + ", totalHitCount=" + totalHitCount
                + '}';
    }

}
