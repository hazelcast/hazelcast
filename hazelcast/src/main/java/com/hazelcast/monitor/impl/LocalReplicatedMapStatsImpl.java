/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.monitor.LocalReplicatedMapStats;
import com.hazelcast.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.util.JsonUtil.getLong;

/**
 * This class collects statistics about the replication map usage for management center and is
 * able to transform those between wire format and instance view
 */
public class LocalReplicatedMapStatsImpl
        implements LocalReplicatedMapStats {

    //CHECKSTYLE:OFF
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> LAST_ACCESS_TIME_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalReplicatedMapStatsImpl.class, "lastAccessTime");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> LAST_UPDATE_TIME_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalReplicatedMapStatsImpl.class, "lastUpdateTime");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> HITS_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalReplicatedMapStatsImpl.class, "hits");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> NUMBER_OF_OTHER_OPERATIONS_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalReplicatedMapStatsImpl.class, "numberOfOtherOperations");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> NUMBER_OF_EVENTS_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalReplicatedMapStatsImpl.class, "numberOfEvents");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> NUMBER_OF_REPLICATION_EVENTS_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalReplicatedMapStatsImpl.class, "numberOfReplicationEvents");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> GET_COUNT_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalReplicatedMapStatsImpl.class, "getCount");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> PUT_COUNT_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalReplicatedMapStatsImpl.class, "putCount");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> REMOVE_COUNT_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalReplicatedMapStatsImpl.class, "removeCount");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> TOTAL_GET_LATENCIES_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalReplicatedMapStatsImpl.class, "totalGetLatencies");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> TOTAL_PUT_LATENCIES_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalReplicatedMapStatsImpl.class, "totalPutLatencies");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> TOTAL_REMOVE_LATENCIES_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalReplicatedMapStatsImpl.class, "totalRemoveLatencies");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> MAX_GET_LATENCY_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalReplicatedMapStatsImpl.class, "maxGetLatency");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> MAX_PUT_LATENCY_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalReplicatedMapStatsImpl.class, "maxPutLatency");
    private static final AtomicLongFieldUpdater<LocalReplicatedMapStatsImpl> MAX_REMOVE_LATENCY_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalReplicatedMapStatsImpl.class, "maxRemoveLatency");
    //CHECKSTYLE:ON

    // These fields are only accessed through the updaters
    private volatile long lastAccessTime;
    private volatile long lastUpdateTime;
    private volatile long hits;
    private volatile long numberOfOtherOperations;
    private volatile long numberOfEvents;
    private volatile long numberOfReplicationEvents;
    private volatile long getCount;
    private volatile long putCount;
    private volatile long removeCount;
    private volatile long totalGetLatencies;
    private volatile long totalPutLatencies;
    private volatile long totalRemoveLatencies;
    private volatile long maxGetLatency;
    private volatile long maxPutLatency;
    private volatile long maxRemoveLatency;

    private long ownedEntryCount;
    private long creationTime;

    public LocalReplicatedMapStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("getCount", getCount);
        root.add("putCount", putCount);
        root.add("removeCount", removeCount);
        root.add("numberOfOtherOperations", numberOfOtherOperations);
        root.add("numberOfEvents", numberOfEvents);
        root.add("numberOfReplicationEvents", numberOfReplicationEvents);
        root.add("lastAccessTime", lastAccessTime);
        root.add("lastUpdateTime", lastUpdateTime);
        root.add("hits", hits);
        root.add("ownedEntryCount", ownedEntryCount);
        root.add("creationTime", creationTime);
        root.add("totalGetLatencies", totalGetLatencies);
        root.add("totalPutLatencies", totalPutLatencies);
        root.add("totalRemoveLatencies", totalRemoveLatencies);
        root.add("maxGetLatency", maxGetLatency);
        root.add("maxPutLatency", maxPutLatency);
        root.add("maxRemoveLatency", maxRemoveLatency);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        GET_COUNT_UPDATER.set(this, getLong(json, "getCount", -1L));
        PUT_COUNT_UPDATER.set(this, getLong(json, "putCount", -1L));
        REMOVE_COUNT_UPDATER.set(this, getLong(json, "removeCount", -1L));
        NUMBER_OF_OTHER_OPERATIONS_UPDATER.set(this, getLong(json, "numberOfOtherOperations", -1L));
        NUMBER_OF_EVENTS_UPDATER.set(this, getLong(json, "numberOfEvents", -1L));
        NUMBER_OF_REPLICATION_EVENTS_UPDATER.set(this, getLong(json, "numberOfReplicationEvents", -1L));
        LAST_ACCESS_TIME_UPDATER.set(this, getLong(json, "lastAccessTime", -1L));
        LAST_UPDATE_TIME_UPDATER.set(this, getLong(json, "lastUpdateTime", -1L));
        HITS_UPDATER.set(this, getLong(json, "hits", -1L));
        ownedEntryCount = getLong(json, "ownedEntryCount", -1L);
        creationTime = getLong(json, "creationTime", -1L);
        TOTAL_GET_LATENCIES_UPDATER.set(this, getLong(json, "totalGetLatencies", -1L));
        TOTAL_PUT_LATENCIES_UPDATER.set(this, getLong(json, "totalPutLatencies", -1L));
        TOTAL_REMOVE_LATENCIES_UPDATER.set(this, getLong(json, "totalRemoveLatencies", -1L));
        MAX_GET_LATENCY_UPDATER.set(this, getLong(json, "maxGetLatency", -1L));
        MAX_PUT_LATENCY_UPDATER.set(this, getLong(json, "maxPutLatency", -1L));
        MAX_REMOVE_LATENCY_UPDATER.set(this, getLong(json, "maxRemoveLatency", -1L));
    }

    @Override
    public long getOwnedEntryCount() {
        return ownedEntryCount;
    }

    public void setOwnedEntryCount(long ownedEntryCount) {
        this.ownedEntryCount = ownedEntryCount;
    }

    @Override
    public long getBackupEntryCount() {
        return 0;
    }

    //todo: unused.
    public void setBackupEntryCount(long backupEntryCount) {
    }

    @Override
    public int getBackupCount() {
        return 0;
    }

    public void setBackupCount(int backupCount) {
    }

    @Override
    public long getOwnedEntryMemoryCost() {
        return 0;
    }

    //todo:unused
    public void setOwnedEntryMemoryCost(long ownedEntryMemoryCost) {
    }

    @Override
    public long getBackupEntryMemoryCost() {
        return 0;
    }

    //todo:unused
    public void setBackupEntryMemoryCost(long backupEntryMemoryCost) {
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        long max = Math.max(this.lastAccessTime, lastAccessTime);
        LAST_ACCESS_TIME_UPDATER.set(this, max);
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        long max = Math.max(this.lastUpdateTime, lastUpdateTime);
        LAST_UPDATE_TIME_UPDATER.set(this, max);
    }

    @Override
    public long getHits() {
        return hits;
    }

    public void setHits(long hits) {
        HITS_UPDATER.set(this, hits);
    }

    @Override
    public long getLockedEntryCount() {
        return 0;
    }

    public void setLockedEntryCount(long lockedEntryCount) {
    }

    @Override
    public long getDirtyEntryCount() {
        return 0;
    }

    public void setDirtyEntryCount(long l) {
    }

    @Override
    public long total() {
        return putCount + getCount + removeCount + numberOfOtherOperations;
    }

    @Override
    public long getPutOperationCount() {
        return putCount;
    }

    public void incrementPuts(long latency) {
        PUT_COUNT_UPDATER.incrementAndGet(this);
        TOTAL_PUT_LATENCIES_UPDATER.addAndGet(this, latency);
        MAX_PUT_LATENCY_UPDATER.set(this, Math.max(maxPutLatency, latency));
    }

    @Override
    public long getGetOperationCount() {
        return getCount;
    }

    public void incrementGets(long latency) {
        GET_COUNT_UPDATER.incrementAndGet(this);
        TOTAL_GET_LATENCIES_UPDATER.addAndGet(this, latency);
        MAX_GET_LATENCY_UPDATER.set(this, Math.max(maxGetLatency, latency));
    }

    @Override
    public long getRemoveOperationCount() {
        return removeCount;
    }

    public void incrementRemoves(long latency) {
        REMOVE_COUNT_UPDATER.incrementAndGet(this);
        TOTAL_REMOVE_LATENCIES_UPDATER.addAndGet(this, latency);
        MAX_REMOVE_LATENCY_UPDATER.set(this, Math.max(maxRemoveLatency, latency));
    }

    @Override
    public long getTotalPutLatency() {
        return totalPutLatencies;
    }

    @Override
    public long getTotalGetLatency() {
        return totalGetLatencies;
    }

    @Override
    public long getTotalRemoveLatency() {
        return totalRemoveLatencies;
    }

    @Override
    public long getMaxPutLatency() {
        return maxPutLatency;
    }

    @Override
    public long getMaxGetLatency() {
        return maxGetLatency;
    }

    @Override
    public long getMaxRemoveLatency() {
        return maxRemoveLatency;
    }

    @Override
    public long getOtherOperationCount() {
        return numberOfOtherOperations;
    }

    public void incrementOtherOperations() {
        NUMBER_OF_OTHER_OPERATIONS_UPDATER.incrementAndGet(this);
    }

    @Override
    public long getEventOperationCount() {
        return numberOfEvents;
    }

    public void incrementReceivedEvents() {
        NUMBER_OF_EVENTS_UPDATER.incrementAndGet(this);
    }

    @Override
    public long getReplicationEventCount() {
        return numberOfReplicationEvents;
    }

    public void incrementReceivedReplicationEvents() {
        NUMBER_OF_REPLICATION_EVENTS_UPDATER.incrementAndGet(this);
    }

    //todo: unused
    public void setHeapCost(long heapCost) {
    }

    @Override
    public long getHeapCost() {
        return 0;
    }

    @Override
    public NearCacheStatsImpl getNearCacheStats() {
        throw new UnsupportedOperationException("Replicated map has no Near Cache!");
    }

    public String toString() {
        return "LocalReplicatedMapStatsImpl{" + "lastAccessTime=" + lastAccessTime + ", lastUpdateTime=" + lastUpdateTime
                + ", hits=" + hits + ", numberOfOtherOperations=" + numberOfOtherOperations + ", numberOfEvents=" + numberOfEvents
                + ", numberOfReplicationEvents=" + numberOfReplicationEvents + ", getCount=" + getCount + ", putCount=" + putCount
                + ", removeCount=" + removeCount + ", totalGetLatencies=" + totalGetLatencies + ", totalPutLatencies="
                + totalPutLatencies + ", totalRemoveLatencies=" + totalRemoveLatencies + ", ownedEntryCount=" + ownedEntryCount
                + ", creationTime=" + creationTime + '}';
    }

}
