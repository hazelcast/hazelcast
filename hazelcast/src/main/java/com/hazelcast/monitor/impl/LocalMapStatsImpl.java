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
import com.eclipsesource.json.JsonValue;
import com.hazelcast.map.MapDataSerializerHook;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.util.JsonUtil.getInt;
import static com.hazelcast.util.JsonUtil.getLong;

public class LocalMapStatsImpl
        implements LocalMapStats, IdentifiedDataSerializable {

    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> LAST_ACCESS_TIME_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalMapStatsImpl.class, "lastAccessTime");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> LAST_UPDATE_TIME_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalMapStatsImpl.class, "lastUpdateTime");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> HITS_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalMapStatsImpl.class, "hits");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> NUMBER_OF_OTHER_OPERATIONS_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalMapStatsImpl.class, "numberOfOtherOperations");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> NUMBER_OF_EVENTS_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalMapStatsImpl.class, "numberOfEvents");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> GET_COUNT_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalMapStatsImpl.class, "getCount");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> PUT_COUNT_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalMapStatsImpl.class, "putCount");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> REMOVE_COUNT_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalMapStatsImpl.class, "removeCount");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> TOTAL_GET_LATENCIES_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalMapStatsImpl.class, "totalGetLatencies");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> TOTAL_PUT_LATENCIES_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalMapStatsImpl.class, "totalPutLatencies");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> TOTAL_REMOVE_LATENCIES_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalMapStatsImpl.class, "totalRemoveLatencies");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> MAX_GET_LATENCY_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalMapStatsImpl.class, "maxGetLatency");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> MAX_PUT_LATENCY_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalMapStatsImpl.class, "maxPutLatency");
    private static final AtomicLongFieldUpdater<LocalMapStatsImpl> MAX_REMOVE_LATENCY_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalMapStatsImpl.class, "maxRemoveLatency");


    // These fields are only accessed through the updaters
    private volatile long creationTime;
    private volatile long lastAccessTime;
    private volatile long lastUpdateTime;
    private volatile long hits;
    private volatile long numberOfOtherOperations;
    private volatile long numberOfEvents;
    private volatile long getCount;
    private volatile long putCount;
    private volatile long removeCount;
    private volatile long totalGetLatencies;
    private volatile long totalPutLatencies;
    private volatile long totalRemoveLatencies;
    private volatile long maxGetLatency;
    private volatile long maxPutLatency;
    private volatile long maxRemoveLatency;

    private AtomicLong ownedEntryCount = new AtomicLong(0);
    private AtomicLong backupEntryCount = new AtomicLong(0);
    private AtomicLong ownedEntryMemoryCost = new AtomicLong(0);
    private AtomicLong backupEntryMemoryCost = new AtomicLong(0);
    /**
     * Holds total heap cost of map & near-cache & backups.
     */
    private AtomicLong heapCost = new AtomicLong(0);
    private AtomicLong lockedEntryCount = new AtomicLong(0);
    private AtomicLong dirtyEntryCount = new AtomicLong(0);
    private AtomicInteger backupCount = new AtomicInteger(0);

    private volatile NearCacheStatsImpl nearCacheStats;

    public LocalMapStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeLong(getCount);
        out.writeLong(putCount);
        out.writeLong(removeCount);
        out.writeLong(numberOfOtherOperations);
        out.writeLong(numberOfEvents);
        out.writeLong(lastAccessTime);
        out.writeLong(lastUpdateTime);
        out.writeLong(hits);
        out.writeLong(ownedEntryCount.get());
        out.writeLong(backupEntryCount.get());
        out.writeInt(backupCount.get());
        out.writeLong(ownedEntryMemoryCost.get());
        out.writeLong(backupEntryMemoryCost.get());
        out.writeLong(creationTime);
        out.writeLong(lockedEntryCount.get());
        out.writeLong(dirtyEntryCount.get());
        out.writeLong(totalGetLatencies);
        out.writeLong(totalPutLatencies);
        out.writeLong(totalRemoveLatencies);
        out.writeLong(maxGetLatency);
        out.writeLong(maxPutLatency);
        out.writeLong(maxRemoveLatency);
        out.writeLong(heapCost.get());
        boolean hasNearCache = nearCacheStats != null;
        out.writeBoolean(hasNearCache);
        if (hasNearCache) {
            nearCacheStats.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        GET_COUNT_UPDATER.set(this, in.readLong());
        PUT_COUNT_UPDATER.set(this, in.readLong());
        REMOVE_COUNT_UPDATER.set(this, in.readLong());
        NUMBER_OF_OTHER_OPERATIONS_UPDATER.set(this, in.readLong());
        NUMBER_OF_EVENTS_UPDATER.set(this, in.readLong());
        LAST_ACCESS_TIME_UPDATER.set(this, in.readLong());
        LAST_UPDATE_TIME_UPDATER.set(this, in.readLong());
        HITS_UPDATER.set(this, in.readLong());
        ownedEntryCount.set(in.readLong());
        backupEntryCount.set(in.readLong());
        backupCount.set(in.readInt());
        ownedEntryMemoryCost.set(in.readLong());
        backupEntryMemoryCost.set(in.readLong());
        creationTime = in.readLong();
        lockedEntryCount.set(in.readLong());
        dirtyEntryCount.set(in.readLong());
        TOTAL_GET_LATENCIES_UPDATER.set(this, in.readLong());
        TOTAL_PUT_LATENCIES_UPDATER.set(this, in.readLong());
        TOTAL_REMOVE_LATENCIES_UPDATER.set(this, in.readLong());
        MAX_GET_LATENCY_UPDATER.set(this, in.readLong());
        MAX_PUT_LATENCY_UPDATER.set(this, in.readLong());
        MAX_REMOVE_LATENCY_UPDATER.set(this, in.readLong());
        heapCost.set(in.readLong());
        boolean hasNearCache = in.readBoolean();
        if (hasNearCache) {
            nearCacheStats = new NearCacheStatsImpl();
            nearCacheStats.readData(in);
        }
    }

    @Override
    public long getOwnedEntryCount() {
        return ownedEntryCount.get();
    }

    public void setOwnedEntryCount(long ownedEntryCount) {
        this.ownedEntryCount.set(ownedEntryCount);
    }

    public void incrementOwnedEntryCount(long ownedEntryCount) {
        this.ownedEntryCount.addAndGet(ownedEntryCount);
    }

    @Override
    public long getBackupEntryCount() {
        return backupEntryCount.get();
    }

    public void setBackupEntryCount(long backupEntryCount) {
        this.backupEntryCount.set(backupEntryCount);
    }

    public void incrementBackupEntryCount(long backupEntryCount) {
        this.backupEntryCount.addAndGet(backupEntryCount);
    }

    @Override
    public int getBackupCount() {
        return backupCount.get();
    }

    public void setBackupCount(int backupCount) {
        this.backupCount.set(backupCount);
    }

    @Override
    public long getOwnedEntryMemoryCost() {
        return ownedEntryMemoryCost.get();
    }

    public void incrementOwnedEntryMemoryCost(long ownedEntryMemoryCost) {
        this.ownedEntryMemoryCost.addAndGet(ownedEntryMemoryCost);
    }

    @Override
    public long getBackupEntryMemoryCost() {
        return backupEntryMemoryCost.get();
    }

    public void incrementBackupEntryMemoryCost(long backupEntryMemoryCost) {
        this.backupEntryMemoryCost.addAndGet(backupEntryMemoryCost);
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
        LAST_ACCESS_TIME_UPDATER.set(this, Math.max(this.lastAccessTime, lastAccessTime));
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        LAST_UPDATE_TIME_UPDATER.set(this, Math.max(this.lastUpdateTime, lastUpdateTime));
    }

    @Override
    public long getHits() {
        return hits;
    }

    public void setHits(long hits) {
        HITS_UPDATER.set(this, hits);
    }

    public void incrementHits(long hits) {
        HITS_UPDATER.addAndGet(this, hits);
    }

    @Override
    public long getLockedEntryCount() {
        return lockedEntryCount.get();
    }

    public void setLockedEntryCount(long lockedEntryCount) {
        this.lockedEntryCount.set(lockedEntryCount);
    }

    public void incrementLockedEntryCount(long lockedEntryCount) {
        this.lockedEntryCount.addAndGet(lockedEntryCount);
    }

    @Override
    public long getDirtyEntryCount() {
        return dirtyEntryCount.get();
    }

    public void incrementDirtyEntryCount(long dirtyEntryCount) {
        this.dirtyEntryCount.addAndGet(dirtyEntryCount);
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

    public void incrementHeapCost(long heapCost) {
        this.heapCost.addAndGet(heapCost);
    }

    @Override
    public long getHeapCost() {
        return heapCost.get();
    }

    @Override
    public NearCacheStatsImpl getNearCacheStats() {
        return nearCacheStats;
    }

    public void setNearCacheStats(NearCacheStatsImpl nearCacheStats) {
        this.nearCacheStats = nearCacheStats;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.MAP_STATS;
    }

    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("getCount", getCount);
        root.add("putCount", putCount);
        root.add("removeCount", removeCount);
        root.add("numberOfOtherOperations", numberOfOtherOperations);
        root.add("numberOfEvents", numberOfEvents);
        root.add("lastAccessTime", lastAccessTime);
        root.add("lastUpdateTime", lastUpdateTime);
        root.add("hits", hits);
        root.add("ownedEntryCount", ownedEntryCount.get());
        root.add("backupEntryCount", backupEntryCount.get());
        root.add("backupCount", backupCount.get());
        root.add("ownedEntryMemoryCost", ownedEntryMemoryCost.get());
        root.add("backupEntryMemoryCost", backupEntryMemoryCost.get());
        root.add("creationTime", creationTime);
        root.add("lockedEntryCount", lockedEntryCount.get());
        root.add("dirtyEntryCount", dirtyEntryCount.get());
        root.add("totalGetLatencies", totalGetLatencies);
        root.add("totalPutLatencies", totalPutLatencies);
        root.add("totalRemoveLatencies", totalRemoveLatencies);
        root.add("maxGetLatency", maxGetLatency);
        root.add("maxPutLatency", maxPutLatency);
        root.add("maxRemoveLatency", maxRemoveLatency);
        root.add("heapCost", heapCost.get());
        if (nearCacheStats != null) {
            root.add("nearCacheStats", nearCacheStats.toJson());
        }
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        GET_COUNT_UPDATER.set(this, getLong(json, "getCount", -1L));
        PUT_COUNT_UPDATER.set(this, getLong(json, "putCount", -1L));
        REMOVE_COUNT_UPDATER.set(this, getLong(json, "removeCount", -1L));
        NUMBER_OF_OTHER_OPERATIONS_UPDATER.set(this, getLong(json, "numberOfOtherOperations", -1L));
        NUMBER_OF_EVENTS_UPDATER.set(this, getLong(json, "numberOfEvents", -1L));
        LAST_ACCESS_TIME_UPDATER.set(this, getLong(json, "lastAccessTime", -1L));
        LAST_UPDATE_TIME_UPDATER.set(this, getLong(json, "lastUpdateTime", -1L));
        HITS_UPDATER.set(this, getLong(json, "hits", -1L));
        ownedEntryCount.set(getLong(json, "ownedEntryCount", -1L));
        backupEntryCount.set(getLong(json, "backupEntryCount", -1L));
        backupCount.set(getInt(json, "backupCount", -1));
        ownedEntryMemoryCost.set(getLong(json, "ownedEntryMemoryCost", -1L));
        backupEntryMemoryCost.set(getLong(json, "backupEntryMemoryCost", -1L));
        creationTime = getLong(json, "creationTime", -1L);
        lockedEntryCount.set(getLong(json, "lockedEntryCount", -1L));
        dirtyEntryCount.set(getLong(json, "dirtyEntryCount", -1L));
        TOTAL_GET_LATENCIES_UPDATER.set(this, getLong(json, "totalGetLatencies", -1L));
        TOTAL_PUT_LATENCIES_UPDATER.set(this, getLong(json, "totalPutLatencies", -1L));
        TOTAL_REMOVE_LATENCIES_UPDATER.set(this, getLong(json, "totalRemoveLatencies", -1L));
        MAX_GET_LATENCY_UPDATER.set(this, getLong(json, "maxGetLatency", -1L));
        MAX_PUT_LATENCY_UPDATER.set(this, getLong(json, "maxPutLatency", -1L));
        MAX_REMOVE_LATENCY_UPDATER.set(this, getLong(json, "maxRemoveLatency", -1L));
        heapCost.set(getLong(json, "heapCost", -1L));
        final JsonValue jsonNearCacheStats = json.get("nearCacheStats");
        if (jsonNearCacheStats != null) {
            nearCacheStats = new NearCacheStatsImpl();
            nearCacheStats.fromJson(jsonNearCacheStats.asObject());
        }
    }

    @Override
    public String toString() {
        return "LocalMapStatsImpl{"
                + "lastAccessTime=" + lastAccessTime
                + ", lastUpdateTime=" + lastUpdateTime
                + ", hits=" + hits
                + ", numberOfOtherOperations=" + numberOfOtherOperations
                + ", numberOfEvents=" + numberOfEvents
                + ", getCount=" + getCount
                + ", putCount=" + putCount
                + ", removeCount=" + removeCount
                + ", totalGetLatencies=" + totalGetLatencies
                + ", totalPutLatencies=" + totalPutLatencies
                + ", totalRemoveLatencies=" + totalRemoveLatencies
                + ", ownedEntryCount=" + ownedEntryCount
                + ", backupEntryCount=" + backupEntryCount
                + ", backupCount=" + backupCount
                + ", ownedEntryMemoryCost=" + ownedEntryMemoryCost
                + ", backupEntryMemoryCost=" + backupEntryMemoryCost
                + ", creationTime=" + creationTime
                + ", lockedEntryCount=" + lockedEntryCount
                + ", dirtyEntryCount=" + dirtyEntryCount
                + ", heapCost=" + heapCost
                + '}';
    }
}
