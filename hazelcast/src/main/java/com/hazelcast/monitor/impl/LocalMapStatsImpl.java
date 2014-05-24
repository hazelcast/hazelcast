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

import com.hazelcast.map.MapDataSerializerHook;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

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

    private volatile long lastAccessTime = 0L;
    private volatile long lastUpdateTime = 0L;
    private volatile long hits = 0L;
    private volatile long numberOfOtherOperations = 0L;
    private volatile long numberOfEvents = 0L;
    private volatile long getCount = 0L;
    private volatile long putCount = 0L;
    private volatile long removeCount = 0L;
    private volatile long totalGetLatencies = 0L;
    private volatile long totalPutLatencies = 0L;
    private volatile long totalRemoveLatencies = 0L;
    private volatile long maxGetLatency = 0L;
    private volatile long maxPutLatency = 0L;
    private volatile long maxRemoveLatency = 0L;
    private long ownedEntryCount;
    private long backupEntryCount;
    private long ownedEntryMemoryCost;
    private long backupEntryMemoryCost;
    // total heap cost with map &  nearcache  & backup
    private long heapCost;
    private long creationTime;
    private long lockedEntryCount;
    private long dirtyEntryCount;
    private int backupCount;

    private NearCacheStatsImpl nearCacheStats;

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
        out.writeLong(ownedEntryCount);
        out.writeLong(backupEntryCount);
        out.writeInt(backupCount);
        out.writeLong(ownedEntryMemoryCost);
        out.writeLong(backupEntryMemoryCost);
        out.writeLong(creationTime);
        out.writeLong(lockedEntryCount);
        out.writeLong(dirtyEntryCount);
        out.writeLong(totalGetLatencies);
        out.writeLong(totalPutLatencies);
        out.writeLong(totalRemoveLatencies);
        out.writeLong(maxGetLatency);
        out.writeLong(maxPutLatency);
        out.writeLong(maxRemoveLatency);
        out.writeLong(heapCost);
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
        ownedEntryCount = in.readLong();
        backupEntryCount = in.readLong();
        backupCount = in.readInt();
        ownedEntryMemoryCost = in.readLong();
        backupEntryMemoryCost = in.readLong();
        creationTime = in.readLong();
        lockedEntryCount = in.readLong();
        dirtyEntryCount = in.readLong();
        TOTAL_GET_LATENCIES_UPDATER.set(this, in.readLong());
        TOTAL_PUT_LATENCIES_UPDATER.set(this, in.readLong());
        TOTAL_REMOVE_LATENCIES_UPDATER.set(this, in.readLong());
        MAX_GET_LATENCY_UPDATER.set(this, in.readLong());
        MAX_PUT_LATENCY_UPDATER.set(this, in.readLong());
        MAX_REMOVE_LATENCY_UPDATER.set(this, in.readLong());
        heapCost = in.readLong();
        boolean hasNearCache = in.readBoolean();
        if (hasNearCache) {
            nearCacheStats = new NearCacheStatsImpl();
            nearCacheStats.readData(in);
        }
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
        return backupEntryCount;
    }

    public void setBackupEntryCount(long backupEntryCount) {
        this.backupEntryCount = backupEntryCount;
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

    public void setOwnedEntryMemoryCost(long ownedEntryMemoryCost) {
        this.ownedEntryMemoryCost = ownedEntryMemoryCost;
    }

    @Override
    public long getBackupEntryMemoryCost() {
        return backupEntryMemoryCost;
    }

    public void setBackupEntryMemoryCost(long backupEntryMemoryCost) {
        this.backupEntryMemoryCost = backupEntryMemoryCost;
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

    @Override
    public long getLockedEntryCount() {
        return lockedEntryCount;
    }

    public void setLockedEntryCount(long lockedEntryCount) {
        this.lockedEntryCount = lockedEntryCount;
    }

    @Override
    public long getDirtyEntryCount() {
        return dirtyEntryCount;
    }

    public void setDirtyEntryCount(long l) {
        this.dirtyEntryCount = l;
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

    public void setHeapCost(long heapCost) {
        this.heapCost = heapCost;
    }

    @Override
    public long getHeapCost() {
        return heapCost;
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

    @Override
    public String toString() {
        return "LocalMapStatsImpl{" + "lastAccessTime=" + lastAccessTime + ", lastUpdateTime=" + lastUpdateTime + ", hits=" + hits
                + ", numberOfOtherOperations=" + numberOfOtherOperations + ", numberOfEvents=" + numberOfEvents + ", getCount="
                + getCount + ", putCount=" + putCount + ", removeCount=" + removeCount + ", totalGetLatencies="
                + totalGetLatencies + ", totalPutLatencies=" + totalPutLatencies + ", totalRemoveLatencies="
                + totalRemoveLatencies + ", ownedEntryCount=" + ownedEntryCount + ", backupEntryCount=" + backupEntryCount
                + ", backupCount=" + backupCount + ", ownedEntryMemoryCost=" + ownedEntryMemoryCost + ", backupEntryMemoryCost="
                + backupEntryMemoryCost + ", creationTime=" + creationTime + ", lockedEntryCount=" + lockedEntryCount
                + ", dirtyEntryCount=" + dirtyEntryCount + ", heapCost=" + heapCost + '}';
    }
}
