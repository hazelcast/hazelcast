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
import java.util.concurrent.atomic.AtomicLong;

public class LocalMapStatsImpl implements LocalMapStats, IdentifiedDataSerializable {
    private final AtomicLong lastAccessTime = new AtomicLong(0);
    private final AtomicLong lastUpdateTime = new AtomicLong(0);
    private final AtomicLong hits = new AtomicLong(0);
    private final AtomicLong numberOfOtherOperations = new AtomicLong(0);
    private final AtomicLong numberOfEvents = new AtomicLong(0);
    private final AtomicLong getCount = new AtomicLong(0);
    private final AtomicLong putCount = new AtomicLong(0);
    private final AtomicLong removeCount = new AtomicLong(0);
    private final AtomicLong totalGetLatencies = new AtomicLong(0);
    private final AtomicLong totalPutLatencies = new AtomicLong(0);
    private final AtomicLong totalRemoveLatencies = new AtomicLong(0);
    private final AtomicLong maxGetLatency = new AtomicLong(0);
    private final AtomicLong maxPutLatency = new AtomicLong(0);
    private final AtomicLong maxRemoveLatency = new AtomicLong(0);
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
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(getCount.get());
        out.writeLong(putCount.get());
        out.writeLong(removeCount.get());
        out.writeLong(numberOfOtherOperations.get());
        out.writeLong(numberOfEvents.get());
        out.writeLong(lastAccessTime.get());
        out.writeLong(lastUpdateTime.get());
        out.writeLong(hits.get());
        out.writeLong(ownedEntryCount);
        out.writeLong(backupEntryCount);
        out.writeInt(backupCount);
        out.writeLong(ownedEntryMemoryCost);
        out.writeLong(backupEntryMemoryCost);
        out.writeLong(creationTime);
        out.writeLong(lockedEntryCount);
        out.writeLong(dirtyEntryCount);
        out.writeLong(totalGetLatencies.get());
        out.writeLong(totalPutLatencies.get());
        out.writeLong(totalRemoveLatencies.get());
        out.writeLong(maxGetLatency.get());
        out.writeLong(maxPutLatency.get());
        out.writeLong(maxRemoveLatency.get());
        out.writeLong(heapCost);
        boolean hasNearCache = nearCacheStats != null;
        out.writeBoolean(hasNearCache);
        if (hasNearCache) {
            nearCacheStats.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        getCount.set(in.readLong());
        putCount.set(in.readLong());
        removeCount.set(in.readLong());
        numberOfOtherOperations.set(in.readLong());
        numberOfEvents.set(in.readLong());
        lastAccessTime.set(in.readLong());
        lastUpdateTime.set(in.readLong());
        hits.set(in.readLong());
        ownedEntryCount = in.readLong();
        backupEntryCount = in.readLong();
        backupCount = in.readInt();
        ownedEntryMemoryCost = in.readLong();
        backupEntryMemoryCost = in.readLong();
        creationTime = in.readLong();
        lockedEntryCount = in.readLong();
        dirtyEntryCount = in.readLong();
        totalGetLatencies.set(in.readLong());
        totalPutLatencies.set(in.readLong());
        totalRemoveLatencies.set(in.readLong());
        maxGetLatency.set(in.readLong());
        maxPutLatency.set(in.readLong());
        maxRemoveLatency.set(in.readLong());
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
        return lastAccessTime.get();
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime.set(Math.max(this.lastAccessTime.get(), lastAccessTime));
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime.get();
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime.set(Math.max(this.lastUpdateTime.get(), lastUpdateTime));
    }

    @Override
    public long getHits() {
        return hits.get();
    }

    public void setHits(long hits) {
        this.hits.set(hits);
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
        return putCount.get() + getCount.get() + removeCount.get() + numberOfOtherOperations.get();
    }

    @Override
    public long getPutOperationCount() {
        return putCount.get();
    }

    public void incrementPuts(long latency) {
        putCount.incrementAndGet();
        totalPutLatencies.addAndGet(latency);
        maxPutLatency.set(Math.max(maxPutLatency.get(), latency));
    }

    @Override
    public long getGetOperationCount() {
        return getCount.get();
    }

    public void incrementGets(long latency) {
        getCount.incrementAndGet();
        totalGetLatencies.addAndGet(latency);
        maxGetLatency.set(Math.max(maxGetLatency.get(), latency));
    }

    @Override
    public long getRemoveOperationCount() {
        return removeCount.get();
    }

    public void incrementRemoves(long latency) {
        removeCount.incrementAndGet();
        totalRemoveLatencies.addAndGet(latency);
        maxRemoveLatency.set(Math.max(maxRemoveLatency.get(), latency));
    }

    @Override
    public long getTotalPutLatency() {
        return totalPutLatencies.get();
    }

    @Override
    public long getTotalGetLatency() {
        return totalGetLatencies.get();
    }

    @Override
    public long getTotalRemoveLatency() {
        return totalRemoveLatencies.get();
    }

    @Override
    public long getMaxPutLatency() {
        return maxPutLatency.get();
    }

    @Override
    public long getMaxGetLatency() {
        return maxGetLatency.get();
    }

    @Override
    public long getMaxRemoveLatency() {
        return maxRemoveLatency.get();
    }

    @Override
    public long getOtherOperationCount() {
        return numberOfOtherOperations.get();
    }

    public void incrementOtherOperations() {
        numberOfOtherOperations.incrementAndGet();
    }

    @Override
    public long getEventOperationCount() {
        return numberOfEvents.get();
    }

    public void incrementReceivedEvents() {
        numberOfEvents.incrementAndGet();
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
