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

import com.hazelcast.monitor.LocalReplicatedMapStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.operation.ReplicatedMapDataSerializerHook;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class LocalReplicatedMapStatsImpl implements LocalReplicatedMapStats, IdentifiedDataSerializable {
    private final AtomicLong lastAccessTime = new AtomicLong(0);
    private final AtomicLong lastUpdateTime = new AtomicLong(0);
    private final AtomicLong hits = new AtomicLong(0);
    private final AtomicLong numberOfOtherOperations = new AtomicLong(0);
    private final AtomicLong numberOfEvents = new AtomicLong(0);
    private final AtomicLong numberOfReplicationEvents = new AtomicLong(0);
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

    private long creationTime;

    public LocalReplicatedMapStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

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
        out.writeLong(creationTime);
        out.writeLong(totalGetLatencies.get());
        out.writeLong(totalPutLatencies.get());
        out.writeLong(totalRemoveLatencies.get());
        out.writeLong(maxGetLatency.get());
        out.writeLong(maxPutLatency.get());
        out.writeLong(maxRemoveLatency.get());
    }

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
        creationTime = in.readLong();
        totalGetLatencies.set(in.readLong());
        totalPutLatencies.set(in.readLong());
        totalRemoveLatencies.set(in.readLong());
        maxGetLatency.set(in.readLong());
        maxPutLatency.set(in.readLong());
        maxRemoveLatency.set(in.readLong());
    }

    public long getOwnedEntryCount() {
        return ownedEntryCount;
    }

    public void setOwnedEntryCount(long ownedEntryCount) {
        this.ownedEntryCount = ownedEntryCount;
    }

    public long getBackupEntryCount() {
        return 0;
    }

    public void setBackupEntryCount(long backupEntryCount) {
    }

    public int getBackupCount() {
        return 0;
    }

    public void setBackupCount(int backupCount) {
    }

    public long getOwnedEntryMemoryCost() {
        return 0;
    }

    public void setOwnedEntryMemoryCost(long ownedEntryMemoryCost) {
    }

    public long getBackupEntryMemoryCost() {
        return 0;
    }

    public void setBackupEntryMemoryCost(long backupEntryMemoryCost) {
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime.get();
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime.set(Math.max(this.lastAccessTime.get(), lastAccessTime));
    }

    public long getLastUpdateTime() {
        return lastUpdateTime.get();
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime.set(Math.max(this.lastUpdateTime.get(), lastUpdateTime));
    }

    public long getHits() {
        return hits.get();
    }

    public void setHits(long hits) {
        this.hits.set(hits);
    }

    public long getLockedEntryCount() {
        return 0;
    }

    public void setLockedEntryCount(long lockedEntryCount) {
    }

    public long getDirtyEntryCount() {
        return 0;
    }

    public void setDirtyEntryCount(long l) {
    }

    public long total() {
        return putCount.get() + getCount.get() + removeCount.get() + numberOfOtherOperations.get();
    }

    public long getPutOperationCount() {
        return putCount.get();
    }

    public void incrementPuts(long latency) {
        putCount.incrementAndGet();
        totalPutLatencies.addAndGet(latency);
        maxPutLatency.set(Math.max(maxPutLatency.get(), latency));
    }

    public long getGetOperationCount() {
        return getCount.get();
    }

    public void incrementGets(long latency) {
        getCount.incrementAndGet();
        totalGetLatencies.addAndGet(latency);
        maxGetLatency.set(Math.max(maxGetLatency.get(), latency));
    }

    public long getRemoveOperationCount() {
        return removeCount.get();
    }

    public void incrementRemoves(long latency) {
        removeCount.incrementAndGet();
        totalRemoveLatencies.addAndGet(latency);
        maxRemoveLatency.set(Math.max(maxRemoveLatency.get(), latency));
    }

    public long getTotalPutLatency() {
        return totalPutLatencies.get();
    }

    public long getTotalGetLatency() {
        return totalGetLatencies.get();
    }

    public long getTotalRemoveLatency() {
        return totalRemoveLatencies.get();
    }

    public long getMaxPutLatency() {
        return maxPutLatency.get();
    }

    public long getMaxGetLatency() {
        return maxGetLatency.get();
    }

    public long getMaxRemoveLatency() {
        return maxRemoveLatency.get();
    }

    public long getOtherOperationCount() {
        return numberOfOtherOperations.get();
    }

    public void incrementOtherOperations() {
        numberOfOtherOperations.incrementAndGet();
    }

    public long getEventOperationCount() {
        return numberOfEvents.get();
    }

    public void incrementReceivedEvents() {
        numberOfEvents.incrementAndGet();
    }

    public long getReplicationEventCount() {
        return numberOfReplicationEvents.get();
    }

    public void incrementReceivedReplicationEvents() {
        numberOfReplicationEvents.incrementAndGet();
    }

    public void setHeapCost(long heapCost) {
    }

    public long getHeapCost() {
        return 0;
    }

    public String toString() {
        return "LocalReplicatedMapStatsImpl{" +
                "lastAccessTime=" + lastAccessTime +
                ", lastUpdateTime=" + lastUpdateTime +
                ", hits=" + hits +
                ", numberOfOtherOperations=" + numberOfOtherOperations +
                ", numberOfEvents=" + numberOfEvents +
                ", numberOfReplicationEvents=" + numberOfReplicationEvents +
                ", getCount=" + getCount +
                ", putCount=" + putCount +
                ", removeCount=" + removeCount +
                ", totalGetLatencies=" + totalGetLatencies +
                ", totalPutLatencies=" + totalPutLatencies +
                ", totalRemoveLatencies=" + totalRemoveLatencies +
                ", ownedEntryCount=" + ownedEntryCount +
                ", creationTime=" + creationTime +
                '}';
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.MAP_STATS;
    }
}
