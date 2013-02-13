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

import com.hazelcast.monitor.LocalMapOperationStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class LocalMapStatsImpl extends LocalInstanceStatsSupport<LocalMapOperationStats> implements LocalMapStats {
    private final AtomicLong lastAccessTime = new AtomicLong();
    private final AtomicLong hits = new AtomicLong();
    private long ownedEntryCount;
    private long backupEntryCount;
    private long ownedEntryMemoryCost;
    private long backupEntryMemoryCost;
    private long creationTime;
    private long lastUpdateTime;
    private long lockedEntryCount;
    private long dirtyEntryCount;

    enum Op {
        CREATE,
        READ,
        UPDATE,
        REMOVE,
        LOCK,
        UNLOCK,
        ADD_LOCK_WAIT,
        REMOVE_LOCK_WAIT
    }

    public LocalMapStatsImpl() {
    }

    void writeDataInternal(ObjectDataOutput out) throws IOException {
        out.writeLong(lastAccessTime.get());
        out.writeLong(hits.get());
        out.writeLong(ownedEntryCount);
        out.writeLong(backupEntryCount);
        out.writeLong(ownedEntryMemoryCost);
        out.writeLong(backupEntryMemoryCost);
        out.writeLong(creationTime);
        out.writeLong(lastUpdateTime);
        out.writeLong(lockedEntryCount);
        out.writeLong(dirtyEntryCount);
    }

    void readDataInternal(ObjectDataInput in) throws IOException {
        lastAccessTime.set(in.readLong());
        hits.set(in.readLong());
        ownedEntryCount = in.readLong();
        backupEntryCount = in.readLong();
        ownedEntryMemoryCost = in.readLong();
        backupEntryMemoryCost = in.readLong();
        creationTime = in.readLong();
        lastUpdateTime = in.readLong();
        lockedEntryCount = in.readLong();
        dirtyEntryCount = in.readLong();
    }

    @Override
    LocalMapOperationStats newOperationStatsInstance() {
        return new LocalMapOperationStatsImpl();
    }

    public long getOwnedEntryCount() {
        return ownedEntryCount;
    }

    public void setOwnedEntryCount(long ownedEntryCount) {
        this.ownedEntryCount = ownedEntryCount;
    }

    public long getBackupEntryCount() {
        return backupEntryCount;
    }

    public void setBackupEntryCount(long backupEntryCount) {
        this.backupEntryCount = backupEntryCount;
    }

    public long getOwnedEntryMemoryCost() {
        return ownedEntryMemoryCost;
    }

    public void setOwnedEntryMemoryCost(long ownedEntryMemoryCost) {
        this.ownedEntryMemoryCost = ownedEntryMemoryCost;
    }

    public long getBackupEntryMemoryCost() {
        return backupEntryMemoryCost;
    }

    public void setBackupEntryMemoryCost(long backupEntryMemoryCost) {
        this.backupEntryMemoryCost = backupEntryMemoryCost;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime.get();
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime.set(Math.max(this.lastAccessTime.get(), lastAccessTime));
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = Math.max(this.lastUpdateTime, lastUpdateTime);
    }

    public long getHits() {
        return hits.get();
    }

    public void setHits(long hits) {
        this.hits.set(hits);
    }

    public long getLockedEntryCount() {
        return lockedEntryCount;
    }

    public void setLockedEntryCount(long lockedEntryCount) {
        this.lockedEntryCount = lockedEntryCount;
    }

    public long getDirtyEntryCount() {
        return dirtyEntryCount;
    }

    public void setDirtyEntryCount(long l) {
        this.dirtyEntryCount = l;
    }

    @Override
    public String toString() {
        return "LocalMapStatsImpl{" +
                "ownedEntryCount=" + ownedEntryCount +
                ", backupEntryCount=" + backupEntryCount +
                ", ownedEntryMemoryCost=" + ownedEntryMemoryCost +
                ", backupEntryMemoryCost=" + backupEntryMemoryCost +
                ", creationTime=" + creationTime +
                ", lastAccessTime=" + lastAccessTime.get() +
                ", lastUpdateTime=" + lastUpdateTime +
                ", hits=" + hits.get() +
                ", lockedEntryCount=" + lockedEntryCount +
                ", dirtyEntryCount=" + dirtyEntryCount +
                ", " + operationStats +
                '}';
    }
}
