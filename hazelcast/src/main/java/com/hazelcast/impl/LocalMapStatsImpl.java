/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.monitor.LocalMapOperationStats;
import com.hazelcast.monitor.LocalMapStats;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LocalMapStatsImpl implements LocalMapStats, Serializable {
    private final AtomicLong lastAccessTime = new AtomicLong();
    private final AtomicInteger hits = new AtomicInteger();
    private int ownedEntryCount;
    private int backupEntryCount;
    private int markedAsRemovedEntryCount;
    private int ownedEntryMemoryCost;
    private int backupEntryMemoryCost;
    private int markedAsRemovedMemoryCost;
    private long creationTime;
    private long lastUpdateTime;
    private long lastEvictionTime;
    private int lockedEntryCount;
    private int lockWaitCount;
    private LocalMapOperationStats operationStats;

    public void incrementHit() {
        hits.incrementAndGet();
        lastAccessTime.set(System.currentTimeMillis());
    }

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

    public void update(Op op, Record record, boolean owned, long updateCost) {
        if (owned) {
            if (op == Op.READ) {
                hits.incrementAndGet();
            } else if (op == Op.UPDATE) {
                lastUpdateTime = System.currentTimeMillis();
                ownedEntryMemoryCost += updateCost;
            } else if (op == Op.CREATE) {
                ownedEntryCount++;
                ownedEntryMemoryCost += record.getCost();
            } else if (op == Op.REMOVE) {
                ownedEntryCount--;
                ownedEntryMemoryCost -= record.getCost();
            } else if (op == Op.LOCK) {
                lockedEntryCount++;
            } else if (op == Op.UNLOCK) {
                lockedEntryCount--;
            } else if (op == Op.ADD_LOCK_WAIT) {
                lockWaitCount++;
            } else if (op == Op.REMOVE_LOCK_WAIT) {
                lockWaitCount--;
            }
        } else {
            if (op == Op.CREATE) {
                backupEntryCount++;
                backupEntryMemoryCost += record.getCost();
            } else if (op == Op.UPDATE) {
                backupEntryMemoryCost += updateCost;
            } else if (op == Op.REMOVE) {
                backupEntryCount--;
                backupEntryMemoryCost -= record.getCost();
            }
        }
    }

    public int getOwnedEntryCount() {
        return ownedEntryCount;
    }

    public void setOwnedEntryCount(int ownedEntryCount) {
        this.ownedEntryCount = ownedEntryCount;
    }

    public int getBackupEntryCount() {
        return backupEntryCount;
    }

    public void setBackupEntryCount(int backupEntryCount) {
        this.backupEntryCount = backupEntryCount;
    }

    public int getMarkedAsRemovedEntryCount() {
        return markedAsRemovedEntryCount;
    }

    public void setMarkedAsRemovedEntryCount(int markedAsRemovedEntryCount) {
        this.markedAsRemovedEntryCount = markedAsRemovedEntryCount;
    }

    public int getOwnedEntryMemoryCost() {
        return ownedEntryMemoryCost;
    }

    public void setOwnedEntryMemoryCost(int ownedEntryMemoryCost) {
        this.ownedEntryMemoryCost = ownedEntryMemoryCost;
    }

    public int getBackupEntryMemoryCost() {
        return backupEntryMemoryCost;
    }

    public void setBackupEntryMemoryCost(int backupEntryMemoryCost) {
        this.backupEntryMemoryCost = backupEntryMemoryCost;
    }

    public int getMarkedAsRemovedMemoryCost() {
        return markedAsRemovedMemoryCost;
    }

    public void setMarkedAsRemovedMemoryCost(int markedAsRemovedMemoryCost) {
        this.markedAsRemovedMemoryCost = markedAsRemovedMemoryCost;
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

    public long getLastEvictionTime() {
        return lastEvictionTime;
    }

    public void setLastEvictionTime(long lastEvictionTime) {
        this.lastEvictionTime = lastEvictionTime;
    }

    public int getHits() {
        return hits.get();
    }

    public void setHits(int hits) {
        this.hits.set(hits);
    }

    public int getLockedEntryCount() {
        return lockedEntryCount;
    }

    public void setLockedEntryCount(int lockedEntryCount) {
        this.lockedEntryCount = lockedEntryCount;
    }

    public int getLockWaitCount() {
        return lockWaitCount;
    }

    public void setLockWaitCount(int lockWaitCount) {
        this.lockWaitCount = lockWaitCount;
    }

    public LocalMapOperationStats getOperationStats() {
        return operationStats;
    }

    public void setOperationStats(LocalMapOperationStats operationStats) {
        this.operationStats = operationStats;
    }

    @Override
    public String toString() {
        return "LocalMapStatsImpl{" +
                "ownedEntryCount=" + ownedEntryCount +
                ", backupEntryCount=" + backupEntryCount +
                ", markedAsRemovedEntryCount=" + markedAsRemovedEntryCount +
                ", ownedEntryMemoryCost=" + ownedEntryMemoryCost +
                ", backupEntryMemoryCost=" + backupEntryMemoryCost +
                ", markedAsRemovedMemoryCost=" + markedAsRemovedMemoryCost +
                ", creationTime=" + creationTime +
                ", lastAccessTime=" + lastAccessTime.get() +
                ", lastUpdateTime=" + lastUpdateTime +
                ", lastEvictionTime=" + lastEvictionTime +
                ", hits=" + hits.get() +
                ", lockedEntryCount=" + lockedEntryCount +
                ", lockWaitCount=" + lockWaitCount +
                ", " + operationStats +
                '}';
    }
}
