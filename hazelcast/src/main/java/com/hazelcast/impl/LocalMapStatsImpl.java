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

import com.hazelcast.monitor.LocalMapStats;

import java.io.Serializable;

public class LocalMapStatsImpl implements LocalMapStats, Serializable {
    private int ownedEntryCount;
    private int backupEntryCount;
    private int markedAsRemovedEntryCount;
    private int ownedEntryMemoryCost;
    private int backupEntryMemoryCost;
    private int markedAsRemovedMemoryCost;
    private long creationTime;
    private long lastAccessTime;
    private long lastUpdateTime;
    private long lastEvictionTime;
    private int hits;
    private int lockedEntryCount;
    private int lockWaitCount;

    public LocalMapStatsImpl() {
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
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = Math.max(this.lastAccessTime, lastAccessTime);
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
        return hits;
    }

    public void setHits(int hits) {
        this.hits = hits;
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
                ", lastAccessTime=" + lastAccessTime +
                ", lastUpdateTime=" + lastUpdateTime +
                ", lastEvictionTime=" + lastEvictionTime +
                ", hits=" + hits +
                ", lockedEntryCount=" + lockedEntryCount +
                ", lockWaitCount=" + lockWaitCount +
                '}';
    }
}
