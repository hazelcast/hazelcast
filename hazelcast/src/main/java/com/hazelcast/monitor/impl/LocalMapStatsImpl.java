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

import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class LocalMapStatsImpl  implements LocalMapStats {
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

    public LocalMapStatsImpl() {
    }

    public void writeData(ObjectDataOutput out) throws IOException {
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

    public void readData(ObjectDataInput in) throws IOException {
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
                '}';
    }
//
//    OperationStat gets = new OperationStat(0, 0);
//    OperationStat puts = new OperationStat(0, 0);
//    OperationStat removes = new OperationStat(0, 0);
//    long numberOfOtherOperations;
//    long numberOfEvents;
//
//    public void writeData(ObjectDataOutput out) throws IOException {
//        puts.writeData(out);
//        gets.writeData(out);
//        removes.writeData(out);
//        out.writeLong(numberOfOtherOperations);
//        out.writeLong(numberOfEvents);
//    }
//
//    public public void readData(ObjectDataInput in) throws IOException {
//        puts = new OperationStat();
//        puts.readData(in);
//        gets = new OperationStat();
//        gets.readData(in);
//        removes = new OperationStat();
//        removes.readData(in);
//        numberOfOtherOperations = in.readLong();
//        numberOfEvents = in.readLong();
//    }
//
//    public long total() {
//        return puts.count + gets.count + removes.count + numberOfOtherOperations;
//    }
//
//    public long getNumberOfPuts() {
//        return puts.count;
//    }
//
//    public long getNumberOfGets() {
//        return gets.count;
//    }
//
//    public long getTotalPutLatency() {
//        return puts.totalLatency;
//    }
//
//    public long getTotalGetLatency() {
//        return gets.totalLatency;
//    }
//
//    public long getTotalRemoveLatency() {
//        return removes.totalLatency;
//    }
//
//    public long getNumberOfRemoves() {
//        return removes.count;
//    }
//
//    public long getNumberOfOtherOperations() {
//        return numberOfOtherOperations;
//    }
//
//    public long getNumberOfEvents() {
//        return numberOfEvents;
//    }
//
//    public String toString() {
//        return "LocalMapOperationStats{" +
//                "total= " + total() +
//                "\n, puts:" + puts +
//                "\n, gets:" + gets +
//                "\n, removes:" + removes +
//                "\n, others: " + numberOfOtherOperations +
//                "\n, received events: " + numberOfEvents +
//                "}";
//    }


    @Override
    public long getNumberOfPuts() {
        return 0;
    }

    @Override
    public long getNumberOfGets() {
        return 0;
    }

    @Override
    public long getTotalPutLatency() {
        return 0;
    }

    @Override
    public long getTotalGetLatency() {
        return 0;
    }

    @Override
    public long getTotalRemoveLatency() {
        return 0;
    }

    @Override
    public long getNumberOfRemoves() {
        return 0;
    }

    @Override
    public long getNumberOfEvents() {
        return 0;
    }

    @Override
    public long getNumberOfOtherOperations() {
        return 0;
    }

    @Override
    public long total() {
        return 0;
    }

    @Override
    public long getPeriodEnd() {
        return 0;
    }

    @Override
    public long getPeriodStart() {
        return 0;
    }
}
