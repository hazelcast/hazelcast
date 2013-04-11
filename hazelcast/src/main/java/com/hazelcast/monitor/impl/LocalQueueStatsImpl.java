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

import com.hazelcast.monitor.LocalQueueStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class LocalQueueStatsImpl implements LocalQueueStats {

    private int ownedItemCount;
    private int backupItemCount;
    private long minAge;
    private long maxAge;
    private long aveAge;
    private long creationTime;
    private AtomicLong numberOfOffers = new AtomicLong(0);
    private AtomicLong numberOfRejectedOffers = new AtomicLong(0);
    private AtomicLong numberOfPolls = new AtomicLong(0);
    private AtomicLong numberOfEmptyPolls = new AtomicLong(0);
    private AtomicLong numberOfOtherOperations = new AtomicLong(0);
    private AtomicLong numberOfEvents = new AtomicLong(0);

    public LocalQueueStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(ownedItemCount);
        out.writeInt(backupItemCount);
        out.writeLong(minAge);
        out.writeLong(maxAge);
        out.writeLong(aveAge);
        out.writeLong(creationTime);
        out.writeLong(numberOfOffers.get());
        out.writeLong(numberOfPolls.get());
        out.writeLong(numberOfRejectedOffers.get());
        out.writeLong(numberOfEmptyPolls.get());
        out.writeLong(numberOfOtherOperations.get());
        out.writeLong(numberOfEvents.get());
    }

    public void readData(ObjectDataInput in) throws IOException {
        ownedItemCount = in.readInt();
        backupItemCount = in.readInt();
        minAge = in.readLong();
        maxAge = in.readLong();
        aveAge = in.readLong();
        creationTime = in.readLong();
        numberOfOffers.set(in.readLong());
        numberOfPolls.set(in.readLong());
        numberOfRejectedOffers.set(in.readLong());
        numberOfEmptyPolls.set(in.readLong());
        numberOfOtherOperations.set(in.readLong());
        numberOfEvents.set(in.readLong());
    }

    public long getMinAge() {
        return minAge;
    }

    public void setMinAge(long minAge) {
        this.minAge = minAge;
    }

    public long getMaxAge() {
        return maxAge;
    }

    public void setMaxAge(long maxAge) {
        this.maxAge = maxAge;
    }

    public long getAvgAge() {
        return aveAge;
    }

    public void setAveAge(long aveAge) {
        this.aveAge = aveAge;
    }

    public long getOwnedItemCount() {
        return ownedItemCount;
    }

    public void setOwnedItemCount(int ownedItemCount) {
        this.ownedItemCount = ownedItemCount;
    }

    public long getBackupItemCount() {
        return backupItemCount;
    }

    public void setBackupItemCount(int backupItemCount) {
        this.backupItemCount = backupItemCount;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long total() {
        return numberOfOffers.get() + numberOfPolls.get() + numberOfOtherOperations.get();
    }

    public long getOfferOperationCount() {
        return numberOfOffers.get();
    }

    public long getRejectedOfferOperationCount() {
        return numberOfRejectedOffers.get();
    }

    public long getPollOperationCount() {
        return numberOfPolls.get();
    }

    public long getEmptyPollOperationCount() {
        return numberOfEmptyPolls.get();
    }

    public long getOtherOperationsCount() {
        return numberOfOtherOperations.get();
    }

    public void incrementOtherOperations() {
        numberOfOtherOperations.incrementAndGet();
    }

    public void incrementOffers() {
        numberOfOffers.incrementAndGet();
    }

    public void incrementRejectedOffers() {
        numberOfRejectedOffers.incrementAndGet();
    }

    public void incrementPolls() {
        numberOfPolls.incrementAndGet();
    }

    public void incrementEmptyPolls() {
        numberOfEmptyPolls.incrementAndGet();
    }

    public void incrementReceivedEvents() {
        numberOfEvents.incrementAndGet();
    }

    public long getEventOperationCount() {
        return numberOfEvents.get();
    }


}
