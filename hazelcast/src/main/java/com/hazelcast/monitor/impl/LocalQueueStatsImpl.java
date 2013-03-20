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
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class LocalQueueStatsImpl implements LocalQueueStats {

    private int ownedItemCount;
    private int backupItemCount;
    private long minAge;
    private long maxAge;
    private long aveAge;

    public LocalQueueStatsImpl() {
    }

    public LocalQueueStatsImpl(int ownedItemCount, int backupItemCount, long minAge, long maxAge, long aveAge) {
        this.ownedItemCount = ownedItemCount;
        this.backupItemCount = backupItemCount;
        this.minAge = minAge;
        this.maxAge = maxAge;
        this.aveAge = aveAge;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(ownedItemCount);
        out.writeInt(backupItemCount);
        out.writeLong(minAge);
        out.writeLong(maxAge);
        out.writeLong(aveAge);
    }

    public void readData(ObjectDataInput in) throws IOException {
        ownedItemCount = in.readInt();
        backupItemCount = in.readInt();
        minAge = in.readLong();
        maxAge = in.readLong();
        aveAge = in.readLong();
    }


    public int getOwnedItemCount() {
        return ownedItemCount;
    }

    public void setOwnedItemCount(int ownedItemCount) {
        this.ownedItemCount = ownedItemCount;
    }

    public void setBackupItemCount(int backupItemCount) {
        this.backupItemCount = backupItemCount;
    }

    public void setMinAge(long minAge) {
        this.minAge = minAge;
    }

    public void setMaxAge(long maxAge) {
        this.maxAge = maxAge;
    }

    public void setAveAge(long aveAge) {
        this.aveAge = aveAge;
    }

    public int getBackupItemCount() {
        return backupItemCount;
    }

    public long getMaxAge() {
        return maxAge;
    }

    public long getMinAge() {
        return minAge;
    }

    public long getAveAge() {
        return aveAge;
    }

    @Override
    public String toString() {
        return "LocalQueueStatsImpl{" +
                "aveAge=" + aveAge +
                ", ownedItemCount=" + ownedItemCount +
                ", backupItemCount=" + backupItemCount +
                ", minAge=" + minAge +
                ", maxAge=" + maxAge +
                '}';
    }


//    long numberOfOffers;
//    long numberOfRejectedOffers;
//    long numberOfPolls;
//    long numberOfEmptyPolls;
//    long numberOfOtherOperations;
//    long numberOfEvents;
//
//    public void writeData(ObjectDataOutput out) throws IOException {
//        out.writeLong(numberOfOffers);
//        out.writeLong(numberOfPolls);
//        out.writeLong(numberOfRejectedOffers);
//        out.writeLong(numberOfEmptyPolls);
//        out.writeLong(numberOfOtherOperations);
//        out.writeLong(numberOfEvents);
//    }
//
//    public void readData(ObjectDataInput in) throws IOException {
//        numberOfOffers = in.readLong();
//        numberOfPolls = in.readLong();
//        numberOfRejectedOffers = in.readLong();
//        numberOfEmptyPolls = in.readLong();
//        numberOfOtherOperations = in.readLong();
//        numberOfEvents = in.readLong();
//    }
//
//    public long total() {
//        return numberOfOffers + numberOfPolls + numberOfOtherOperations;
//    }
//
//    public long getNumberOfOffers() {
//        return numberOfOffers;
//    }
//
//    public long getNumberOfRejectedOffers() {
//        return numberOfRejectedOffers;
//    }
//
//    public long getNumberOfPolls() {
//        return numberOfPolls;
//    }
//
//    public long getNumberOfEmptyPolls() {
//        return numberOfEmptyPolls;
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
//        return "LocalQueueOperationStats{" +
//                "total= " + total() +
//                ", offers:" + numberOfOffers +
//                ", polls:" + numberOfPolls +
//                ", rejectedOffers:" + numberOfRejectedOffers +
//                ", emptyPolls:" + numberOfEmptyPolls +
//                ", others: " + numberOfOtherOperations +
//                ", received events: " + numberOfEvents +
//                "}";
//    }


    @Override
    public long getPeriodEnd() {
        return 0;
    }

    @Override
    public long getPeriodStart() {
        return 0;
    }
}
