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

import com.hazelcast.monitor.LocalSemaphoreStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class LocalSemaphoreStatsImpl implements LocalSemaphoreStats {
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }


    //    OperationStat acquires = new OperationStat(0, 0);
//    OperationStat nonAcquires = new OperationStat(0, 0);
//    long numberOfRejectedAcquires;
//    long numberOfPermitsAcquired;
//    long numberOfPermitsReleased;
//    long numberOfPermitsAttached;
//    long numberOfPermitsDetached;
//    long numberOfPermitsReduced;
//
//    public void writeData(ObjectDataOutput out) throws IOException {
//        acquires.writeData(out);
//        nonAcquires.writeData(out);
//        out.writeLong(numberOfRejectedAcquires);
//        out.writeLong(numberOfPermitsAcquired);
//        out.writeLong(numberOfPermitsReleased);
//        out.writeLong(numberOfPermitsAttached);
//        out.writeLong(numberOfPermitsDetached);
//        out.writeLong(numberOfPermitsReduced);
//    }
//
//    public void readData(ObjectDataInput in) throws IOException {
//        (acquires = new OperationStat()).readData(in);
//        (nonAcquires = new OperationStat()).readData(in);
//        numberOfRejectedAcquires = in.readLong();
//        numberOfPermitsAcquired = in.readLong();
//        numberOfPermitsReleased = in.readLong();
//        numberOfPermitsAttached = in.readLong();
//        numberOfPermitsDetached = in.readLong();
//        numberOfPermitsReduced = in.readLong();
//    }
//
//    public long total() {
//        return acquires.count + nonAcquires.count;
//    }
//
//    public long getNumberOfAcquireOps() {
//        return acquires.count;
//    }
//
//    public long getNumberOfNonAcquireOps() {
//        return nonAcquires.count;
//    }
//
//    public long getTotalAcquireLatency() {
//        return acquires.totalLatency;
//    }
//
//    public long getTotalNonAcquireLatency() {
//        return nonAcquires.totalLatency;
//    }
//
//    public long getNumberOfRejectedAcquires() {
//        return numberOfRejectedAcquires;
//    }
//
//    public long getNumberOfPermitsAcquired() {
//        return numberOfPermitsAcquired;
//    }
//
//    public long getNumberOfPermitsReduced() {
//        return numberOfPermitsReduced;
//    }
//
//    public long getNumberOfPermitsReleased() {
//        return numberOfPermitsReleased;
//    }
//
//    public long getNumberOfAttachedPermits() {
//        return numberOfPermitsAttached;
//    }
//
//    public long getNumberOfDetachedPermits() {
//        return numberOfPermitsDetached;
//    }
//
//    public String toString() {
//        return "LocalSemaphoreOperationStats{" +
//                "total= " + total() +
//                ", acquires total:" + acquires +
//                ", acquires rejected:" + numberOfRejectedAcquires +
//                ", non-acquires:" + nonAcquires +
//                ", permits acquired: " + numberOfPermitsAcquired +
//                ", permits released: " + numberOfPermitsReleased +
//                ", permits attached: " + numberOfPermitsAttached +
//                ", permits detached: " + numberOfPermitsDetached +
//                ", permits reduced: " + numberOfPermitsReduced +
//                "}";
//    }

    public long getCreationTime() {
        return 0;
    }
}
