/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl.monitor;

import com.hazelcast.monitor.LocalMapOperationStats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LocalMapOperationStatsImpl extends LocalOperationStatsSupport implements LocalMapOperationStats {

    OperationStat gets = new OperationStat(0, 0);
    OperationStat puts = new OperationStat(0, 0);
    OperationStat removes = new OperationStat(0, 0);
    long numberOfOtherOperations;
    long numberOfEvents;

    void writeDataInternal(DataOutput out) throws IOException {
        puts.writeData(out);
        gets.writeData(out);
        removes.writeData(out);
        out.writeLong(numberOfOtherOperations);
        out.writeLong(numberOfEvents);
    }

    void readDataInternal(DataInput in) throws IOException {
        puts = new OperationStat();
        puts.readData(in);
        gets = new OperationStat();
        gets.readData(in);
        removes = new OperationStat();
        removes.readData(in);
        numberOfOtherOperations = in.readLong();
        numberOfEvents = in.readLong();
    }

    public long total() {
        return puts.count + gets.count + removes.count + numberOfOtherOperations;
    }

    public long getNumberOfPuts() {
        return puts.count;
    }

    public long getNumberOfGets() {
        return gets.count;
    }

    public long getTotalPutLatency() {
        return puts.totalLatency;
    }

    public long getTotalGetLatency() {
        return gets.totalLatency;
    }

    public long getTotalRemoveLatency() {
        return removes.totalLatency;
    }

    public long getNumberOfRemoves() {
        return removes.count;
    }

    public long getNumberOfOtherOperations() {
        return numberOfOtherOperations;
    }

    public long getNumberOfEvents() {
        return numberOfEvents;
    }

    public String toString() {
        return "LocalMapOperationStats{" +
                "total= " + total() +
                "\n, puts:" + puts +
                "\n, gets:" + gets +
                "\n, removes:" + removes +
                "\n, others: " + numberOfOtherOperations +
                "\n, received events: " + numberOfEvents +
                "}";
    }
}
