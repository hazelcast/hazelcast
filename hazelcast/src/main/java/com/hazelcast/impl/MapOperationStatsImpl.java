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
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapOperationStatsImpl implements LocalMapOperationStats {

    long periodStart;
    long periodEnd;
    OperationCounter gets;
    OperationCounter puts;
    long numberOfRemoves;
    long numberOfOtherOperations;
    long numberOfEvents;

    public void writeData(DataOutput out) throws IOException {
        puts.writeData(out);
        gets.writeData(out);
        out.writeLong(numberOfRemoves);
        out.writeLong(numberOfOtherOperations);
        out.writeLong(numberOfEvents);
        out.writeLong(periodStart);
        out.writeLong(periodEnd);
    }

    public void readData(DataInput in) throws IOException {
        puts = new OperationCounter();
        puts.readData(in);
        gets = new OperationCounter();
        gets.readData(in);
        numberOfRemoves = in.readLong();
        numberOfOtherOperations = in.readLong();
        numberOfEvents = in.readLong();
        periodStart = in.readLong();
        periodEnd = in.readLong();
    }

    public long total() {
        return puts.count + gets.count + numberOfRemoves + numberOfOtherOperations;
    }

    public long getPeriodStart() {
        return periodStart;
    }

    public long getPeriodEnd() {
        return periodEnd;
    }

    public long getNumberOfPuts() {
        return puts.count;
    }

    public long getNumberOfGets() {
        return gets.count;
    }

    public long getNumberOfRemoves() {
        return numberOfRemoves;
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
                "\n, removes:" + numberOfRemoves +
                "\n, others: " + numberOfOtherOperations +
                "\n, received events: " + numberOfEvents +
                "}";
    }

    class OperationCounter implements DataSerializable {
        long count;
        long totalLatency;

        public OperationCounter() {
            this(0, 0);
        }

        public OperationCounter(long c, long l) {
            this.count = c;
            this.totalLatency = l;
        }

        @Override
        public String toString() {
            return "OperationCounter{" +
                    "count=" + count +
                    ", averageLatency=" + ((count == 0) ? 0 : totalLatency / count) +
                    '}';
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeLong(count);
            out.writeLong(totalLatency);
        }

        public void readData(DataInput in) throws IOException {
            count = in.readLong();
            totalLatency = in.readLong();
        }

        public void add(long c, long l) {
            count += c;
            totalLatency += l;
        }
    }
}
