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

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapOperationStatsImpl implements DataSerializable, MapOperationStats {

    long periodStart;
    long periodEnd;
    long numberOfPuts;
    long numberOfGets;
    long numberOfRemoves;
    long numberOfOtherOperations;

    public void writeData(DataOutput out) throws IOException {
        out.writeLong(numberOfPuts);
        out.writeLong(numberOfGets);
        out.writeLong(numberOfRemoves);
        out.writeLong(numberOfOtherOperations);
        out.writeLong(periodStart);
        out.writeLong(periodEnd);
    }

    public void readData(DataInput in) throws IOException {
        numberOfPuts = in.readLong();
        numberOfGets = in.readLong();
        numberOfRemoves = in.readLong();
        numberOfOtherOperations = in.readLong();
        periodStart = in.readLong();
        periodEnd = in.readLong();
    }

    public long total() {
        return numberOfPuts + numberOfGets + numberOfRemoves + numberOfOtherOperations;
    }

    public String toString() {
        return "MapOperationStats{" +
                "total= " + total() +
                ", puts:" + numberOfPuts +
                ", gets:" + numberOfGets +
                ", removes:" + numberOfRemoves +
                ", others: " + numberOfOtherOperations +
                "}";
    }

    public long getPeriodStart() {
        return periodStart;
    }

    public long getPeriodEnd() {
        return periodEnd;
    }

    public long getNumberOfPuts() {
        return numberOfPuts;
    }

    public long getNumberOfGets() {
        return numberOfGets;
    }

    public long getNumberOfRemoves() {
        return numberOfRemoves;
    }

    public long getNumberOfOtherOperations() {
        return numberOfOtherOperations;
    }
}
