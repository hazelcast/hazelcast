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

package com.hazelcast.monitor.impl;

import com.hazelcast.monitor.LocalQueueOperationStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class LocalQueueOperationStatsImpl extends LocalOperationStatsSupport
        implements LocalQueueOperationStats {

    long numberOfOffers;
    long numberOfRejectedOffers;
    long numberOfPolls;
    long numberOfEmptyPolls;
    long numberOfOtherOperations;
    long numberOfEvents;

    void writeDataInternal(ObjectDataOutput out) throws IOException {
        out.writeLong(numberOfOffers);
        out.writeLong(numberOfPolls);
        out.writeLong(numberOfRejectedOffers);
        out.writeLong(numberOfEmptyPolls);
        out.writeLong(numberOfOtherOperations);
        out.writeLong(numberOfEvents);
    }

    void readDataInternal(ObjectDataInput in) throws IOException {
        numberOfOffers = in.readLong();
        numberOfPolls = in.readLong();
        numberOfRejectedOffers = in.readLong();
        numberOfEmptyPolls = in.readLong();
        numberOfOtherOperations = in.readLong();
        numberOfEvents = in.readLong();
    }

    public long total() {
        return numberOfOffers + numberOfPolls + numberOfOtherOperations;
    }

    public long getNumberOfOffers() {
        return numberOfOffers;
    }

    public long getNumberOfRejectedOffers() {
        return numberOfRejectedOffers;
    }

    public long getNumberOfPolls() {
        return numberOfPolls;
    }

    public long getNumberOfEmptyPolls() {
        return numberOfEmptyPolls;
    }

    public long getNumberOfOtherOperations() {
        return numberOfOtherOperations;
    }

    public long getNumberOfEvents() {
        return numberOfEvents;
    }

    public String toString() {
        return "LocalQueueOperationStats{" +
                "total= " + total() +
                ", offers:" + numberOfOffers +
                ", polls:" + numberOfPolls +
                ", rejectedOffers:" + numberOfRejectedOffers +
                ", emptyPolls:" + numberOfEmptyPolls +
                ", others: " + numberOfOtherOperations +
                ", received events: " + numberOfEvents +
                "}";
    }
}