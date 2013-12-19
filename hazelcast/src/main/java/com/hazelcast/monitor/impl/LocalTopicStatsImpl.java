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

import com.hazelcast.management.JsonWriter;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class LocalTopicStatsImpl implements LocalTopicStats {

    private long creationTime;
    private AtomicLong totalPublishes = new AtomicLong(0);
    private AtomicLong totalReceivedMessages = new AtomicLong(0);

    public LocalTopicStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public void toJson(JsonWriter writer) {
        writer.write("creationTime", creationTime);
        writer.write("totalPublishes", totalPublishes);
        writer.write("totalReceivedMessages", totalReceivedMessages);
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(creationTime);
        out.writeLong(totalPublishes.get());
        out.writeLong(totalReceivedMessages.get());
    }

    public void readData(ObjectDataInput in) throws IOException {
        creationTime = in.readLong();
        totalPublishes.set(in.readLong());
        totalReceivedMessages.set(in.readLong());
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getPublishOperationCount() {
        return totalPublishes.get();
    }

    public void incrementPublishes() {
        totalPublishes.incrementAndGet();
    }

    public long getReceiveOperationCount() {
        return totalReceivedMessages.get();
    }

    public void incrementReceives() {
        totalReceivedMessages.incrementAndGet();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocalTopicStatsImpl that = (LocalTopicStatsImpl) o;

        if (creationTime != that.creationTime) return false;
        if (totalPublishes.get() != that.totalPublishes.get()) return false;
        if (totalReceivedMessages.get() != that.totalReceivedMessages.get()) return false;
        return true;
    }
}
