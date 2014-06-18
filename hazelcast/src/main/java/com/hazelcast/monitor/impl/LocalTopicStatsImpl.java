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

import com.eclipsesource.json.JsonObject;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Clock;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.util.JsonUtil.getLong;

public class LocalTopicStatsImpl
        implements LocalTopicStats {

    private static final AtomicLongFieldUpdater<LocalTopicStatsImpl> TOTAL_PUBLISHES_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalTopicStatsImpl.class, "totalPublishes");
    private static final AtomicLongFieldUpdater<LocalTopicStatsImpl> TOTAL_RECEIVED_MESSAGES_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalTopicStatsImpl.class, "totalReceivedMessages");
    private long creationTime;

    // These fields are only accessed through the updaters
    private volatile long totalPublishes;
    private volatile long totalReceivedMessages;

    public LocalTopicStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeLong(creationTime);
        out.writeLong(totalPublishes);
        out.writeLong(totalReceivedMessages);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        creationTime = in.readLong();
        TOTAL_PUBLISHES_UPDATER.set(this, in.readLong());
        TOTAL_RECEIVED_MESSAGES_UPDATER.set(this, in.readLong());
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("creationTime", creationTime);
        root.add("totalPublishes", totalPublishes);
        root.add("totalReceivedMessages", totalReceivedMessages);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        creationTime = getLong(json, "creationTime", -1L);
        TOTAL_PUBLISHES_UPDATER.set(this, getLong(json, "totalPublishes", -1L));
        TOTAL_RECEIVED_MESSAGES_UPDATER.set(this, getLong(json, "totalReceivedMessages", -1L));
    }

    @Override
    public long getPublishOperationCount() {
        return totalPublishes;
    }

    public void incrementPublishes() {
        TOTAL_PUBLISHES_UPDATER.incrementAndGet(this);
    }

    @Override
    public long getReceiveOperationCount() {
        return totalReceivedMessages;
    }

    public void incrementReceives() {
        TOTAL_RECEIVED_MESSAGES_UPDATER.incrementAndGet(this);
    }

}
