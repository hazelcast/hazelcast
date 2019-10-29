/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.topic.LocalTopicStats;
import com.hazelcast.topic.impl.reliable.ReliableMessageRunner;
import com.hazelcast.internal.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.util.JsonUtil.getLong;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public class LocalTopicStatsImpl implements LocalTopicStats {

    private static final AtomicLongFieldUpdater<LocalTopicStatsImpl> TOTAL_PUBLISHES =
            newUpdater(LocalTopicStatsImpl.class, "totalPublishes");
    private static final AtomicLongFieldUpdater<LocalTopicStatsImpl> TOTAL_RECEIVED_MESSAGES =
            newUpdater(LocalTopicStatsImpl.class, "totalReceivedMessages");
    @Probe
    private long creationTime;

    // These fields are only accessed through the updaters
    @Probe
    private volatile long totalPublishes;
    @Probe
    private volatile long totalReceivedMessages;

    public LocalTopicStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getPublishOperationCount() {
        return totalPublishes;
    }

    /**
     * Increment the number of locally published messages. The count can be local
     * to the member or local to a single proxy (whereas there are many proxies
     * on one member).
     *
     * @see com.hazelcast.topic.impl.TopicService
     * @see com.hazelcast.topic.impl.reliable.ReliableTopicService
     */
    public void incrementPublishes() {
        TOTAL_PUBLISHES.incrementAndGet(this);
    }

    @Override
    public long getReceiveOperationCount() {
        return totalReceivedMessages;
    }

    /**
     * Increment the number of locally received messages. The count can be local
     * to the member or local to a single listener (whereas there are many listeners
     * on one member).
     *
     * @see com.hazelcast.topic.impl.TopicService
     * @see ReliableMessageRunner
     */
    public void incrementReceives() {
        TOTAL_RECEIVED_MESSAGES.incrementAndGet(this);
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
        totalPublishes = getLong(json, "totalPublishes", -1L);
        totalReceivedMessages = getLong(json, "totalReceivedMessages", -1L);
    }

    @Override
    public String toString() {
        return "LocalTopicStatsImpl{"
                + "creationTime=" + creationTime
                + ", totalPublishes=" + totalPublishes
                + ", totalReceivedMessages=" + totalReceivedMessages
                + '}';
    }
}
