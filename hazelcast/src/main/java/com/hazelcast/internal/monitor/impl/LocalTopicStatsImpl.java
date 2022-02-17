/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.Clock;
import com.hazelcast.topic.LocalTopicStats;
import com.hazelcast.topic.impl.reliable.ReliableMessageRunner;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TOPIC_METRIC_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TOPIC_METRIC_TOTAL_PUBLISHES;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TOPIC_METRIC_TOTAL_RECEIVED_MESSAGES;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public class LocalTopicStatsImpl implements LocalTopicStats {

    private static final AtomicLongFieldUpdater<LocalTopicStatsImpl> TOTAL_PUBLISHES =
            newUpdater(LocalTopicStatsImpl.class, "totalPublishes");
    private static final AtomicLongFieldUpdater<LocalTopicStatsImpl> TOTAL_RECEIVED_MESSAGES =
            newUpdater(LocalTopicStatsImpl.class, "totalReceivedMessages");
    @Probe(name = TOPIC_METRIC_CREATION_TIME, unit = MS)
    private final long creationTime;

    // These fields are only accessed through the updaters
    @Probe(name = TOPIC_METRIC_TOTAL_PUBLISHES)
    private volatile long totalPublishes;
    @Probe(name = TOPIC_METRIC_TOTAL_RECEIVED_MESSAGES)
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
    public String toString() {
        return "LocalTopicStatsImpl{"
                + "creationTime=" + creationTime
                + ", totalPublishes=" + totalPublishes
                + ", totalReceivedMessages=" + totalReceivedMessages
                + '}';
    }
}
