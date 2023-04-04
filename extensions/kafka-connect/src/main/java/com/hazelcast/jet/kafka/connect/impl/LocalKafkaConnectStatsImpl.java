/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kafka.connect.impl;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.Clock;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.KAFKA_CONNECT_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.KAFKA_CONNECT_SOURCE_RECORD_POLL_AVG_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.KAFKA_CONNECT_SOURCE_RECORD_POLL_TOTAL;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public class LocalKafkaConnectStatsImpl implements LocalKafkaConnectStats {

    private final AtomicLongFieldUpdater<LocalKafkaConnectStatsImpl> sourceRecordPollTotalUpdater =
            newUpdater(LocalKafkaConnectStatsImpl.class, "sourceRecordPollTotal");
    @Probe(name = KAFKA_CONNECT_CREATION_TIME, unit = MS)
    private final long creationTime;

    // These fields are only accessed through the updaters
    @Probe(name = KAFKA_CONNECT_SOURCE_RECORD_POLL_TOTAL)
    private volatile long sourceRecordPollTotal;

    @Probe(name = KAFKA_CONNECT_SOURCE_RECORD_POLL_AVG_TIME, unit = MS)
    private volatile long sourceRecordPollAvgTime;

    private final AtomicLong sourceRecordPollTimes = new AtomicLong();
    private final AtomicLong sourceRecordPollCount = new AtomicLong();

    public LocalKafkaConnectStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Increment the number of locally polled source records.
     */
    public void incrementSourceRecordPoll(long amount) {
        sourceRecordPollTotalUpdater.addAndGet(this, amount);
    }

    public void addSourceRecordPollDuration(Duration duration) {
        long times = sourceRecordPollTimes.addAndGet(duration.toMillis());
        long count = sourceRecordPollCount.incrementAndGet();
        sourceRecordPollAvgTime = times / count;
    }

    public long getSourceRecordPollTotal() {
        return sourceRecordPollTotal;
    }

}
