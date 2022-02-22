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

import com.hazelcast.executor.LocalExecutorStats;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_CANCELLED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_COMPLETED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_PENDING;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_STARTED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_TOTAL_EXECUTION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.EXECUTOR_METRIC_TOTAL_START_LATENCY;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;

public class LocalExecutorStatsImpl implements LocalExecutorStats {

    private static final AtomicLongFieldUpdater<LocalExecutorStatsImpl> PENDING = AtomicLongFieldUpdater
            .newUpdater(LocalExecutorStatsImpl.class, "pending");
    private static final AtomicLongFieldUpdater<LocalExecutorStatsImpl> STARTED = AtomicLongFieldUpdater
            .newUpdater(LocalExecutorStatsImpl.class, "started");
    private static final AtomicLongFieldUpdater<LocalExecutorStatsImpl> COMPLETED = AtomicLongFieldUpdater
            .newUpdater(LocalExecutorStatsImpl.class, "completed");
    private static final AtomicLongFieldUpdater<LocalExecutorStatsImpl> CANCELLED = AtomicLongFieldUpdater
            .newUpdater(LocalExecutorStatsImpl.class, "cancelled");
    private static final AtomicLongFieldUpdater<LocalExecutorStatsImpl> TOTAL_START_LATENCY = AtomicLongFieldUpdater
            .newUpdater(LocalExecutorStatsImpl.class, "totalStartLatency");
    private static final AtomicLongFieldUpdater<LocalExecutorStatsImpl> TOTAL_EXECUTION_TIME = AtomicLongFieldUpdater
            .newUpdater(LocalExecutorStatsImpl.class, "totalExecutionTime");

    @Probe(name = EXECUTOR_METRIC_CREATION_TIME, unit = MS)
    private final long creationTime;

    // These fields are only accessed through the updaters
    @Probe(name = EXECUTOR_METRIC_PENDING)
    private volatile long pending;
    @Probe(name = EXECUTOR_METRIC_STARTED)
    private volatile long started;
    @Probe(name = EXECUTOR_METRIC_COMPLETED)
    private volatile long completed;
    @Probe(name = EXECUTOR_METRIC_CANCELLED)
    private volatile long cancelled;
    @Probe(name = EXECUTOR_METRIC_TOTAL_START_LATENCY, unit = MS)
    private volatile long totalStartLatency;
    @Probe(name = EXECUTOR_METRIC_TOTAL_EXECUTION_TIME, unit = MS)
    private volatile long totalExecutionTime;

    public LocalExecutorStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    public void startPending() {
        PENDING.incrementAndGet(this);
    }

    public void startExecution(long elapsed) {
        TOTAL_START_LATENCY.addAndGet(this, elapsed);
        STARTED.incrementAndGet(this);
        PENDING.decrementAndGet(this);
    }

    public void finishExecution(long elapsed) {
        TOTAL_EXECUTION_TIME.addAndGet(this, elapsed);
        COMPLETED.incrementAndGet(this);
    }

    public void rejectExecution() {
        PENDING.decrementAndGet(this);
    }

    public void cancelExecution() {
        CANCELLED.incrementAndGet(this);
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getPendingTaskCount() {
        return pending;
    }

    @Override
    public long getStartedTaskCount() {
        return started;
    }

    @Override
    public long getCompletedTaskCount() {
        return completed;
    }

    @Override
    public long getCancelledTaskCount() {
        return cancelled;
    }

    @Override
    public long getTotalStartLatency() {
        return totalStartLatency;
    }

    @Override
    public long getTotalExecutionLatency() {
        return totalExecutionTime;
    }

    @Override
    public String toString() {
        return "LocalExecutorStatsImpl{"
                + "creationTime=" + creationTime
                + ", pending=" + pending
                + ", started=" + started
                + ", completed=" + completed
                + ", cancelled=" + cancelled
                + ", totalStartLatency=" + totalStartLatency
                + ", totalExecutionTime=" + totalExecutionTime
                + '}';
    }
}
