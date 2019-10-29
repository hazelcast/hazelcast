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
import com.hazelcast.executor.LocalExecutorStats;
import com.hazelcast.internal.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.util.JsonUtil.getLong;

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
    private long creationTime;

    // These fields are only accessed through the updaters
    @Probe
    private volatile long pending;
    @Probe
    private volatile long started;
    @Probe
    private volatile long completed;
    @Probe
    private volatile long cancelled;
    @Probe
    private volatile long totalStartLatency;
    @Probe
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
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("creationTime", creationTime);
        root.add("pending", pending);
        root.add("started", started);
        root.add("completed", completed);
        root.add("cancelled", cancelled);
        root.add("totalStartLatency", totalStartLatency);
        root.add("totalExecutionTime", totalExecutionTime);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        creationTime = getLong(json, "creationTime", -1L);
        PENDING.set(this, getLong(json, "pending", -1L));
        STARTED.set(this, getLong(json, "started", -1L));
        COMPLETED.set(this, getLong(json, "completed", -1L));
        CANCELLED.set(this, getLong(json, "cancelled", -1L));
        TOTAL_START_LATENCY.set(this, getLong(json, "totalStartLatency", -1L));
        TOTAL_EXECUTION_TIME.set(this, getLong(json, "totalExecutionTime", -1L));
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
