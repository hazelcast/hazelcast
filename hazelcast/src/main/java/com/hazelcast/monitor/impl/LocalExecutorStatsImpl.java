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
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.util.JsonUtil.getLong;

public class LocalExecutorStatsImpl implements LocalExecutorStats {

    private static final AtomicLongFieldUpdater<LocalExecutorStatsImpl> PENDING_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalExecutorStatsImpl.class, "pending");
    private static final AtomicLongFieldUpdater<LocalExecutorStatsImpl> STARTED_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalExecutorStatsImpl.class, "started");
    private static final AtomicLongFieldUpdater<LocalExecutorStatsImpl> COMPLETED_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalExecutorStatsImpl.class, "completed");
    private static final AtomicLongFieldUpdater<LocalExecutorStatsImpl> CANCELLED_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalExecutorStatsImpl.class, "cancelled");
    private static final AtomicLongFieldUpdater<LocalExecutorStatsImpl> TOTAL_START_LATENCY_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalExecutorStatsImpl.class, "totalStartLatency");
    private static final AtomicLongFieldUpdater<LocalExecutorStatsImpl> TOTAL_EXECUTION_TIME_UPDATER = AtomicLongFieldUpdater
            .newUpdater(LocalExecutorStatsImpl.class, "totalExecutionTime");
    private long creationTime;

    // These fields are only accessed through the updaters
    private volatile long pending;
    private volatile long started;
    private volatile long completed;
    private volatile long cancelled;
    private volatile long totalStartLatency;
    private volatile long totalExecutionTime;

    public LocalExecutorStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    public void startPending() {
        PENDING_UPDATER.incrementAndGet(this);
    }

    public void startExecution(long elapsed) {
        TOTAL_START_LATENCY_UPDATER.addAndGet(this, elapsed);
        STARTED_UPDATER.incrementAndGet(this);
        PENDING_UPDATER.decrementAndGet(this);
    }

    public void finishExecution(long elapsed) {
        TOTAL_EXECUTION_TIME_UPDATER.addAndGet(this, elapsed);
        COMPLETED_UPDATER.incrementAndGet(this);
    }

    public void rejectExecution() {
        PENDING_UPDATER.decrementAndGet(this);
    }

    public void cancelExecution() {
        CANCELLED_UPDATER.incrementAndGet(this);
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
        root.add("totalStartLatency", totalStartLatency);
        root.add("completed", completed);
        root.add("totalExecutionTime", totalExecutionTime);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        creationTime = getLong(json, "creationTime", -1L);
        PENDING_UPDATER.set(this, getLong(json, "pending", -1L));
        STARTED_UPDATER.set(this, getLong(json, "started", -1L));
        TOTAL_START_LATENCY_UPDATER.set(this, getLong(json, "totalStartLatency", -1L));
        COMPLETED_UPDATER.set(this, getLong(json, "completed", -1L));
        TOTAL_EXECUTION_TIME_UPDATER.set(this, getLong(json, "totalExecutionTime", -1L));
    }
}
