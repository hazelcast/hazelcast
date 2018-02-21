/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.scheduledexecutor.ScheduledTaskStatistics;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.cluster.Versions.V3_10;

public class ScheduledTaskStatisticsImpl
        implements ScheduledTaskStatistics, TaskLifecycleListener, Versioned {

    private static final TimeUnit MEASUREMENT_UNIT = TimeUnit.NANOSECONDS;

    private long runs;
    private long lastRunDuration;
    private long lastIdleDuration;
    private long totalRunDuration;
    private long totalIdleDuration;

    private transient long createdAt;
    private transient long firstRunStart;
    private transient long lastRunStart;
    private transient long lastRunEnd;

    public ScheduledTaskStatisticsImpl() {
    }

    public ScheduledTaskStatisticsImpl(ScheduledTaskStatisticsImpl copy) {
        this(copy.createdAt, copy.getTotalRuns(), copy.firstRunStart, copy.lastRunStart, copy.lastRunEnd,
                copy.getLastIdleTime(MEASUREMENT_UNIT), copy.getTotalRunTime(MEASUREMENT_UNIT),
                copy.getTotalIdleTime(MEASUREMENT_UNIT), copy.getLastRunDuration(MEASUREMENT_UNIT));
    }

    public ScheduledTaskStatisticsImpl(long runs, long lastIdleTimeNanos, long totalRunTimeNanos, long totalIdleTimeNanos) {
        this.runs = runs;
        this.lastIdleDuration = lastIdleTimeNanos;
        this.totalRunDuration = totalRunTimeNanos;
        this.totalIdleDuration = totalIdleTimeNanos;
    }

    ScheduledTaskStatisticsImpl(long createdAt, long runs, long firstRunStartNanos, long lastRunStartNanos, long lastRunEndNanos,
                                long lastIdleTimeNanos, long totalRunTimeNanos, long totalIdleTimeNanos,
                                long lastRunDurationNanos) {
        this.createdAt = createdAt;
        this.runs = runs;
        this.firstRunStart = firstRunStartNanos;
        this.lastRunStart = lastRunStartNanos;
        this.lastRunEnd = lastRunEndNanos;
        this.lastRunDuration = lastRunDurationNanos;
        this.lastIdleDuration = lastIdleTimeNanos;
        this.totalRunDuration = totalRunTimeNanos;
        this.totalIdleDuration = totalIdleTimeNanos;
    }

    @Override
    public long getTotalRuns() {
        return runs;
    }

    @Override
    public long getLastRunDuration(TimeUnit unit) {
        return unit.convert(lastRunDuration, MEASUREMENT_UNIT);
    }

    @Override
    public long getLastIdleTime(TimeUnit unit) {
        return unit.convert(lastIdleDuration, MEASUREMENT_UNIT);
    }

    @Override
    public long getTotalIdleTime(TimeUnit unit) {
        return unit.convert(totalIdleDuration, MEASUREMENT_UNIT);
    }

    @Override
    public long getTotalRunTime(TimeUnit unit) {
        return unit.convert(totalRunDuration, MEASUREMENT_UNIT);
    }

    @Override
    public int getFactoryId() {
        return ScheduledExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ScheduledExecutorDataSerializerHook.TASK_STATS;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeLong(runs);
        out.writeLong(lastIdleDuration);
        out.writeLong(totalIdleDuration);
        out.writeLong(totalRunDuration);
        // RU_COMPAT_3_9
        if (out.getVersion().isGreaterOrEqual(V3_10)) {
            out.writeLong(lastRunDuration);
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        runs = in.readLong();
        lastIdleDuration = in.readLong();
        totalIdleDuration = in.readLong();
        totalRunDuration = in.readLong();
        // RU_COMPAT_3_9
        if (in.getVersion().isGreaterOrEqual(V3_10)) {
            lastRunDuration = in.readLong();
        }
    }

    @Override
    public void onInit() {
        this.createdAt = System.nanoTime();
    }

    @Override
    public void onBeforeRun() {
        long now = System.nanoTime();
        this.lastRunStart = now;
        this.lastIdleDuration = now - (lastRunEnd != 0L ? lastRunEnd : createdAt);
        this.totalIdleDuration += lastIdleDuration;

        if (this.firstRunStart == 0L) {
            this.firstRunStart = this.lastRunStart;
        }
    }

    @Override
    public void onAfterRun() {
        long now = System.nanoTime();

        this.lastRunEnd = now;
        this.lastRunDuration = lastRunEnd - lastRunStart;
        this.runs++;
        this.totalRunDuration += lastRunDuration;

    }

    public ScheduledTaskStatisticsImpl snapshot() {
        return new ScheduledTaskStatisticsImpl(this);
    }

    @Override
    public String toString() {
        return "ScheduledTaskStatisticsImpl{"
                + "runs=" + runs
                + ", lastIdleDuration=" + lastIdleDuration
                + ", totalRunDuration=" + totalRunDuration
                + ", totalIdleDuration=" + totalIdleDuration
                + ", lastRunDuration=" + lastRunDuration
                + '}';
    }
}
