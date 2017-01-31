/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.scheduledexecutor.ScheduledTaskStatistics;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class ScheduledTaskStatisticsImpl
        implements ScheduledTaskStatistics, TaskLifecycleListener {

    private static final TimeUnit MEASUREMENT_UNIT = TimeUnit.NANOSECONDS;

    private long runs;

    private long createdAt;

    private long firstRunStart;

    private long lastRunStart;

    private long lastRunEnd;

    private long lastIdleTime;

    private long totalRunTime;

    private long totalIdleTime;

    public ScheduledTaskStatisticsImpl() {
    }

    public ScheduledTaskStatisticsImpl(ScheduledTaskStatisticsImpl copy) {
        this(copy.createdAt, copy.getTotalRuns(), copy.firstRunStart, copy.lastRunStart,
                copy.lastRunEnd, copy.getLastIdleTime(MEASUREMENT_UNIT), copy.getTotalRunTime(MEASUREMENT_UNIT),
                copy.getTotalIdleTime(MEASUREMENT_UNIT));
    }

    public ScheduledTaskStatisticsImpl(long runs, long lastIdleTimeNanos, long totalRunTimeNanos,
                                       long totalIdleTimeNanos) {
        this.runs = runs;
        this.lastIdleTime = lastIdleTimeNanos;
        this.totalRunTime = totalRunTimeNanos;
        this.totalIdleTime = totalIdleTimeNanos;
    }

    ScheduledTaskStatisticsImpl(long createdAt, long runs, long firstRunStartNanos, long lastRunStartNanos,
                                       long lastRunEndNanos, long lastIdleTimeNanos, long totalRunTimeNanos,
                                       long totalIdleTimeNanos) {
        this.createdAt = createdAt;
        this.runs = runs;
        this.firstRunStart = firstRunStartNanos;
        this.lastRunStart = lastRunStartNanos;
        this.lastRunEnd = lastRunEndNanos;
        this.lastIdleTime = lastIdleTimeNanos;
        this.totalRunTime = totalRunTimeNanos;
        this.totalIdleTime = totalIdleTimeNanos;
    }

    @Override
    public long getTotalRuns() {
        return runs;
    }

    @Override
    public long getLastRunDuration(TimeUnit unit) {
        long duration = lastRunEnd - lastRunStart;
        return unit.convert(duration, MEASUREMENT_UNIT);
    }

    @Override
    public long getLastIdleTime(TimeUnit unit) {
        return unit.convert(lastIdleTime, MEASUREMENT_UNIT);
    }

    @Override
    public long getTotalIdleTime(TimeUnit unit) {
        return unit.convert(totalIdleTime, MEASUREMENT_UNIT);
    }

    @Override
    public long getTotalRunTime(TimeUnit unit) {
        return unit.convert(totalRunTime, MEASUREMENT_UNIT);
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
        out.writeLong(lastIdleTime);
        out.writeLong(totalIdleTime);
        out.writeLong(totalRunTime);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        runs = in.readLong();
        lastIdleTime = in.readLong();
        totalIdleTime = in.readLong();
        totalRunTime = in.readLong();
    }

    @Override
    public void onInit() {
        this.createdAt  = System.nanoTime();
    }

    @Override
    public void onBeforeRun() {
        long now = System.nanoTime();
        this.lastRunStart = now;
        this.lastIdleTime = now - (lastRunEnd != 0L ? lastRunEnd : createdAt);
        this.totalIdleTime += lastIdleTime;

        if (this.firstRunStart == 0L) {
            this.firstRunStart = this.lastRunStart;
        }
    }

    @Override
    public void onAfterRun() {
        long now = System.nanoTime();

        long lastRunTime = now - lastRunStart;

        this.lastRunEnd = now;
        this.runs++;
        this.totalRunTime += lastRunTime;

    }

    public ScheduledTaskStatisticsImpl snapshot() {
        return new ScheduledTaskStatisticsImpl(this);
    }

    @Override
    public String toString() {
        return "ScheduledTaskStatisticsImpl{ runs=" + runs + ", createdAt="
                + createdAt + ", firstRunStart=" + firstRunStart + ", lastRunStart=" + lastRunStart + ", lastRunEnd=" + lastRunEnd
                + ", lastIdleTime=" + lastIdleTime + ", totalRunTime=" + totalRunTime + ", totalIdleTime=" + totalIdleTime + '}';
    }
}
