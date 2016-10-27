/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduleexecutor.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class ScheduledTaskStatisticsImpl
        implements AmendableScheduledTaskStatistics {

    private final static TimeUnit MEASUREMENT_UNIT = TimeUnit.NANOSECONDS;

    private final long createdAtMonotonic = System.nanoTime();

    private long runs;

    private long createdAt = Clock.currentTimeMillis();

    private long firstRunStart;

    private long lastRunStart;

    private long lastRunEnd;

    private long lastIdleTime;

    private long totalRunTime;

    private long totalIdleTime;

    @Override
    public long getTotalRuns() {
        return runs;
    }

    @Override
    public long getCreatedAt() {
        return createdAt;
    }

    @Override
    public long getFirstRunStart(TimeUnit unit) {
        return unit.convert(firstRunStart, MEASUREMENT_UNIT);
    }

    @Override
    public long getLastRunStart(TimeUnit unit) {
        return unit.convert(lastRunStart, MEASUREMENT_UNIT);
    }

    @Override
    public long getLastRunEnd(TimeUnit unit) {
        return unit.convert(lastRunEnd, MEASUREMENT_UNIT);
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
        out.writeLong(createdAt);
        out.writeLong(firstRunStart);
        out.writeLong(lastRunStart);
        out.writeLong(lastRunEnd);
        out.writeLong(lastIdleTime);
        out.writeLong(totalIdleTime);
        out.writeLong(totalRunTime);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        runs = in.readLong();
        createdAt = in.readLong();
        firstRunStart = in.readLong();
        lastRunStart = in.readLong();
        lastRunEnd = in.readLong();
        lastIdleTime = in.readLong();
        totalIdleTime = in.readLong();
        totalRunTime = in.readLong();
    }

    @Override
    public void onBeforeRun() {
        long now = System.nanoTime();
        this.lastRunStart = now;
        this.lastIdleTime = now - (lastRunEnd != 0L ? lastRunEnd : createdAtMonotonic);
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
}
