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

import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class LocalExecutorStatsImpl implements LocalExecutorStats {

    private long creationTime;
    private final AtomicLong pending = new AtomicLong(0);
    private final AtomicLong started = new AtomicLong(0);
    private final AtomicLong totalStartLatency = new AtomicLong(0);
    private final AtomicLong completed = new AtomicLong(0);
    private final AtomicLong totalExecutionTime = new AtomicLong(0);

    public LocalExecutorStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(creationTime);
        out.writeLong(pending.get());
        out.writeLong(started.get());
        out.writeLong(totalStartLatency.get());
        out.writeLong(completed.get());
        out.writeLong(totalExecutionTime.get());
    }


    public void readData(ObjectDataInput in) throws IOException {
        creationTime = in.readLong();
        pending.set(in.readLong());
        started.set(in.readLong());
        totalStartLatency.set(in.readLong());
        completed.set(in.readLong());
        totalExecutionTime.set(in.readLong());
    }

    public void startExecution(long elapsed) {
        totalStartLatency.addAndGet(elapsed);
        started.incrementAndGet();
        pending.decrementAndGet();
    }

    public void finishExecution(long elapsed) {
        totalExecutionTime.addAndGet(elapsed);
        completed.incrementAndGet();
    }

    public void startPending() {
        pending.incrementAndGet();
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getPendingTaskCount() {
        return pending.get();
    }

    public long getStartedTaskCount() {
        return started.get();
    }

    public long getCompletedTaskCount() {
        return completed.get();
    }

    public long getTotalStartLatency() {
        return totalStartLatency.get();
    }

    public long getTotalExecutionLatency() {
        return totalExecutionTime.get();
    }
}
