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

import com.hazelcast.monitor.LocalExecutorOperationStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class LocalExecutorOperationStatsImpl extends LocalOperationStatsSupport implements LocalExecutorOperationStats {

    final AtomicLong pending = new AtomicLong(0);
    final AtomicLong started = new AtomicLong(0);
    final AtomicLong startLatency = new AtomicLong(0);
    final AtomicLong completed = new AtomicLong(0);
    final AtomicLong totalExecutionTime = new AtomicLong(0);
    final AtomicLong minExecutionTime = new AtomicLong(Long.MAX_VALUE);
    final AtomicLong maxExecutionTime = new AtomicLong(Long.MIN_VALUE);


    public LocalExecutorOperationStatsImpl() {
    }

    public long getPending() {
        return pending.get();
    }

    public long getStarted() {
        return started.get();
    }

    public long getAverageStartLatency() {
        if (started.get() == 0)
            return 0;
        return startLatency.get() / started.get();
    }

    public long getCompleted() {
        return completed.get();
    }

    public long getMinExecutionTime() {
        return minExecutionTime.get();
    }

    public long getAverageExecutionTime() {
        if (completed.get() == 0) {
            return 0;
        }
        return totalExecutionTime.get() / completed.get();
    }

    public long getMaxExecutionTime() {
        return maxExecutionTime.get();
    }

    public void writeDataInternal(ObjectDataOutput out) throws IOException {
        out.writeLong(pending.get());
        out.writeLong(started.get());
        out.writeLong(startLatency.get());
        out.writeLong(completed.get());
        out.writeLong(totalExecutionTime.get());
        out.writeLong(minExecutionTime.get());
        out.writeLong(maxExecutionTime.get());
    }

    public void readDataInternal(ObjectDataInput in) throws IOException {
        pending.set(in.readLong());
        started.set(in.readLong());
        startLatency.set(in.readLong());
        completed.set(in.readLong());
        totalExecutionTime.set(in.readLong());
        minExecutionTime.set(in.readLong());
        maxExecutionTime.set(in.readLong());
    }

    @Override
    public String toString() {
        return "LocalExecutorOperationStatsImpl{" +
                "pending=" + pending +
                ", started=" + started +
                ", startLatency=" + startLatency +
                ", completed=" + completed +
                ", totalExecutionTime=" + totalExecutionTime +
                ", averageExecutionTime= " + getAverageExecutionTime() +
                ", minExecutionTime=" + minExecutionTime +
                ", maxExecutionTime=" + maxExecutionTime +
                '}';
    }
}
