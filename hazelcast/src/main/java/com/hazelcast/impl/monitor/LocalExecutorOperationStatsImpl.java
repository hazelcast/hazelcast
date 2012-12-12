/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl.monitor;

import com.hazelcast.monitor.LocalExecutorOperationStats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class LocalExecutorOperationStatsImpl extends LocalOperationStatsSupport implements LocalExecutorOperationStats  {
    private String executorName;
    final AtomicLong pending = new AtomicLong(0);
    final AtomicLong started = new AtomicLong(0);
    final AtomicLong startLatency = new AtomicLong(0);
    final AtomicLong completed = new AtomicLong(0);
    final AtomicLong completionTime = new AtomicLong(0);
    final AtomicLong minCompletionTime = new AtomicLong(Long.MAX_VALUE);
    final AtomicLong maxCompletionTime = new AtomicLong(Long.MIN_VALUE);


    public LocalExecutorOperationStatsImpl(String executorName) {
        this.executorName = executorName;
    }

    public String getExecutorName() {
        return executorName;
    }

    public long getPending() {
        return pending.get();
    }

    public long getStarted() {
        return started.get();
    }

    public long getStartLatency() {
        return startLatency.get();
    }

    public long getAverageStartLatency() {
        if(started.get() == 0)
            return 0;
        return startLatency.get() / started.get();
    }

    public long getCompleted() {
        return completed.get();
    }

    public long getCompletionTime() {
        return completionTime.get();
    }

    public long getMinCompletionTime() {
        return minCompletionTime.get();
    }

    public long getAverageCompletionTime() {
        if(completed.get() == 0) {
            return 0;
        }
        return completionTime.get() / completed.get();
    }

    public long getMaxCompletionTime() {
        return maxCompletionTime.get();
    }

    public void writeDataInternal(DataOutput out) throws IOException {
        out.writeUTF(executorName);
        out.writeLong(pending.get());
        out.writeLong(started.get());
        out.writeLong(startLatency.get());
        out.writeLong(completed.get());
        out.writeLong(completionTime.get());
        out.writeLong(minCompletionTime.get());
        out.writeLong(maxCompletionTime.get());
    }

    public void readDataInternal(DataInput in) throws IOException {
        executorName = in.readUTF();
        pending.set(in.readLong());
        started.set(in.readLong());
        startLatency.set(in.readLong());
        completed.set(in.readLong());
        completionTime.set(in.readLong());
        minCompletionTime.set(in.readLong());
        maxCompletionTime.set(in.readLong());
    }

}
