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

import java.util.concurrent.atomic.AtomicLong;

public class ExecutorOperationsCounter extends OperationsCounterSupport<LocalExecutorOperationStats> {

    private final static LocalExecutorOperationStats empty = new LocalExecutorOperationStatsImpl();

    private final OperationCounter executionStarts = new OperationCounter();
    private final OperationCounter executionEnds = new OperationCounter();
    private final AtomicLong pending = new AtomicLong(0);
    private final AtomicLong maxExecutionTime = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong minExecutionTime = new AtomicLong(Long.MAX_VALUE);

    public ExecutorOperationsCounter(long interval) {
        super(interval);
    }

    ExecutorOperationsCounter getAndReset() {
        OperationCounter executionNow = executionStarts.copyAndReset();
        OperationCounter waitNow = executionEnds.copyAndReset();
        long pendingNow = pending.get();
        long maxCompletion = maxExecutionTime.getAndSet(Long.MIN_VALUE);
        long minCompletion = minExecutionTime.getAndSet(Long.MAX_VALUE);

        ExecutorOperationsCounter newOne = new ExecutorOperationsCounter(interval);
        newOne.executionStarts.set(executionNow);
        newOne.executionEnds.set(waitNow);
        newOne.pending.set(pendingNow);
        newOne.maxExecutionTime.set(maxCompletion);
        newOne.minExecutionTime.set(minCompletion);

        newOne.startTime = this.startTime;
        newOne.endTime = now();
        this.startTime = newOne.endTime;
        return newOne;
    }

    public void finishExecution(long elapsed) {
        executionEnds.count(elapsed);
        if (elapsed > maxExecutionTime.get())
            maxExecutionTime.set(elapsed);
        if (elapsed < minExecutionTime.get())
            minExecutionTime.set(elapsed);
        publishSubResult();
    }

    public void startExecution(long elapsed) {
        executionStarts.count(elapsed);
        pending.decrementAndGet();
        publishSubResult();
    }

    public void startPending() {
        pending.incrementAndGet();
        publishSubResult();
    }

    LocalExecutorOperationStats aggregateSubCounterStats() {
        LocalExecutorOperationStatsImpl stats = new LocalExecutorOperationStatsImpl();
        stats.periodStart = ((ExecutorOperationsCounter) listOfSubCounters.get(0)).startTime;
        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;

        for (int i = 0; i < listOfSubCounters.size(); i++) {
            ExecutorOperationsCounter sub = (ExecutorOperationsCounter) listOfSubCounters.get(i);

            if (sub.minExecutionTime.get() < min)
                min = sub.minExecutionTime.get();

            if (sub.maxExecutionTime.get() > max)
                max = sub.maxExecutionTime.get();

            stats.started.addAndGet(sub.executionStarts.count.get());
            stats.startLatency.addAndGet(sub.executionStarts.totalLatency.get());

            stats.completed.addAndGet(sub.executionEnds.count.get());
            stats.totalExecutionTime.addAndGet(sub.executionEnds.totalLatency.get());
            stats.periodEnd = sub.endTime;
        }
        stats.maxExecutionTime.set(max);
        stats.minExecutionTime.set(min);
        stats.pending.set(((ExecutorOperationsCounter) listOfSubCounters.get(listOfSubCounters.size() - 1)).pending.get());
        return stats;
    }

    LocalExecutorOperationStats getThis() {
        LocalExecutorOperationStatsImpl stats = new LocalExecutorOperationStatsImpl();
        stats.periodStart = this.startTime;
        stats.started.set(this.executionStarts.count.get());
        stats.startLatency.set(this.executionStarts.totalLatency.get());

        stats.completed.set(this.executionEnds.count.get());
        stats.totalExecutionTime.set(this.executionEnds.totalLatency.get());

        stats.maxExecutionTime.set(this.maxExecutionTime.get());
        stats.minExecutionTime.set(this.minExecutionTime.get());
        stats.periodEnd = now();
        return stats;
    }

    LocalExecutorOperationStats getEmpty() {
        return empty;
    }


}
