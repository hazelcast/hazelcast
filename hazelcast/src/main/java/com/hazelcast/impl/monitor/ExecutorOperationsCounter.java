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

import java.util.concurrent.atomic.AtomicLong;

public class ExecutorOperationsCounter extends OperationsCounterSupport<LocalExecutorOperationStats> {

    private final static LocalExecutorOperationStats empty = new LocalExecutorOperationStatsImpl("");

    private final OperationCounter executionStarts = new OperationCounter();
    private final OperationCounter executionEnds = new OperationCounter();
    private final AtomicLong pending = new AtomicLong(0);
    private final AtomicLong maxCompletionTime = new AtomicLong(Long.MIN_VALUE);
    private final AtomicLong minCompletionTime = new AtomicLong(Long.MAX_VALUE);
    private final String executorName;

    public ExecutorOperationsCounter(long interval, String name) {
        super(interval);
        executorName = name;
    }

    ExecutorOperationsCounter getAndReset() {
        OperationCounter executionNow = executionStarts.copyAndReset();
        OperationCounter waitNow = executionEnds.copyAndReset();
        long pendingNow = pending.get();
        long maxCompletion = maxCompletionTime.getAndSet(Long.MIN_VALUE);
        long minCompletion = minCompletionTime.getAndSet(Long.MAX_VALUE);

        ExecutorOperationsCounter newOne = new ExecutorOperationsCounter(interval, executorName);
        newOne.executionStarts.set(executionNow);
        newOne.executionEnds.set(waitNow);
        newOne.pending.set(pendingNow);
        newOne.maxCompletionTime.set(maxCompletion);
        newOne.minCompletionTime.set(minCompletion);

        newOne.startTime = this.startTime;
        newOne.endTime = now();
        this.startTime = newOne.endTime;
        return newOne;
    }

    public void finishExecution(long elapsed) {
        executionEnds.count(elapsed);
        if(elapsed > maxCompletionTime.get())
            maxCompletionTime.set(elapsed);
        if(elapsed < minCompletionTime.get())
            minCompletionTime.set(elapsed);
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
        LocalExecutorOperationStatsImpl stats = new LocalExecutorOperationStatsImpl(executorName);
        stats.periodStart = ((ExecutorOperationsCounter) listOfSubCounters.get(0)).startTime;
        long max = Long.MIN_VALUE;
        long min = Long.MAX_VALUE;

        for (int i = 0; i < listOfSubCounters.size(); i++) {
            ExecutorOperationsCounter sub = (ExecutorOperationsCounter) listOfSubCounters.get(i);

            if(sub.minCompletionTime.get() < min)
                min = sub.minCompletionTime.get();

            if(sub.maxCompletionTime.get() > max)
                max = sub.maxCompletionTime.get();

            stats.started.addAndGet(sub.executionStarts.count.get());
            stats.startLatency.addAndGet(sub.executionStarts.totalLatency.get());

            stats.completed.addAndGet(sub.executionEnds.count.get());
            stats.completionTime.addAndGet(sub.executionEnds.totalLatency.get());
            stats.periodEnd = sub.endTime;
        }
        stats.maxCompletionTime.set(max);
        stats.minCompletionTime.set(min);
        stats.pending.set(((ExecutorOperationsCounter) listOfSubCounters.get(listOfSubCounters.size() - 1)).pending.get());
        return stats;
    }

    LocalExecutorOperationStats getThis() {
        LocalExecutorOperationStatsImpl stats = new LocalExecutorOperationStatsImpl(executorName);
        stats.periodStart = this.startTime;
        stats.started.set(this.executionStarts.count.get());
        stats.startLatency.set(this.executionStarts.totalLatency.get());

        stats.completed.set(this.executionEnds.count.get());
        stats.completionTime.set(this.executionEnds.totalLatency.get());

        stats.maxCompletionTime.set(this.maxCompletionTime.get());
        stats.minCompletionTime.set(this.minCompletionTime.get());
        stats.periodEnd = now();
        return stats;
    }

    LocalExecutorOperationStats getEmpty() {
        return empty;
    }


}
