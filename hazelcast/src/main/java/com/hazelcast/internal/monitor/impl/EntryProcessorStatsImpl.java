/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.EntryProcessorStats;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.util.ConcurrencyUtil.setMax;
import static com.hazelcast.internal.util.ConcurrencyUtil.setMin;
import static com.hazelcast.internal.util.TimeUtil.convertNanosToMillis;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public class EntryProcessorStatsImpl implements EntryProcessorStats {

    private static final AtomicLongFieldUpdater<EntryProcessorStatsImpl> MIN_EXECUTION_TIME_IN_MILLIS =
            newUpdater(EntryProcessorStatsImpl.class, "minExecutionTimeInNanos");
    private static final AtomicLongFieldUpdater<EntryProcessorStatsImpl> MAX_EXECUTION_TIME_IN_MILLIS =
            newUpdater(EntryProcessorStatsImpl.class, "maxExecutionTimeInNanos");
    private static final AtomicLongFieldUpdater<EntryProcessorStatsImpl> TOTAL_EXECUTION_TIME_IN_MILLIS =
            newUpdater(EntryProcessorStatsImpl.class, "totalExecutionTimeInNanos");
    private static final AtomicLongFieldUpdater<EntryProcessorStatsImpl> NUMBER_OF_EXECUTIONS =
            newUpdater(EntryProcessorStatsImpl.class, "numberOfExecutions");
    private static final AtomicLongFieldUpdater<EntryProcessorStatsImpl> NUMBER_OF_ENTRIES_UPDATED =
            newUpdater(EntryProcessorStatsImpl.class, "numberOfEntriesUpdated");


    private volatile long minExecutionTimeInNanos;
    private volatile long maxExecutionTimeInNanos;
    private volatile long totalExecutionTimeInNanos;
    private volatile long numberOfExecutions;
    private volatile long numberOfEntriesUpdated;

    @Override
    public long getNumberOfExecutions() {
        return numberOfExecutions;
    }

    @Override
    public long getMinExecutionTimeInMillis() {
        return convertNanosToMillis(minExecutionTimeInNanos);
    }

    @Override
    public long getMaxExecutionTimeInMillis() {
        return convertNanosToMillis(maxExecutionTimeInNanos);
    }

    @Override
    public float getAverageExecutionTimeInMillis() {
        long totalTimeInNanos = convertNanosToMillis(totalExecutionTimeInNanos);
        long numberOfExecutions = getNumberOfExecutions();

        if (totalTimeInNanos == 0 || numberOfExecutions == 0) {
            return 0;
        }
        return (1f * totalTimeInNanos) / numberOfExecutions;
    }

    @Override
    public long getNumberOfEntriesUpdated() {
        return numberOfEntriesUpdated;
    }

    public void recordExecutionTime(long executionTimeInMillis) {
        NUMBER_OF_EXECUTIONS.incrementAndGet(this);
        TOTAL_EXECUTION_TIME_IN_MILLIS.addAndGet(this, executionTimeInMillis);
        setMax(this, MAX_EXECUTION_TIME_IN_MILLIS, executionTimeInMillis);
        setMin(this, MIN_EXECUTION_TIME_IN_MILLIS, executionTimeInMillis);
    }

    public void incrementNumberOfEntriesUpdated(long delta) {
        NUMBER_OF_ENTRIES_UPDATED.addAndGet(this, delta);
    }
}
