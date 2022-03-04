/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.monitor.LocalPNCounterStats;
import com.hazelcast.internal.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PNCOUNTER_METRIC_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PNCOUNTER_METRIC_TOTAL_DECREMENT_OPERATION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PNCOUNTER_METRIC_TOTAL_INCREMENT_OPERATION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.PNCOUNTER_METRIC_VALUE;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

/**
 * Local PN counter statistics thread safe implementation
 */
public class LocalPNCounterStatsImpl implements LocalPNCounterStats {

    private static final AtomicLongFieldUpdater<LocalPNCounterStatsImpl> TOTAL_INCREMENT_OPERATION_COUNT =
            newUpdater(LocalPNCounterStatsImpl.class, "totalIncrementOperationCount");
    private static final AtomicLongFieldUpdater<LocalPNCounterStatsImpl> TOTAL_DECREMENT_OPERATION_COUNT =
            newUpdater(LocalPNCounterStatsImpl.class, "totalDecrementOperationCount");

    @Probe(name = PNCOUNTER_METRIC_CREATION_TIME, unit = MS)
    private final long creationTime;
    @Probe(name = PNCOUNTER_METRIC_VALUE)
    private volatile long value;
    @Probe(name = PNCOUNTER_METRIC_TOTAL_INCREMENT_OPERATION_COUNT)
    private volatile long totalIncrementOperationCount;
    @Probe(name = PNCOUNTER_METRIC_TOTAL_DECREMENT_OPERATION_COUNT)
    private volatile long totalDecrementOperationCount;

    public LocalPNCounterStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getValue() {
        return value;
    }

    @Override
    public long getTotalIncrementOperationCount() {
        return totalIncrementOperationCount;
    }

    @Override
    public long getTotalDecrementOperationCount() {
        return totalDecrementOperationCount;
    }

    /**
     * Sets the current value for the PN counter.
     *
     * @param value the PN counter value
     */
    public void setValue(long value) {
        this.value = value;
    }

    /**
     * Increments the number of add (including increment) operations on this
     * PN counter.
     */
    public void incrementIncrementOperationCount() {
        TOTAL_INCREMENT_OPERATION_COUNT.incrementAndGet(this);
    }

    /**
     * Increments the number of subtract (including decrement) operations on
     * this PN counter.
     */
    public void incrementDecrementOperationCount() {
        TOTAL_DECREMENT_OPERATION_COUNT.incrementAndGet(this);
    }

    @Override
    public String toString() {
        return "LocalPNCounterStatsImpl{"
                + "creationTime=" + creationTime
                + ", value=" + value
                + ", totalIncrementOperationCount=" + totalIncrementOperationCount
                + ", totalDecrementOperationCount=" + totalDecrementOperationCount
                + '}';
    }
}
