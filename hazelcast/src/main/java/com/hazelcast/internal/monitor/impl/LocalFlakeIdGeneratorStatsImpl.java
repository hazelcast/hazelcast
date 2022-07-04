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
import com.hazelcast.internal.monitor.LocalFlakeIdGeneratorStats;
import com.hazelcast.internal.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.FLAKE_ID_METRIC_BATCH_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.FLAKE_ID_METRIC_CREATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.FLAKE_ID_METRIC_ID_COUNT;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public class LocalFlakeIdGeneratorStatsImpl implements LocalFlakeIdGeneratorStats {

    private static final AtomicLongFieldUpdater<LocalFlakeIdGeneratorStatsImpl> BATCH_COUNT =
            newUpdater(LocalFlakeIdGeneratorStatsImpl.class, "batchCount");
    private static final AtomicLongFieldUpdater<LocalFlakeIdGeneratorStatsImpl> ID_COUNT =
            newUpdater(LocalFlakeIdGeneratorStatsImpl.class, "idCount");

    @Probe(name = FLAKE_ID_METRIC_CREATION_TIME, unit = MS)
    private final long creationTime;
    @Probe(name = FLAKE_ID_METRIC_BATCH_COUNT)
    private volatile long batchCount;
    @Probe(name = FLAKE_ID_METRIC_ID_COUNT)
    private volatile long idCount;

    public LocalFlakeIdGeneratorStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getBatchCount() {
        return batchCount;
    }

    public long getIdCount() {
        return idCount;
    }

    public void update(int batchSize) {
        BATCH_COUNT.incrementAndGet(this);
        ID_COUNT.addAndGet(this, batchSize);
    }

    @Override
    public String toString() {
        return "LocalFlakeIdStatsImpl{"
                + "creationTime=" + creationTime
                + ", batchCount=" + batchCount
                + ", idCount=" + idCount
                + '}';
    }
}
