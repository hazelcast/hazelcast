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
import com.hazelcast.partition.LocalReplicationStats;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_DIFF_PARTITION_REPLICATION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_DIFF_PARTITION_REPLICATION_RECORDS_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_FULL_PARTITION_REPLICATION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_FULL_PARTITION_REPLICATION_RECORDS_COUNT;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public class LocalReplicationStatsImpl implements LocalReplicationStats {

    private static final AtomicLongFieldUpdater<LocalReplicationStatsImpl> FULL_PARTITION_REPLICATION_COUNT =
            newUpdater(LocalReplicationStatsImpl.class, "fullPartitionReplicationCount");
    private static final AtomicLongFieldUpdater<LocalReplicationStatsImpl> DIFF_PARTITION_REPLICATION_COUNT =
            newUpdater(LocalReplicationStatsImpl.class, "diffPartitionReplicationCount");
    private static final AtomicLongFieldUpdater<LocalReplicationStatsImpl> FULL_PARTITION_REPLICATION_RECORDS_COUNT =
            newUpdater(LocalReplicationStatsImpl.class, "fullPartitionReplicationRecordsCount");
    private static final AtomicLongFieldUpdater<LocalReplicationStatsImpl> DIFF_PARTITION_REPLICATION_RECORDS_COUNT =
            newUpdater(LocalReplicationStatsImpl.class, "diffPartitionReplicationRecordsCount");

    @Probe(name = MAP_METRIC_FULL_PARTITION_REPLICATION_COUNT)
    private volatile long fullPartitionReplicationCount;
    @Probe(name = MAP_METRIC_DIFF_PARTITION_REPLICATION_COUNT)
    private volatile long diffPartitionReplicationCount;
    @Probe(name = MAP_METRIC_FULL_PARTITION_REPLICATION_RECORDS_COUNT)
    private volatile long fullPartitionReplicationRecordsCount;
    @Probe(name = MAP_METRIC_DIFF_PARTITION_REPLICATION_RECORDS_COUNT)
    private volatile long diffPartitionReplicationRecordsCount;

    @Override
    public long getDifferentialReplicationRecordCount() {
        return diffPartitionReplicationRecordsCount;
    }

    @Override
    public long getFullReplicationRecordCount() {
        return fullPartitionReplicationRecordsCount;
    }

    @Override
    public long getDifferentialPartitionReplicationCount() {
        return diffPartitionReplicationCount;
    }

    @Override
    public long getFullPartitionReplicationCount() {
        return fullPartitionReplicationCount;
    }

    public void incrementFullPartitionReplicationCount() {
        FULL_PARTITION_REPLICATION_COUNT.incrementAndGet(this);
    }

    public void incrementDiffPartitionReplicationCount() {
        DIFF_PARTITION_REPLICATION_COUNT.incrementAndGet(this);
    }

    public void incrementFullPartitionReplicationRecordsCount(long delta) {
        FULL_PARTITION_REPLICATION_RECORDS_COUNT.addAndGet(this, delta);
    }

    public void incrementDiffPartitionReplicationRecordsCount(long delta) {
        DIFF_PARTITION_REPLICATION_RECORDS_COUNT.addAndGet(this, delta);
    }

    @Override
    public String toString() {
        return "LocalReplicationStats{" + "fullPartitionReplicationCount=" + fullPartitionReplicationCount
                + ", differentialPartitionReplicationCount=" + diffPartitionReplicationCount
                + ", fullPartitionReplicationRecordsCount="
                + fullPartitionReplicationRecordsCount
                + ", differentialPartitionReplicationRecordsCount="
                + diffPartitionReplicationRecordsCount + '}';
    }
}
