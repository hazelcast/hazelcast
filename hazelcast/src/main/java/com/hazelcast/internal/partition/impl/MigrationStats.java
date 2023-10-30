/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.MigrationStateImpl;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.partition.MigrationState;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MIGRATION_METRIC_COMPLETED_MIGRATIONS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MIGRATION_METRIC_ELAPSED_DESTINATION_COMMIT_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MIGRATION_METRIC_ELAPSED_MIGRATION_OPERATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MIGRATION_METRIC_ELAPSED_MIGRATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MIGRATION_METRIC_LAST_REPARTITION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MIGRATION_METRIC_PLANNED_MIGRATIONS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MIGRATION_METRIC_TOTAL_COMPLETED_MIGRATIONS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MIGRATION_METRIC_TOTAL_ELAPSED_DESTINATION_COMMIT_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MIGRATION_METRIC_TOTAL_ELAPSED_MIGRATION_OPERATION_TIME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MIGRATION_METRIC_TOTAL_ELAPSED_MIGRATION_TIME;
import static com.hazelcast.internal.metrics.ProbeUnit.MS;
import static com.hazelcast.internal.metrics.ProbeUnit.NS;

/**
 * Collection of stats for partition migration tasks.
 * <p>
 * {@code volatile long} used in preference of {@link AtomicLong} for
 * <a href="https://stackoverflow.com/a/12859121">performance reasons</a>
 */
public class MigrationStats {

    @Probe(name = MIGRATION_METRIC_LAST_REPARTITION_TIME, unit = MS)
    private volatile long lastRepartitionTime;

    /**
     * Exists only as a monotonic reference for timing executions
     *
     * @see <a href="https://github.com/hazelcast/hazelcast/pull/25028#discussion_r1269604720">Discussion</a>
     */
    private volatile long lastRepartitionNanos;

    @Probe(name = MIGRATION_METRIC_PLANNED_MIGRATIONS)
    private volatile int plannedMigrations;

    @Probe(name = MIGRATION_METRIC_COMPLETED_MIGRATIONS)
    private final LongAdder completedMigrations = new LongAdder();

    @Probe(name = MIGRATION_METRIC_TOTAL_COMPLETED_MIGRATIONS)
    private final LongAdder totalCompletedMigrations = new LongAdder();

    /**
     * elapsed time of migration &amp; replication operations' executions
     */
    private final MigrationTimer migrationOperationTime = new MigrationTimer();


    /**
     * elapsed time of commit operations' executions
     */
    private final MigrationTimer destinationCommitTime = new MigrationTimer();

    /**
     * elapsed time from start of migration tasks to their completion
     */
    private final MigrationTimer migrationTime = new MigrationTimer();

    /**
     * Marks start of new repartitioning.
     * Resets stats from previous repartitioning round.
     *
     * @param migrations number of planned migration tasks
     */
    void markNewRepartition(int migrations) {
        lastRepartitionTime = Clock.currentTimeMillis();
        lastRepartitionNanos = Timer.nanos();
        plannedMigrations = migrations;

        migrationOperationTime.markNewRepartition();
        destinationCommitTime.markNewRepartition();
        migrationTime.markNewRepartition();

        completedMigrations.reset();
    }

    void incrementCompletedMigrations() {
        completedMigrations.increment();
        totalCompletedMigrations.increment();
    }

    /**
     * @return the last repartition time.
     */
    public Date getLastRepartitionTime() {
        return new Date(lastRepartitionTime);
    }

    /**
     * @return the number of planned migrations on the latest repartitioning round.
     */
    public int getPlannedMigrations() {
        return plannedMigrations;
    }

    /**
     * @return the number of completed migrations on the latest repartitioning round.
     */
    public int getCompletedMigrations() {
        return completedMigrations.intValue();
    }

    /**
     * @return the number of remaining migrations on the latest repartitioning round.
     */
    public int getRemainingMigrations() {
        return plannedMigrations - getCompletedMigrations();
    }

    /**
     * @return the total number of completed migrations since the beginning.
     */
    public int getTotalCompletedMigrations() {
        return totalCompletedMigrations.intValue();
    }

    /**
     * @see #migrationOperationTime
     */
    void recordMigrationOperationTime() {
        migrationOperationTime.calculateElapsed(lastRepartitionNanos);
    }

    /**
     * @see #migrationOperationTime
     * @see MigrationTimer#getElapsedMilliseconds()
     */
    public long getElapsedMigrationOperationTime() {
        return migrationOperationTime.getElapsedMilliseconds();
    }

    /**
     * @see #migrationOperationTime
     * @see MigrationTimer#getElapsedNanoseconds()
     */
    @Probe(name = MIGRATION_METRIC_ELAPSED_MIGRATION_OPERATION_TIME, unit = NS)
    public long getElapsedMigrationOperationTimeNanoseconds() {
        return migrationOperationTime.getElapsedNanoseconds();
    }

    /**
     * @see #migrationOperationTime
     * @see MigrationTimer#getTotalElapsedMilliseconds()
     */
    public long getTotalElapsedMigrationOperationTime() {
        return migrationOperationTime.getTotalElapsedMilliseconds();
    }

    /**
     * @see #migrationOperationTime
     * @see MigrationTimer#getTotalElapsedNanoseconds()
     */
    @Probe(name = MIGRATION_METRIC_TOTAL_ELAPSED_MIGRATION_OPERATION_TIME, unit = NS)
    public long getTotalElapsedMigrationOperationTimeNanoseconds() {
        return migrationOperationTime.getTotalElapsedNanoseconds();
    }


    /**
     * @see #destinationCommitTime
     */
    void recordDestinationCommitTime() {
        destinationCommitTime.calculateElapsed(lastRepartitionNanos);
    }

    /**
     * @see #destinationCommitTime
     * @see MigrationTimer#getElapsedMilliseconds()
     */
    public long getElapsedDestinationCommitTime() {
        return destinationCommitTime.getElapsedMilliseconds();
    }

    /**
     * @see #destinationCommitTime
     * @see MigrationTimer#getElapsedNanoseconds()
     */
    @Probe(name = MIGRATION_METRIC_ELAPSED_DESTINATION_COMMIT_TIME, unit = NS)
    public long getElapsedDestinationCommitTimeNanoseconds() {
        return destinationCommitTime.getElapsedNanoseconds();
    }

    /**
     * @see #destinationCommitTime
     * @see MigrationTimer#getTotalElapsedMilliseconds()
     */
    public long getTotalElapsedDestinationCommitTime() {
        return destinationCommitTime.getTotalElapsedMilliseconds();
    }

    /**
     * @see #destinationCommitTime
     * @see MigrationTimer#getTotalElapsedNanoseconds()
     */
    @Probe(name = MIGRATION_METRIC_TOTAL_ELAPSED_DESTINATION_COMMIT_TIME, unit = NS)
    public long getTotalElapsedDestinationCommitTimeNanoseconds() {
        return destinationCommitTime.getTotalElapsedNanoseconds();
    }

    /**
     * @see #migrationTime
     */
    void recordMigrationTaskTime() {
        migrationTime.calculateElapsed(lastRepartitionNanos);
    }

    /**
     * @see #migrationTime
     * @see MigrationTimer#getElapsedMilliseconds()
     */
    public long getElapsedMigrationTime() {
        return migrationTime.getElapsedMilliseconds();
    }

    /**
     * @see #migrationTime
     * @see MigrationTimer#getElapsedNanoseconds()
     */
    @Probe(name = MIGRATION_METRIC_ELAPSED_MIGRATION_TIME, unit = NS)
    public long getElapsedMigrationTimeNanoseconds() {
        return migrationTime.getElapsedNanoseconds();
    }

    /**
     * @see #migrationTime
     * @see MigrationTimer#getTotalElapsedMilliseconds()
     */
    public long getTotalElapsedMigrationTime() {
        return migrationTime.getTotalElapsedMilliseconds();
    }

    /**
     * @see #migrationTime
     * @see MigrationTimer#getTotalElapsedNanoseconds()
     */
    @Probe(name = MIGRATION_METRIC_TOTAL_ELAPSED_MIGRATION_TIME, unit = NS)
    public long getTotalElapsedMigrationTimeNanoseconds() {
        return migrationTime.getTotalElapsedNanoseconds();
    }

    public MigrationState toMigrationState() {
        return new MigrationStateImpl(lastRepartitionTime, plannedMigrations,
                completedMigrations.intValue(), getElapsedMigrationTime());
    }

    public String formatToString(boolean detailed) {
        StringBuilder s = new StringBuilder();
        s.append("repartitionTime=").append(getLastRepartitionTime())
                .append(", plannedMigrations=").append(plannedMigrations)
                .append(", completedMigrations=").append(getCompletedMigrations())
                .append(", remainingMigrations=").append(getRemainingMigrations())
                .append(", totalCompletedMigrations=").append(getTotalCompletedMigrations());

        if (detailed) {
            s.append(", elapsedMigrationOperationTime=").append(getElapsedMigrationOperationTime()).append("ms")
                    .append(", totalElapsedMigrationOperationTime=").append(getTotalElapsedMigrationOperationTime()).append("ms")
                    .append(", elapsedDestinationCommitTime=").append(getElapsedDestinationCommitTime()).append("ms")
                    .append(", totalElapsedDestinationCommitTime=").append(getTotalElapsedDestinationCommitTime()).append("ms")
                    .append(", elapsedMigrationTime=").append(getElapsedMigrationTime()).append("ms")
                    .append(", totalElapsedMigrationTime=").append(getTotalElapsedMigrationTime()).append("ms");
        }
        return s.toString();
    }

    @Override
    public String toString() {
        return "MigrationStats{" + formatToString(true) + "}";
    }
}
