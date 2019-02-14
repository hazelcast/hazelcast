/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.util.Clock;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collection of stats for partition migration tasks.
 */
public class MigrationStats {

    @Probe
    private final AtomicLong lastRepartitionTime = new AtomicLong();

    @Probe
    private final AtomicLong completedMigrations = new AtomicLong();

    @Probe
    private final AtomicLong totalCompletedMigrations = new AtomicLong();

    @Probe
    private final AtomicLong elapsedMigrationOperationTime = new AtomicLong();

    @Probe
    private final AtomicLong elapsedDestinationCommitTime = new AtomicLong();

    @Probe
    private final AtomicLong elapsedMigrationTime = new AtomicLong();

    @Probe
    private final AtomicLong totalElapsedMigrationOperationTime = new AtomicLong();

    @Probe
    private final AtomicLong totalElapsedDestinationCommitTime = new AtomicLong();

    @Probe
    private final AtomicLong totalElapsedMigrationTime = new AtomicLong();

    /**
     * Marks start of new repartitioning.
     * Resets stats from previous repartitioning round.
     */
    void markNewRepartition() {
        lastRepartitionTime.set(Clock.currentTimeMillis());
        elapsedMigrationOperationTime.set(0);
        elapsedDestinationCommitTime.set(0);
        elapsedMigrationTime.set(0);
        completedMigrations.set(0);
    }

    void incrementCompletedMigrations() {
        completedMigrations.incrementAndGet();
        totalCompletedMigrations.incrementAndGet();
    }

    void recordMigrationOperationTime(long time) {
        elapsedMigrationOperationTime.addAndGet(time);
        totalElapsedMigrationOperationTime.addAndGet(time);
    }

    void recordDestinationCommitTime(long time) {
        elapsedDestinationCommitTime.addAndGet(time);
        totalElapsedDestinationCommitTime.addAndGet(time);
    }

    void recordMigrationTaskTime(long time) {
        elapsedMigrationTime.addAndGet(time);
        totalElapsedMigrationTime.addAndGet(time);
    }

    /**
     * Returns the last repartition time.
     */
    public Date getLastRepartitionTime() {
        return new Date(lastRepartitionTime.get());
    }

    /**
     * Returns the number of completed migrations on the latest repartitioning round.
     */
    public long getCompletedMigrations() {
        return completedMigrations.get();
    }

    /**
     * Returns the total number of completed migrations since the beginning.
     */
    public long getTotalCompletedMigrations() {
        return totalCompletedMigrations.get();
    }

    /**
     * Returns the total elapsed time of migration & replication operations' executions
     * from source to destination endpoints, in milliseconds, on the latest repartitioning round.
     */
    public long getElapsedMigrationOperationTime() {
        return TimeUnit.NANOSECONDS.toMillis(elapsedMigrationOperationTime.get());
    }

    /**
     * Returns the total elapsed time of commit operations' executions to the destination endpoint,
     * in milliseconds, on the latest repartitioning round.
     */
    public long getElapsedDestinationCommitTime() {
        return TimeUnit.NANOSECONDS.toMillis(elapsedDestinationCommitTime.get());
    }

    /**
     * Returns the total elapsed time from start of migration tasks to their completion,
     * in milliseconds, on the latest repartitioning round.
     */
    public long getElapsedMigrationTime() {
        return TimeUnit.NANOSECONDS.toMillis(elapsedMigrationTime.get());
    }

    /**
     * Returns the total elapsed time of migration & replication operations' executions
     * from source to destination endpoints, in milliseconds, since the beginning.
     */
    public long getTotalElapsedMigrationOperationTime() {
        return TimeUnit.NANOSECONDS.toMillis(totalElapsedMigrationOperationTime.get());
    }

    /**
     * Returns the total elapsed time of commit operations' executions to the destination endpoint,
     * in milliseconds, since the beginning.
     */
    public long getTotalElapsedDestinationCommitTime() {
        return TimeUnit.NANOSECONDS.toMillis(totalElapsedDestinationCommitTime.get());
    }

    /**
     * Returns the total elapsed time from start of migration tasks to their completion,
     * in milliseconds, since the beginning.
     */
    public long getTotalElapsedMigrationTime() {
        return TimeUnit.NANOSECONDS.toMillis(totalElapsedMigrationTime.get());
    }

    public String formatToString(boolean detailed) {
        StringBuilder s = new StringBuilder();
        s.append("lastRepartitionTime=").append(getLastRepartitionTime())
                .append(", completedMigrations=").append(getCompletedMigrations())
                .append(", totalCompletedMigrations=").append(getTotalCompletedMigrations());

        if (detailed) {
            s.append(", elapsedMigrationOperationTime=").append(getElapsedMigrationOperationTime()).append("ms")
                    .append(", totalElapsedMigrationOperationTime=").append(getTotalElapsedMigrationOperationTime()).append("ms")
                    .append(", elapsedDestinationCommitTime=").append(getElapsedDestinationCommitTime()).append("ms")
                    .append(", totalElapsedDestinationCommitTime=").append(getTotalElapsedDestinationCommitTime()).append("ms");
        }

        s.append(", elapsedMigrationTime=").append(getElapsedMigrationTime()).append("ms")
                .append(", totalElapsedMigrationTime=").append(getTotalElapsedMigrationTime()).append("ms");
        return s.toString();
    }

    @Override
    public String toString() {
        return "MigrationStats{" + formatToString(true) + "}";
    }
}
