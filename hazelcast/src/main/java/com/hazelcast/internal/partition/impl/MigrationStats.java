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
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.partition.MigrationStateImpl;
import com.hazelcast.partition.MigrationState;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collection of stats for partition migration tasks.
 */
public class MigrationStats {

    @Probe
    private final AtomicLong lastRepartitionTime = new AtomicLong();

    @Probe
    private volatile int plannedMigrations;

    @Probe
    private final AtomicInteger completedMigrations = new AtomicInteger();

    @Probe
    private final AtomicInteger totalCompletedMigrations = new AtomicInteger();

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
     * @param migrations number of planned migration tasks
     */
    void markNewRepartition(int migrations) {
        lastRepartitionTime.set(Clock.currentTimeMillis());
        plannedMigrations = migrations;
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
     * Returns the number of planned migrations on the latest repartitioning round.
     */
    public int getPlannedMigrations() {
        return plannedMigrations;
    }

    /**
     * Returns the number of completed migrations on the latest repartitioning round.
     */
    public int getCompletedMigrations() {
        return completedMigrations.get();
    }

    /**
     * Returns the number of remaining migrations on the latest repartitioning round.
     */
    public int getRemainingMigrations() {
        return plannedMigrations - completedMigrations.get();
    }

    /**
     * Returns the total number of completed migrations since the beginning.
     */
    public int getTotalCompletedMigrations() {
        return totalCompletedMigrations.get();
    }

    /**
     * Returns the total elapsed time of migration &amp; replication operations' executions
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
     * Returns the total elapsed time of migration &amp; replication operations' executions
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

    public MigrationState toMigrationState() {
        return new MigrationStateImpl(lastRepartitionTime.get(), plannedMigrations,
                completedMigrations.get(), getElapsedMigrationTime());
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
