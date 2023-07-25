package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.util.Timer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;

/**
 * the total elapsed time of executions from source to destination endpoints
 */
public class MigrationTimer {
    /**
     * on the latest repartitioning round.
     */
    private final LongAccumulator elapsedNanoseconds = new LongAccumulator(Long::max, 0);

    /**
     * the sum of all previous rounds, except the latest, since the beginning.
     */
    private volatile long lastTotalElapsedNanoseconds;

    protected void markNewRepartition() {
        lastTotalElapsedNanoseconds = getTotalElapsedNanoseconds();
        elapsedNanoseconds.reset();
    }

    protected void calculateElapsed(final long lastRepartitionNanos) {
        elapsedNanoseconds.accumulate(Timer.nanosElapsed(lastRepartitionNanos));
    }

    /**
     * @return nanoseconds
     * @see #elapsedNanoseconds
     */
    public long getElapsedNanoseconds() {
        return elapsedNanoseconds.get();
    }

    /**
     * @return milliseconds
     * @see #elapsedNanoseconds
     */
    public long getElapsedMilliseconds() {
        return TimeUnit.NANOSECONDS.toMillis(getElapsedNanoseconds());
    }

    /**
     * @return the sum of all previous rounds, since the beginning - nanoseconds
     */
    public long getTotalElapsedNanoseconds() {
        return lastTotalElapsedNanoseconds + getElapsedNanoseconds();
    }

    /**
     * @return the sum of all previous rounds, since the beginning - milliseconds
     */
    public long getTotalElapsedMilliseconds() {
        return TimeUnit.NANOSECONDS.toMillis(getTotalElapsedNanoseconds());
    }
}
