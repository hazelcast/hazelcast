package com.hazelcast.internal.tpc.util;

import static com.hazelcast.internal.tpc.util.Preconditions.checkPositive;

/**
 * A {@link NanoClock} that doesn't use the {@link System#nanoTime()} for every call to {@link #nanoTime()} to
 * reduce cost, but it will cache the result of previous calls for some iterations.
 * <p>
 * This class is not thread-safe.
 */
public class CachableNanoClock implements NanoClock {
    private static final long START_TIME = System.nanoTime();
    private final int refreshPeriod;
    private int iteration;
    private long value;

    public CachableNanoClock(int refreshPeriod) {
        this.refreshPeriod = checkPositive(refreshPeriod, "refreshPeriod");
    }

    @Override
    public long nanoTime() {
        if (iteration == refreshPeriod) {
            value = System.nanoTime() - START_TIME;
            iteration = 0;
            return value;
        } else {
            iteration++;
            return value;
        }
    }
}
