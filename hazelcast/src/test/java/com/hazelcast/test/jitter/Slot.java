package com.hazelcast.test.jitter;

import java.text.DateFormat;
import java.util.Date;

import static com.hazelcast.test.jitter.JitterRule.LONG_HICCUP_THRESHOLD;
import static java.lang.Math.max;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class Slot {
    private final long startInterval;

    private volatile long accumulatedHiccupsNanos;
    private volatile long maxPauseNanos;
    private volatile  int pausesOverThreshold;

    public Slot(long startInterval) {
        this.startInterval = startInterval;
    }

    public long getStartIntervalMillis() {
        return startInterval;
    }

    public long getMaxPauseNanos() {
        return maxPauseNanos;
    }

    public long getAccumulatedHiccupsNanos() {
        return accumulatedHiccupsNanos;
    }

    public void recordHiccup(long hiccupNanos) {
        accumulatedHiccupsNanos += hiccupNanos;
        maxPauseNanos = max(maxPauseNanos, hiccupNanos);
        if (hiccupNanos > LONG_HICCUP_THRESHOLD) {
            pausesOverThreshold++;
        }
    }

    public String toHumanFriendly(DateFormat dateFormat) {
        return dateFormat.format(new Date(startInterval))
                + ", accumulated pauses: " + NANOSECONDS.toMillis(accumulatedHiccupsNanos) + " ms"
                + ", max pause: " + NANOSECONDS.toMillis(maxPauseNanos) + " ms"
                + ", pauses over " + NANOSECONDS.toMillis(LONG_HICCUP_THRESHOLD) + " ms: " + pausesOverThreshold;
    }
}
