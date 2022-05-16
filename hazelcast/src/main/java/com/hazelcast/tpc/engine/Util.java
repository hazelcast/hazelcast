package com.hazelcast.tpc.engine;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class Util {

    /**
     * Gets the number of nanoseconds from the Java epoch of 1970-01-01T00:00:00Z.
     *
     * @return the epoch time in nanoseconds.
     */
    public static long epochNanos() {

        //todo: litter
        Instant now = Instant.now();
        long seconds = now.getEpochSecond();
        long nanosFromSecond = now.getNano();

        return (seconds * 1_000_000_000) + nanosFromSecond;
    }
}
