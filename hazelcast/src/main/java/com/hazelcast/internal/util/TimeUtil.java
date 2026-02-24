/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Utility class for Time &amp; unit conversions
 */
public final class TimeUtil {

    private static final int NANOS_PER_MICROSECOND = 1000;
    private static final int MILLIS_PER_SECOND = 1000;
    private static final int SECONDS_PER_MINUTE = 60;
    private static final int MINUTES_PER_HOUR = 60;
    private static final int HOURS_PER_DAY = 24;
    private static final int BUILDER_CAPACITY = 32;

    private TimeUtil() {
    }

    /**
     * Convert nanoseconds to milliseconds
     * If conversion result is 0 and {@code nanos} was &gt; 0, then 1 is returned.
     *
     * @param nanos The number of nanoseconds
     * @return The millisecond representation of the input
     */
    public static long convertNanosToMillis(long nanos) {
        return timeInMsOrOneIfResultIsZero(nanos, NANOSECONDS);
    }

    /**
     * Convert milliseconds to nanoseconds
     *
     * @param millis The number of milliseconds
     * @return The nanoseconds representation of the input
     */
    public static long convertMillisToNanos(long millis) {
        return MILLISECONDS.toNanos(millis);
    }

    /**
     * Convert time to milliseconds based on the given time unit.
     * If conversion result is 0 and {@code time} was &gt; 0, then 1 is returned.
     *
     * @param time     The input time
     * @param timeUnit The time unit to base the conversion on
     * @return The millisecond representation of the input time
     * @throws NullPointerException if time unit is <code>null</code>
     */
    public static long timeInMsOrOneIfResultIsZero(long time, TimeUnit timeUnit) {
        long timeInMillis = timeUnit.toMillis(time);
        if (time > 0 && timeInMillis == 0) {
            timeInMillis = 1;
        }

        return timeInMillis;
    }

    /**
     * Convert time to milliseconds based on the given time unit.
     * If time unit is null, then input time is treated as milliseconds.
     *
     * @param time     The input time
     * @param timeUnit The time unit to base the conversion on
     * @return The millisecond representation of the time based on the unit, or the time
     * itself if the unit is <code>null</code>
     */
    public static long timeInMsOrTimeIfNullUnit(long time, TimeUnit timeUnit) {
        return timeUnit != null ? timeUnit.toMillis(time) : time;
    }

    /**
     * Cleans the milliseconds info from the timestamp
     *
     * @param timestamp The input timestamp
     * @return The timestamp with the milliseconds field filled with zeros
     */
    public static long zeroOutMs(long timestamp) {
        return SECONDS.toMillis(MILLISECONDS.toSeconds(timestamp));
    }

    public static String toHumanReadableNanos(long nanos) {
        if (nanos < NANOS_PER_MICROSECOND) {
            return nanos + "ns";
        }

        long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        long remainingNanos = nanos - TimeUnit.MILLISECONDS.toNanos(millis);

        // If no full millisecond, just nanos
        if (millis == 0) {
            return remainingNanos + "ns";
        }

        long seconds = millis / MILLIS_PER_SECOND;
        long ms = millis % MILLIS_PER_SECOND;

        long minutes = seconds / SECONDS_PER_MINUTE;
        long s = seconds % SECONDS_PER_MINUTE;

        long hours = minutes / MINUTES_PER_HOUR;
        long m = minutes % MINUTES_PER_HOUR;

        long days = hours / HOURS_PER_DAY;
        long h = hours % HOURS_PER_DAY;

        StringBuilder sb = new StringBuilder(BUILDER_CAPACITY);

        appendIfNonZero(sb, days, "d");
        appendIfNonZero(sb, h, "h");
        appendIfNonZero(sb, m, "m");
        appendIfNonZero(sb, s, "s");
        appendIfNonZero(sb, ms, "ms");

        if (remainingNanos > 0) {
            appendIfNonZero(sb, remainingNanos, "ns");
        }

        return sb.isEmpty() ? "0ms" : sb.toString().trim();
    }

    private static void appendIfNonZero(StringBuilder sb, long value, String suffix) {
        if (value > 0) {
            if (!sb.isEmpty()) {
                sb.append(' ');
            }
            sb.append(value).append(suffix);
        }
    }
}
