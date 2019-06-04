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

package com.hazelcast.util;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Utility class for Time &amp; unit conversions
 */
public final class TimeUtil {

    private TimeUtil() { }

    /**
     * Convert time to milliseconds based on the given time unit.
     * If conversion result is 0 and {@code time} was &gt; 0, then 1 is returned.
     *
     * @param time The input time
     * @param timeUnit The time unit to base the conversion on
     *
     * @throws NullPointerException if time unit is <code>null</code>
     * @return The millisecond representation of the input time
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
     * @param time The input time
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
}
