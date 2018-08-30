/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
 * Utility class for Time & unit conversions
 */
public final class TimeUtil {

    private TimeUtil() { }

    /**
     * Convert time to milliseconds based on the input time-unit.
     * If conversion results in the value 0, then it the value is replaced with the positive 1.
     *
     * @param time The input time
     * @param timeunit The input time-unit to base the conversion on
     *
     * @throws NullPointerException if timeunit is <code>null</code>
     * @return The milliseconds representation of the input time
     */
    public static long timeInMsOrOneIfResultIsZero(long time, TimeUnit timeunit) {
        long timeInMillis = timeunit.toMillis(time);
        if (time > 0 && timeInMillis == 0) {
            timeInMillis = 1;
        }

        return timeInMillis;
    }

    /**
     * Convert time to milliseconds based on the input time-unit.
     * If time-unit is null, then input time is treated as milliseconds
     *
     * @param time The input time
     * @param timeunit The input time-unit to base the conversion on
     * @return The milliseconds representation of the time based on the unit, or the time
     * itself if unit is <code>null</code>
     */
    public static long timeInMsOrTimeIfNullUnit(long time, TimeUnit timeunit) {
        return timeunit != null ? timeunit.toMillis(time) : time;
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
