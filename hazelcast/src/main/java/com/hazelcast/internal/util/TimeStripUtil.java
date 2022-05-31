/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.impl.record.Record;

import static com.hazelcast.internal.util.TimeUtil.zeroOutMs;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Contains millis to seconds, second to millis
 * conversions based on a fixed epoch time.
 */
public final class TimeStripUtil {

    /**
     * Base time to be used for storing time values as diffs
     * (int) rather than full-blown epoch based values (long)
     * This allows for a space in seconds, of roughly 68 years.
     * <p>
     * Reference value (1514764800000) -
     * Monday, January 1, 2018 12:00:00 AM
     * <p>
     * The fixed time in the past (instead of {@link
     * System#currentTimeMillis()} prevents any time
     * discrepancies among nodes, mis-translated as
     * diffs of -1 ie. {@link Record#UNSET} values.
     * <p>
     * (see:
     * https://github.com/hazelcast/hazelcast-enterprise/issues/2527)
     */
    public static final long EPOCH_TIME_MILLIS = zeroOutMs(1514764800000L);
    private static final int UNSET = -1;

    private TimeStripUtil() {
    }

    public static int stripBaseTime(long millis) {
        if (millis == Long.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }

        if (millis > 0) {
            long toSeconds = MILLISECONDS.toSeconds(millis - EPOCH_TIME_MILLIS);
            return toSeconds >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) toSeconds;
        }

        return UNSET;
    }

    public static long recomputeWithBaseTime(int seconds) {
        if (seconds == UNSET) {
            return 0L;
        }

        if (seconds == Integer.MAX_VALUE) {
            return Long.MAX_VALUE;
        }

        return EPOCH_TIME_MILLIS + SECONDS.toMillis(seconds);
    }
}
