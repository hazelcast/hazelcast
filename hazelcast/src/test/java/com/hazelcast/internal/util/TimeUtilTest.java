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

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TimeUtilTest {

    @Test
    void whenLessThanOneMicrosecond_thenReturnNanosOnly() {
        assertEquals("0ns", TimeUtil.toHumanReadableNanos(0));
        assertEquals("1ns", TimeUtil.toHumanReadableNanos(1));
        assertEquals("999ns", TimeUtil.toHumanReadableNanos(999));
    }

    @Test
    void whenExactlyOneMicrosecond_thenStillNanos() {
        // 1 microsecond = 1000ns, but method switches based on millis,
        // so this still ends up as nanos only
        assertEquals("1000ns", TimeUtil.toHumanReadableNanos(1_000));
    }

    @Test
    void whenLessThanOneMillisecond_thenReturnNanos() {
        assertEquals("999999ns", TimeUtil.toHumanReadableNanos(999_999));
    }

    @Test
    void whenExactlyOneMillisecond_thenReturnMilliseconds() {
        assertEquals("1ms", TimeUtil.toHumanReadableNanos(TimeUnit.MILLISECONDS.toNanos(1)));
    }

    @Test
    void whenMillisecondsWithRemainingNanos_thenIncludeBoth() {
        long nanos = TimeUnit.MILLISECONDS.toNanos(1) + 123;
        assertEquals("1ms 123ns", TimeUtil.toHumanReadableNanos(nanos));
    }

    @Test
    void whenSecondsOnly_thenReturnSeconds() {
        assertEquals("1s", TimeUtil.toHumanReadableNanos(TimeUnit.SECONDS.toNanos(1)));
        assertEquals("5s", TimeUtil.toHumanReadableNanos(TimeUnit.SECONDS.toNanos(5)));
    }

    @Test
    void whenSecondsAndMilliseconds_thenIncludeBoth() {
        long nanos = TimeUnit.SECONDS.toNanos(2) + TimeUnit.MILLISECONDS.toNanos(250);
        assertEquals("2s 250ms", TimeUtil.toHumanReadableNanos(nanos));
    }

    @Test
    void whenMinutesSecondsAndMillis_thenIncludeAllNonZeroUnits() {
        long nanos =
                TimeUnit.MINUTES.toNanos(1)
                        + TimeUnit.SECONDS.toNanos(2)
                        + TimeUnit.MILLISECONDS.toNanos(3);

        assertEquals("1m 2s 3ms", TimeUtil.toHumanReadableNanos(nanos));
    }

    @Test
    void whenHoursMinutesSeconds_thenIncludeAll() {
        long nanos =
                TimeUnit.HOURS.toNanos(1)
                        + TimeUnit.MINUTES.toNanos(2)
                        + TimeUnit.SECONDS.toNanos(3);

        assertEquals("1h 2m 3s", TimeUtil.toHumanReadableNanos(nanos));
    }

    @Test
    void whenDaysHoursMinutesSecondsMillis_thenIncludeAll() {
        long nanos =
                TimeUnit.DAYS.toNanos(1)
                        + TimeUnit.HOURS.toNanos(2)
                        + TimeUnit.MINUTES.toNanos(3)
                        + TimeUnit.SECONDS.toNanos(4)
                        + TimeUnit.MILLISECONDS.toNanos(5);

        assertEquals("1d 2h 3m 4s 5ms", TimeUtil.toHumanReadableNanos(nanos));
    }

    @Test
    void whenAllUnitsIncludingNanos_thenIncludeNanosAtEnd() {
        long nanos =
                TimeUnit.SECONDS.toNanos(1)
                        + TimeUnit.MILLISECONDS.toNanos(2)
                        + 31111;

        assertEquals("1s 2ms 31111ns", TimeUtil.toHumanReadableNanos(nanos));
    }

    @Test
    void whenOnlyZeroMillisAfterConversion_thenReturnZeroMs() {
        assertEquals("0ns", TimeUtil.toHumanReadableNanos(0));
    }
}
