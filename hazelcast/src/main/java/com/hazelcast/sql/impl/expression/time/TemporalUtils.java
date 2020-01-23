/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.time;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.type.DataType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;

public final class TemporalUtils {
    /** Default timestamp precision (nanos). */
    public static final int NANO_PRECISION = 9;

    /** Number of nanoseconds in a second. */
    public static final int NANO_IN_SECONDS = 1_000_000_000;

    /** Order multiplication step. */
    private static final int MULTIPLY_STEP = 10;

    private static final int[] PRECISION_DIVISORS = new int[NANO_PRECISION + 1];

    static {
        int divisor = 1;

        for (int i = NANO_PRECISION; i >= 0; i--) {
            PRECISION_DIVISORS[i] = divisor;

            divisor *= MULTIPLY_STEP;
        }
    }

    private TemporalUtils() {
        // No-op.
    }

    /**
     * Convert provided string to any supported date/time object.
     *
     * @param input Input.
     * @return Mathing date/time.
     */
    public static TemporalValue parseAny(String input) {
        if (input == null) {
            return null;
        }

        try {
            if (input.contains("T") || input.contains("t")) {
                // Looks like it is a timestamp.
                if (input.contains("+")) {
                    // Time zone present.
                    return new TemporalValue(
                        DataType.TIMESTAMP_WITH_TIMEZONE_OFFSET_DATE_TIME,
                        OffsetDateTime.parse(input)
                    );
                } else {
                    // No time zone.
                    return new TemporalValue(DataType.TIMESTAMP, LocalDateTime.parse(input));
                }
            } else if (input.contains("-")) {
                // Looks like it is a date.
                return new TemporalValue(DataType.DATE, LocalDate.parse(input));
            } else {
                // Otherwise it is a time.
                return new TemporalValue(DataType.TIME, LocalTime.parse(input));
            }
        } catch (DateTimeParseException ignore) {
            throw new HazelcastSqlException(-1, "Failed to parse a string to DATE/TIME: " + input);
        }
    }

    public static int getDivisorForPrecision(int precision) {
        assert precision >= 0 && precision <= NANO_PRECISION;

        return PRECISION_DIVISORS[precision];
    }
}
