/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.datetime;

import java.time.OffsetDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.WeekFields;

public enum ExtractField {
    CENTURY {
        @Override
        public double extract(OffsetDateTime time) {
            int year = time.getYear();
            int century = (year / YEARS_IN_CENTURY) + FIRST_CENTURY;
            return century;
        }
    },
    DAY {
        @Override
        public double extract(OffsetDateTime time) {
            return time.get(ChronoField.DAY_OF_MONTH);
        }
    },
    DECADE {
        @Override
        public double extract(OffsetDateTime time) {
            int year = time.getYear();
            int decade = year / YEARS_IN_DECADE;
            return decade;
        }
    },
    DOW {
        @Override
        public double extract(OffsetDateTime time) {
            int dayOfWeekUnadjusted = time.get(WeekFields.SUNDAY_START.dayOfWeek());
            return dayOfWeekUnadjusted - FIRST_DAY_OF_WEEK;
        }
    },
    DOY {
        @Override
        public double extract(OffsetDateTime time) {
            return time.getDayOfYear();
        }
    },
    EPOCH {
        @Override
        public double extract(OffsetDateTime time) {
            long secondsSince =  time.getLong(ChronoField.INSTANT_SECONDS);
            int carryNanoseconds = time.get(ChronoField.NANO_OF_SECOND);
            double carrySeconds = ((double) carryNanoseconds / (double) NANOSECONDS_IN_SECOND);
            return carrySeconds + secondsSince;
        }
    },
    HOUR {
        @Override
        public double extract(OffsetDateTime time) {
            return time.getHour();
        }
    },
    ISODOW {
        @Override
        public double extract(OffsetDateTime time) {
            return time.get(WeekFields.ISO.dayOfWeek());
        }
    },
    ISOYEAR {
        @Override
        public double extract(OffsetDateTime time) {
            int isoWeek = time.get(WeekFields.ISO.weekBasedYear());
            return isoWeek;
        }
    },
    MICROSECOND {
        @Override
        public double extract(OffsetDateTime time) {
            int fractionMicroseconds = time.get(ChronoField.MICRO_OF_SECOND);
            int seconds = time.getSecond();
            return fractionMicroseconds + (seconds * MICROSECONDS_IN_SECOND);
        }
    },
    MILLENNIUM {
        @Override
        public double extract(OffsetDateTime time) {
            int year = time.getYear();
            int millennium = (year / YEARS_IN_MILLENNIUM) + FIRST_MILLENNIUM;
            return millennium;
        }
    },
    MILLISECOND {
        @Override
        public double extract(OffsetDateTime time) {
            int fractionMilliseconds = time.get(ChronoField.MILLI_OF_SECOND);
            int seconds = time.getSecond();
            return fractionMilliseconds + (seconds * MILLISECONDS_IN_SECOND);
        }
    },
    MINUTE {
        @Override
        public double extract(OffsetDateTime time) {
            return time.getMinute();
        }
    },
    MONTH {
        @Override
        public double extract(OffsetDateTime time) {
            return time.getMonthValue();
        }
    },
    QUARTER {
        @Override
        public double extract(OffsetDateTime time) {
            return time.get(IsoFields.QUARTER_OF_YEAR);
        }
    },
    SECOND {
        @Override
        public double extract(OffsetDateTime time) {
            return time.getSecond();
        }
    },
    WEEK {
        @Override
        public double extract(OffsetDateTime time) {
            return time.get(WeekFields.ISO.weekOfWeekBasedYear());
        }
    },
    YEAR {
        @Override
        public double extract(OffsetDateTime time) {
            return time.getYear();
        }
    };

    protected static final int YEARS_IN_MILLENNIUM = 1000;

    protected static final int YEARS_IN_CENTURY = 100;

    protected static final int YEARS_IN_DECADE = 10;

    protected static final int NANOSECONDS_IN_SECOND = 1_000_000_000;

    protected static final int MICROSECONDS_IN_SECOND = 1_000_000;

    protected static final int MILLISECONDS_IN_SECOND = 1_000;

    protected static final int FIRST_MILLENNIUM = 1;

    protected static final int FIRST_CENTURY = 1;

    protected static final int FIRST_DAY_OF_WEEK = 1;

    public abstract double extract(OffsetDateTime time);
}
