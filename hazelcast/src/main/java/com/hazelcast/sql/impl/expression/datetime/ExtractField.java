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

package com.hazelcast.sql.impl.expression.datetime;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoField;
import java.time.temporal.IsoFields;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;

public enum ExtractField {
    CENTURY {
        @Override
        public double extract(OffsetDateTime time) {
            return extractCentury(time);
        }

        @Override
        public double extract(LocalDateTime time) {
            return extractCentury(time);
        }

        @Override
        public double extract(LocalDate date) {
            return extractCentury(date);
        }

        @Override
        public double extract(LocalTime time) {
            throw new IllegalArgumentException("Cannot extract CENTURY from TIME");
        }

        private double extractCentury(Temporal temporal) {
            int year = temporal.get(ChronoField.YEAR);
            int absYear = Math.abs(year);
            int sign = year < 0 ? -1 : 1;
            int quotient = absYear / YEARS_IN_CENTURY;
            int remainder = absYear % YEARS_IN_CENTURY;
            int absCentury = quotient + (remainder == 0 ? -1 : 0) + FIRST_CENTURY;
            return sign * absCentury;
        }
    },
    DAY {
        @Override
        public double extract(OffsetDateTime time) {
            return time.getDayOfMonth();
        }

        @Override
        public double extract(LocalDateTime time) {
            return time.getDayOfMonth();
        }

        @Override
        public double extract(LocalDate date) {
            return date.getDayOfMonth();
        }

        @Override
        public double extract(LocalTime time) {
            throw new IllegalArgumentException("Cannot extract DAY from TIME");
        }
    },
    DECADE {
        @Override
        public double extract(OffsetDateTime time) {
            return extractDecade(time);
        }

        @Override
        public double extract(LocalDateTime time) {
            return extractDecade(time);
        }

        @Override
        public double extract(LocalDate date) {
            return extractDecade(date);
        }

        @Override
        public double extract(LocalTime time) {
            throw new IllegalArgumentException("Cannot extract DECADE from TIME");
        }

        private double extractDecade(Temporal temporal) {
            int year = temporal.get(ChronoField.YEAR);
            int decade = year / YEARS_IN_DECADE;
            return decade;
        }
    },
    DOW {
        @Override
        public double extract(OffsetDateTime time) {
            return extractDow(time);
        }

        @Override
        public double extract(LocalDateTime time) {
            return extractDow(time);
        }

        @Override
        public double extract(LocalDate date) {
            return extractDow(date);
        }

        @Override
        public double extract(LocalTime time) {
            throw new IllegalArgumentException("Cannot extract DOW from TIME");
        }

        private double extractDow(Temporal temporal) {
            int dayOfWeekUnadjusted = temporal.get(WeekFields.SUNDAY_START.dayOfWeek());
            return dayOfWeekUnadjusted - FIRST_DAY_OF_WEEK;
        }
    },
    DOY {
        @Override
        public double extract(OffsetDateTime time) {
            return time.getDayOfYear();
        }

        @Override
        public double extract(LocalDateTime time) {
            return time.getDayOfYear();
        }

        @Override
        public double extract(LocalDate date) {
            return date.getDayOfYear();
        }

        @Override
        public double extract(LocalTime time) {
            throw new IllegalArgumentException("Cannot extract DOY from TIME");
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

        @Override
        public double extract(LocalDateTime time) {
            return EPOCH.extract(OffsetDateTime.of(time, ZoneOffset.UTC));
        }

        @Override
        public double extract(LocalDate date) {
            return EPOCH.extract(LocalDateTime.of(date, LocalTime.MIDNIGHT));
        }

        @Override
        public double extract(LocalTime time) {
            return EPOCH.extract(LocalDateTime.of(LocalDate.ofEpochDay(0), time));
        }
    },
    HOUR {
        @Override
        public double extract(OffsetDateTime time) {
            return time.getHour();
        }

        @Override
        public double extract(LocalDateTime time) {
            return time.getHour();
        }

        @Override
        public double extract(LocalDate date) {
            return 0;
        }

        @Override
        public double extract(LocalTime time) {
            return time.getHour();
        }
    },
    ISODOW {
        @Override
        public double extract(OffsetDateTime time) {
            return time.get(ISODOW_FIELD);
        }

        @Override
        public double extract(LocalDateTime time) {
            return time.get(ISODOW_FIELD);
        }

        @Override
        public double extract(LocalDate date) {
            return date.get(ISODOW_FIELD);
        }

        @Override
        public double extract(LocalTime time) {
            throw new IllegalArgumentException("Cannot extract ISODOW from TIME");
        }
    },
    ISOYEAR {
        @Override
        public double extract(OffsetDateTime time) {
            return time.get(ISOYEAR_FIELD);
        }

        @Override
        public double extract(LocalDateTime time) {
            return time.get(ISOYEAR_FIELD);
        }

        @Override
        public double extract(LocalDate date) {
            return date.get(ISOYEAR_FIELD);
        }

        @Override
        public double extract(LocalTime time) {
            throw new IllegalArgumentException("Cannot extract ISOYEAR from TIME");
        }
    },
    MICROSECOND {
        @Override
        public double extract(OffsetDateTime time) {
            return extractMicrosecond(time);
        }

        @Override
        public double extract(LocalDateTime time) {
            return extractMicrosecond(time);
        }

        @Override
        public double extract(LocalDate date) {
            return 0;
        }

        @Override
        public double extract(LocalTime time) {
            return extractMicrosecond(time);
        }

        private double extractMicrosecond(Temporal temporal) {
            int fractionMicroseconds = temporal.get(ChronoField.MICRO_OF_SECOND);
            int seconds = temporal.get(ChronoField.SECOND_OF_MINUTE);
            return fractionMicroseconds + (seconds * MICROSECONDS_IN_SECOND);
        }
    },
    MILLENNIUM {
        @Override
        public double extract(OffsetDateTime time) {
            return extractMillennium(time);
        }

        @Override
        public double extract(LocalDateTime time) {
            return extractMillennium(time);
        }

        @Override
        public double extract(LocalDate date) {
            return extractMillennium(date);
        }

        @Override
        public double extract(LocalTime time) {
            throw new IllegalArgumentException("Cannot extract MILLENNIUM from TIME");
        }

        private double extractMillennium(Temporal temporal) {
            int year = temporal.get(ChronoField.YEAR);
            int absYear = Math.abs(year);
            int sign = year < 0 ? -1 : 1;
            int quotient = absYear / YEARS_IN_MILLENNIUM;
            int remainder = absYear % YEARS_IN_MILLENNIUM;
            int absMillennium = quotient + (remainder == 0 ? -1 : 0) + FIRST_MILLENNIUM;
            return sign * absMillennium;
        }
    },
    MILLISECOND {
        @Override
        public double extract(OffsetDateTime time) {
            int fractionMilliseconds = time.get(ChronoField.MILLI_OF_SECOND);
            int seconds = time.getSecond();
            return fractionMilliseconds + (seconds * MILLISECONDS_IN_SECOND);
        }

        @Override
        public double extract(LocalDateTime time) {
            return extract(OffsetDateTime.of(time, ZoneOffset.UTC));
        }

        @Override
        public double extract(LocalDate date) {
            return 0;
        }

        @Override
        public double extract(LocalTime time) {
            return extract(LocalDateTime.of(LocalDate.ofEpochDay(0), time));
        }
    },
    MINUTE {
        @Override
        public double extract(OffsetDateTime time) {
            return time.getMinute();
        }

        @Override
        public double extract(LocalDateTime time) {
            return time.getMinute();
        }

        @Override
        public double extract(LocalDate date) {
            return 0;
        }

        @Override
        public double extract(LocalTime time) {
            return time.getMinute();
        }
    },
    MONTH {
        @Override
        public double extract(OffsetDateTime time) {
            return time.getMonthValue();
        }

        @Override
        public double extract(LocalDateTime time) {
            return time.getMonthValue();
        }

        @Override
        public double extract(LocalDate date) {
            return date.getMonthValue();
        }

        @Override
        public double extract(LocalTime time) {
            throw new IllegalArgumentException("Cannot extract MONTH from TIME");
        }
    },
    QUARTER {
        @Override
        public double extract(OffsetDateTime time) {
            return time.get(QUARTER_FIELD);
        }

        @Override
        public double extract(LocalDateTime time) {
            return time.get(QUARTER_FIELD);
        }

        @Override
        public double extract(LocalDate date) {
            return date.get(QUARTER_FIELD);
        }

        @Override
        public double extract(LocalTime time) {
            throw new IllegalArgumentException("Cannot extract QUARTER from TIME");
        }
    },
    SECOND {
        @Override
        public double extract(OffsetDateTime time) {
            return time.getSecond();
        }

        @Override
        public double extract(LocalDateTime time) {
            return time.getSecond();
        }

        @Override
        public double extract(LocalDate date) {
            return 0;
        }

        @Override
        public double extract(LocalTime time) {
            return time.getSecond();
        }
    },
    WEEK {
        @Override
        public double extract(OffsetDateTime time) {
            return time.get(WEEK_FIELD);
        }

        @Override
        public double extract(LocalDateTime time) {
            return time.get(WEEK_FIELD);
        }

        @Override
        public double extract(LocalDate date) {
            return date.get(WEEK_FIELD);
        }

        @Override
        public double extract(LocalTime time) {
            throw new IllegalArgumentException("Cannot extract WEEK from TIME");
        }
    },
    YEAR {
        @Override
        public double extract(OffsetDateTime time) {
            return time.getYear();
        }

        @Override
        public double extract(LocalDateTime time) {
            return time.getYear();
        }

        @Override
        public double extract(LocalDate date) {
            return date.getYear();
        }

        @Override
        public double extract(LocalTime time) {
            throw new IllegalArgumentException("Cannot extract YEAR from TIME");
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

    protected static final TemporalField WEEK_FIELD = WeekFields.ISO.weekOfWeekBasedYear();

    protected static final TemporalField QUARTER_FIELD = IsoFields.QUARTER_OF_YEAR;

    protected static final TemporalField ISOYEAR_FIELD = WeekFields.ISO.weekBasedYear();

    protected static final TemporalField ISODOW_FIELD = WeekFields.ISO.dayOfWeek();

    public abstract double extract(OffsetDateTime time);
    public abstract double extract(LocalDateTime time);
    public abstract double extract(LocalDate date);
    public abstract double extract(LocalTime time);
}
