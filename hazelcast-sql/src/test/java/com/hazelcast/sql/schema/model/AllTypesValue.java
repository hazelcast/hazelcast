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

package com.hazelcast.sql.schema.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Objects;

/**
 * A class that has a field of every supported type in SQL.
 */
@SuppressWarnings("unused")
public final class AllTypesValue implements Serializable {

    private final String string;
    private final char character0;
    private final boolean boolean0;
    private final byte byte0;
    private final short short0;
    private final int int0;
    private final long long0;
    private final float float0;
    private final double double0;
    private final BigDecimal bigDecimal;
    private final BigInteger bigInteger;
    private final LocalTime localTime;
    private final LocalDate localDate;
    private final LocalDateTime localDateTime;
    private final Date date;
    private final GregorianCalendar calendar;
    private final Instant instant;
    private final ZonedDateTime zonedDateTime;
    private final OffsetDateTime offsetDateTime;
    // TODO: when intervals are properly supported
    /*private SqlYearMonthInterval yearMonthInterval;
    private SqlDaySecondInterval daySecondInterval;*/

    @SuppressWarnings("checkstyle:ParameterNumber")
    public AllTypesValue(String string, char character0, boolean boolean0, byte byte0, short short0, int int0, long long0,
                         float float0, double double0, BigDecimal bigDecimal, BigInteger bigInteger, LocalTime localTime,
                         LocalDate localDate, LocalDateTime localDateTime, Date date, GregorianCalendar calendar,
                         Instant instant, ZonedDateTime zonedDateTime, OffsetDateTime offsetDateTime/*,
                         SqlYearMonthInterval yearMonthInterval, SqlDaySecondInterval daySecondInterval*/) {
        this.string = string;
        this.character0 = character0;
        this.boolean0 = boolean0;
        this.byte0 = byte0;
        this.short0 = short0;
        this.int0 = int0;
        this.long0 = long0;
        this.float0 = float0;
        this.double0 = double0;
        this.bigDecimal = bigDecimal;
        this.bigInteger = bigInteger;
        this.localTime = localTime;
        this.localDate = localDate;
        this.localDateTime = localDateTime;
        this.date = date;
        this.calendar = calendar;
        this.instant = instant;
        this.zonedDateTime = zonedDateTime;
        this.offsetDateTime = offsetDateTime;
        /*this.yearMonthInterval= yearMonthIntervall;
        this.daySecondInterval = daySecondInterval;*/
    }

    public String getString() {
        return string;
    }

    public char getCharacter0() {
        return character0;
    }

    public boolean isBoolean0() {
        return boolean0;
    }

    public byte getByte0() {
        return byte0;
    }

    public short getShort0() {
        return short0;
    }

    public int getInt0() {
        return int0;
    }

    public long getLong0() {
        return long0;
    }

    public float getFloat0() {
        return float0;
    }

    public double getDouble0() {
        return double0;
    }

    public BigDecimal getBigDecimal() {
        return bigDecimal;
    }

    public BigInteger getBigInteger() {
        return bigInteger;
    }

    public LocalTime getLocalTime() {
        return localTime;
    }

    public LocalDate getLocalDate() {
        return localDate;
    }

    public LocalDateTime getLocalDateTime() {
        return localDateTime;
    }

    public Date getDate() {
        return date;
    }

    public GregorianCalendar getCalendar() {
        return calendar;
    }

    public Instant getInstant() {
        return instant;
    }

    public ZonedDateTime getZonedDateTime() {
        return zonedDateTime;
    }

    public OffsetDateTime getOffsetDateTime() {
        return offsetDateTime;
    }

    /*public SqlYearMonthInterval getYearMonthInterval() {
        return yearMonthInterval;
    }

    public void setYearMonthInterval(SqlYearMonthInterval yearMonthInterval) {
        this.yearMonthInterval = yearMonthInterval;
    }

    public SqlDaySecondInterval getDaySecondInterval() {
        return daySecondInterval;
    }

    public void setDaySecondInterval(SqlDaySecondInterval daySecondInterval) {
        this.daySecondInterval = daySecondInterval;
    }*/

    @Override
    public String toString() {
        return "AllTypesValue{"
                + "string='" + string + '\''
                + ", character0=" + character0
                + ", boolean0=" + boolean0
                + ", byte0=" + byte0
                + ", short0=" + short0
                + ", int0=" + int0
                + ", long0=" + long0
                + ", float0=" + float0
                + ", double0=" + double0
                + ", bigDecimal=" + bigDecimal
                + ", bigInteger=" + bigInteger
                + ", localTime=" + localTime
                + ", localDate=" + localDate
                + ", localDateTime=" + localDateTime
                + ", date=" + date
                + ", calendar=" + calendar
                + ", instant=" + instant
                + ", zonedDateTime=" + zonedDateTime
                + ", offsetDateTime=" + offsetDateTime
                //+ ", yearMonthInterval=" + yearMonthInterval
                //+ ", daySecondInterval=" + daySecondInterval
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AllTypesValue that = (AllTypesValue) o;
        return character0 == that.character0
                && boolean0 == that.boolean0
                && byte0 == that.byte0
                && short0 == that.short0
                && int0 == that.int0
                && long0 == that.long0
                && Float.compare(that.float0, float0) == 0
                && Double.compare(that.double0, double0) == 0
                && Objects.equals(string, that.string)
                && Objects.equals(bigDecimal, that.bigDecimal)
                && Objects.equals(bigInteger, that.bigInteger)
                && Objects.equals(localTime, that.localTime)
                && Objects.equals(localDate, that.localDate)
                && Objects.equals(localDateTime, that.localDateTime)
                && Objects.equals(date, that.date)
                && Objects.equals(calendar, that.calendar)
                && Objects.equals(instant, that.instant)
                && Objects.equals(zonedDateTime, that.zonedDateTime)
                && Objects.equals(offsetDateTime, that.offsetDateTime) /*
                && Objects.equals(yearMonthInterval, that.yearMonthInterval)
                && Objects.equals(daySecondInterval, that.daySecondInterval)*/;
    }
}
