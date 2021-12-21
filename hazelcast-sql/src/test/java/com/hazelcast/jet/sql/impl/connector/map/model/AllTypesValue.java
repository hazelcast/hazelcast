/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.map.model;

import com.google.common.collect.ImmutableMap;

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
import java.util.Map;
import java.util.Objects;

import static java.time.Instant.ofEpochMilli;
import static java.time.ZoneOffset.UTC;

/**
 * A class that has a field of every supported type in SQL.
 */
@SuppressWarnings("unused") // getters-setters are used through reflection
public final class AllTypesValue implements Serializable {

    private String string;
    private char character0;
    private boolean boolean0;
    private byte byte0;
    private short short0;
    private int int0;
    private long long0;
    private float float0;
    private double double0;
    private BigDecimal bigDecimal;
    private BigInteger bigInteger;
    private LocalTime localTime;
    private LocalDate localDate;
    private LocalDateTime localDateTime;
    private Date date;
    private GregorianCalendar calendar;
    private Instant instant;
    private ZonedDateTime zonedDateTime;
    private OffsetDateTime offsetDateTime;
    private Map<Integer, Integer> map;
    private Object object;

    public AllTypesValue() {
    }

    @SuppressWarnings({"checkstyle:ParameterNumber", "checkstyle:ExecutableStatementCount"})
    public AllTypesValue(String string, char character0, boolean boolean0, byte byte0, short short0, int int0,
                         long long0, float float0, double double0, BigDecimal bigDecimal, BigInteger bigInteger,
                         LocalTime localTime, LocalDate localDate, LocalDateTime localDateTime, Date date,
                         GregorianCalendar calendar, Instant instant, ZonedDateTime zonedDateTime,
                         OffsetDateTime offsetDateTime, Map<Integer, Integer> map, Object object
    ) {
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
        this.map = map;
        this.object = object;
    }

    public static AllTypesValue testValue() {
        return new AllTypesValue(
                "string",
                's',
                true,
                (byte) 127,
                (short) 32767,
                2147483647,
                9223372036854775807L,
                1234567890.1f,
                123451234567890.1,
                new BigDecimal("9223372036854775.123"),
                new BigInteger("9223372036854775"),
                LocalTime.of(12, 23, 34),
                LocalDate.of(2020, 4, 15),
                LocalDateTime.of(2020, 4, 15, 12, 23, 34, 1_000_000),
                Date.from(ofEpochMilli(1586953414200L)),
                GregorianCalendar.from(ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC)),
                ofEpochMilli(1586953414200L),
                ZonedDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC),
                OffsetDateTime.of(2020, 4, 15, 12, 23, 34, 200_000_000, UTC),
                ImmutableMap.of(42, 43),
                null
        );
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public char getCharacter0() {
        return character0;
    }

    public void setCharacter0(char character0) {
        this.character0 = character0;
    }

    public boolean isBoolean0() {
        return boolean0;
    }

    public void setBoolean0(boolean boolean0) {
        this.boolean0 = boolean0;
    }

    public byte getByte0() {
        return byte0;
    }

    public void setByte0(byte byte0) {
        this.byte0 = byte0;
    }

    public short getShort0() {
        return short0;
    }

    public void setShort0(short short0) {
        this.short0 = short0;
    }

    public int getInt0() {
        return int0;
    }

    public void setInt0(int int0) {
        this.int0 = int0;
    }

    public long getLong0() {
        return long0;
    }

    public void setLong0(long long0) {
        this.long0 = long0;
    }

    public float getFloat0() {
        return float0;
    }

    public void setFloat0(float float0) {
        this.float0 = float0;
    }

    public double getDouble0() {
        return double0;
    }

    public void setDouble0(double double0) {
        this.double0 = double0;
    }

    public BigDecimal getBigDecimal() {
        return bigDecimal;
    }

    public void setBigDecimal(BigDecimal bigDecimal) {
        this.bigDecimal = bigDecimal;
    }

    public BigInteger getBigInteger() {
        return bigInteger;
    }

    public void setBigInteger(BigInteger bigInteger) {
        this.bigInteger = bigInteger;
    }

    public LocalTime getLocalTime() {
        return localTime;
    }

    public void setLocalTime(LocalTime localTime) {
        this.localTime = localTime;
    }

    public LocalDate getLocalDate() {
        return localDate;
    }

    public void setLocalDate(LocalDate localDate) {
        this.localDate = localDate;
    }

    public LocalDateTime getLocalDateTime() {
        return localDateTime;
    }

    public void setLocalDateTime(LocalDateTime localDateTime) {
        this.localDateTime = localDateTime;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public GregorianCalendar getCalendar() {
        return calendar;
    }

    public void setCalendar(GregorianCalendar calendar) {
        this.calendar = calendar;
    }

    public Instant getInstant() {
        return instant;
    }

    public void setInstant(Instant instant) {
        this.instant = instant;
    }

    public ZonedDateTime getZonedDateTime() {
        return zonedDateTime;
    }

    public void setZonedDateTime(ZonedDateTime zonedDateTime) {
        this.zonedDateTime = zonedDateTime;
    }

    public OffsetDateTime getOffsetDateTime() {
        return offsetDateTime;
    }

    public void setOffsetDateTime(OffsetDateTime offsetDateTime) {
        this.offsetDateTime = offsetDateTime;
    }

    public Map<Integer, Integer> getMap() {
        return map;
    }

    public void setMap(Map<Integer, Integer> map) {
        this.map = map;
    }

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    @Override
    public String toString() {
        return "AllTypesValue{" +
                "string='" + string + '\'' +
                ", character0=" + character0 +
                ", boolean0=" + boolean0 +
                ", byte0=" + byte0 +
                ", short0=" + short0 +
                ", int0=" + int0 +
                ", long0=" + long0 +
                ", float0=" + float0 +
                ", double0=" + double0 +
                ", bigDecimal=" + bigDecimal +
                ", bigInteger=" + bigInteger +
                ", localTime=" + localTime +
                ", localDate=" + localDate +
                ", localDateTime=" + localDateTime +
                ", date=" + date +
                ", calendar=" + calendar +
                ", instant=" + instant +
                ", zonedDateTime=" + zonedDateTime +
                ", offsetDateTime=" + offsetDateTime +
                ", map=" + map +
                ", object=" + object +
                '}';
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
        return character0 == that.character0 &&
                boolean0 == that.boolean0 &&
                byte0 == that.byte0 &&
                short0 == that.short0 &&
                int0 == that.int0 &&
                long0 == that.long0 &&
                Float.compare(that.float0, float0) == 0 &&
                Double.compare(that.double0, double0) == 0 &&
                Objects.equals(string, that.string) &&
                Objects.equals(bigDecimal, that.bigDecimal) &&
                Objects.equals(bigInteger, that.bigInteger) &&
                Objects.equals(localTime, that.localTime) &&
                Objects.equals(localDate, that.localDate) &&
                Objects.equals(localDateTime, that.localDateTime) &&
                Objects.equals(date, that.date) &&
                Objects.equals(calendar, that.calendar) &&
                Objects.equals(instant, that.instant) &&
                Objects.equals(zonedDateTime, that.zonedDateTime) &&
                Objects.equals(offsetDateTime, that.offsetDateTime) &&
                Objects.equals(map, that.map) &&
                Objects.equals(object, that.object);
    }

    @Override
    public int hashCode() {
        return Objects.hash(string,
                character0,
                boolean0,
                byte0,
                short0,
                int0,
                long0,
                float0,
                double0,
                bigDecimal,
                bigInteger,
                localTime,
                localDate,
                localDateTime,
                date,
                calendar,
                instant,
                zonedDateTime,
                offsetDateTime,
                map,
                object
        );
    }
}
