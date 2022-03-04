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

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlCustomClass;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.math.ExpressionMath;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DATE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DOUBLE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.JSON;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.NULL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.REAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIME;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TINYINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.values;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@SuppressWarnings("SimplifiableJUnitAssertion")
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConvertersTest {
    @Test
    public void testGetById() {
        checkGetById(StringConverter.INSTANCE);
        checkGetById(CharacterConverter.INSTANCE);

        checkGetById(BooleanConverter.INSTANCE);
        checkGetById(ByteConverter.INSTANCE);
        checkGetById(ShortConverter.INSTANCE);
        checkGetById(IntegerConverter.INSTANCE);
        checkGetById(LongConverter.INSTANCE);
        checkGetById(BigDecimalConverter.INSTANCE);
        checkGetById(BigIntegerConverter.INSTANCE);
        checkGetById(FloatConverter.INSTANCE);
        checkGetById(DoubleConverter.INSTANCE);

        checkGetById(LocalTimeConverter.INSTANCE);
        checkGetById(LocalDateConverter.INSTANCE);
        checkGetById(LocalDateTimeConverter.INSTANCE);
        checkGetById(DateConverter.INSTANCE);
        checkGetById(CalendarConverter.INSTANCE);
        checkGetById(InstantConverter.INSTANCE);
        checkGetById(OffsetDateTimeConverter.INSTANCE);
        checkGetById(ZonedDateTimeConverter.INSTANCE);

        checkGetById(ObjectConverter.INSTANCE);
        checkGetById(JsonConverter.INSTANCE);

        checkGetById(NullConverter.INSTANCE);
    }

    @Test
    public void testGetByClass() {
        checkGetByClass(StringConverter.INSTANCE, String.class);
        checkGetByClass(CharacterConverter.INSTANCE, char.class, Character.class);

        checkGetByClass(BooleanConverter.INSTANCE, boolean.class, Boolean.class);
        checkGetByClass(ByteConverter.INSTANCE, byte.class, Byte.class);
        checkGetByClass(ShortConverter.INSTANCE, short.class, Short.class);
        checkGetByClass(IntegerConverter.INSTANCE, int.class, Integer.class);
        checkGetByClass(LongConverter.INSTANCE, long.class, Long.class);
        checkGetByClass(BigDecimalConverter.INSTANCE, BigDecimal.class);
        checkGetByClass(BigIntegerConverter.INSTANCE, BigInteger.class);
        checkGetByClass(FloatConverter.INSTANCE, float.class, Float.class);
        checkGetByClass(DoubleConverter.INSTANCE, double.class, Double.class);

        checkGetByClass(LocalTimeConverter.INSTANCE, LocalTime.class);
        checkGetByClass(LocalDateConverter.INSTANCE, LocalDate.class);
        checkGetByClass(LocalDateTimeConverter.INSTANCE, LocalDateTime.class);
        checkGetByClass(DateConverter.INSTANCE, Date.class);
        checkGetByClass(CalendarConverter.INSTANCE, Calendar.class, GregorianCalendar.class);
        checkGetByClass(InstantConverter.INSTANCE, Instant.class);
        checkGetByClass(OffsetDateTimeConverter.INSTANCE, OffsetDateTime.class);
        checkGetByClass(ZonedDateTimeConverter.INSTANCE, ZonedDateTime.class);

        checkGetByClass(ObjectConverter.INSTANCE, Object.class, SqlCustomClass.class);
        checkGetByClass(JsonConverter.INSTANCE, HazelcastJsonValue.class);

        checkGetByClass(NullConverter.INSTANCE, void.class);
        checkGetByClass(NullConverter.INSTANCE, Void.class);
    }

    @Test
    public void testJsonConverter() {
        JsonConverter converter = JsonConverter.INSTANCE;
        checkConverter(converter, Converter.ID_JSON, JSON, HazelcastJsonValue.class);
        checkConverterConversions(converter, VARCHAR, JSON, OBJECT);

        assertEquals("[1,2,3]", converter.asVarchar(new HazelcastJsonValue("[1,2,3]")));

        assertEquals(new HazelcastJsonValue("[1,2,3]"), converter.asObject(new HazelcastJsonValue("[1,2,3]")));

        checkConverterSelf(converter);
    }

    @Test
    public void testBooleanConverter() {
        BooleanConverter converter = BooleanConverter.INSTANCE;

        checkConverter(converter, Converter.ID_BOOLEAN, BOOLEAN, Boolean.class);
        checkConverterConversions(converter, VARCHAR, OBJECT);

        assertEquals("true", converter.asVarchar(true));
        assertEquals("false", converter.asVarchar(false));

        assertEquals(true, converter.asObject(true));

        checkConverterSelf(converter);
    }

    @Test
    public void testByteConverter() {
        ByteConverter converter = ByteConverter.INSTANCE;

        checkConverter(converter, Converter.ID_BYTE, TINYINT, Byte.class);
        checkConverterConversions(converter, VARCHAR, SMALLINT, INTEGER, BIGINT, DECIMAL, REAL, DOUBLE, OBJECT);

        assertEquals("1", converter.asVarchar((byte) 1));

        assertEquals(1, converter.asTinyint((byte) 1));
        assertEquals(1, converter.asSmallint((byte) 1));
        assertEquals(1, converter.asInt((byte) 1));
        assertEquals(1L, converter.asBigint((byte) 1));
        assertEquals(BigDecimal.ONE, converter.asDecimal((byte) 1));

        assertEquals(1.0f, converter.asReal((byte) 1), 0);
        assertEquals(1.0d, converter.asDouble((byte) 1), 0);

        assertEquals((byte) 1, converter.asObject((byte) 1));

        checkConverterSelf(converter);
    }

    @Test
    public void testShortConverter() {
        ShortConverter converter = ShortConverter.INSTANCE;

        checkConverter(converter, Converter.ID_SHORT, SMALLINT, Short.class);
        checkConverterConversions(converter, VARCHAR, TINYINT, INTEGER, BIGINT, DECIMAL, REAL, DOUBLE, OBJECT);

        assertEquals("1", converter.asVarchar((short) 1));

        assertEquals(1, converter.asTinyint((short) 1));
        checkDataException(() -> converter.asTinyint(Short.MAX_VALUE));
        assertEquals(1, converter.asSmallint((short) 1));
        assertEquals(1, converter.asInt((short) 1));
        assertEquals(1L, converter.asBigint((short) 1));
        assertEquals(BigDecimal.ONE, converter.asDecimal((short) 1));

        assertEquals(1.0f, converter.asReal((short) 1), 0);
        assertEquals(1.0d, converter.asDouble((short) 1), 0);

        assertEquals((short) 1, converter.asObject((short) 1));

        checkConverterSelf(converter);
    }

    @Test
    public void testIntConverter() {
        IntegerConverter converter = IntegerConverter.INSTANCE;

        checkConverter(converter, Converter.ID_INTEGER, INTEGER, Integer.class);
        checkConverterConversions(converter, VARCHAR, TINYINT, SMALLINT, BIGINT, DECIMAL, REAL, DOUBLE, OBJECT);

        assertEquals("1", converter.asVarchar(1));

        assertEquals(1, converter.asTinyint(1));
        checkDataException(() -> converter.asTinyint(Integer.MAX_VALUE));
        assertEquals(1, converter.asSmallint(1));
        checkDataException(() -> converter.asSmallint(Integer.MAX_VALUE));
        assertEquals(1, converter.asInt(1));
        assertEquals(1L, converter.asBigint(1));
        assertEquals(BigDecimal.ONE, converter.asDecimal(1));

        assertEquals(1.0f, converter.asReal(1), 0);
        assertEquals(1.0d, converter.asDouble(1), 0);

        assertEquals(1, converter.asObject(1));

        checkConverterSelf(converter);
    }

    @Test
    public void testLongConverter() {
        LongConverter converter = LongConverter.INSTANCE;

        checkConverter(converter, Converter.ID_LONG, BIGINT, Long.class);
        checkConverterConversions(converter, VARCHAR, TINYINT, SMALLINT, INTEGER, DECIMAL, REAL, DOUBLE, OBJECT);

        assertEquals("1", converter.asVarchar(1L));

        assertEquals(1, converter.asTinyint(1L));
        checkDataException(() -> converter.asTinyint(Long.MAX_VALUE));
        assertEquals(1, converter.asSmallint(1L));
        checkDataException(() -> converter.asSmallint(Long.MAX_VALUE));
        assertEquals(1, converter.asInt(1L));
        checkDataException(() -> converter.asInt(Long.MAX_VALUE));
        assertEquals(1L, converter.asBigint(1L));
        assertEquals(BigDecimal.ONE, converter.asDecimal(1L));

        assertEquals(1.0f, converter.asReal(1L), 0);
        assertEquals(1.0d, converter.asDouble(1L), 0);

        assertEquals(1L, converter.asObject(1L));

        checkConverterSelf(converter);
    }

    @Test
    public void testBigIntegerConverter() {
        BigIntegerConverter converter = BigIntegerConverter.INSTANCE;

        checkConverter(converter, Converter.ID_BIG_INTEGER, DECIMAL, BigInteger.class);
        checkConverterConversions(converter, VARCHAR, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, OBJECT);

        BigInteger bigValue = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);

        assertEquals("1", converter.asVarchar(BigInteger.ONE));

        assertEquals(1, converter.asTinyint(BigInteger.ONE));
        checkDataException(() -> converter.asTinyint(bigValue));
        assertEquals(1, converter.asSmallint(BigInteger.ONE));
        checkDataException(() -> converter.asSmallint(bigValue));
        assertEquals(1, converter.asInt(BigInteger.ONE));
        checkDataException(() -> converter.asInt(bigValue));
        assertEquals(1L, converter.asBigint(BigInteger.ONE));
        checkDataException(() -> converter.asBigint(bigValue));
        assertEquals(BigDecimal.ONE, converter.asDecimal(BigInteger.ONE));

        assertEquals(1.0f, converter.asReal(BigInteger.ONE), 0);
        assertEquals(1.0d, converter.asDouble(BigInteger.ONE), 0);

        assertEquals(BigDecimal.ONE, converter.asObject(BigInteger.ONE));

        checkConverterSelf(converter);
    }

    @Test
    public void testBigDecimalConverter() {
        BigDecimalConverter converter = BigDecimalConverter.INSTANCE;

        checkConverter(converter, Converter.ID_BIG_DECIMAL, DECIMAL, BigDecimal.class);
        checkConverterConversions(converter, VARCHAR, TINYINT, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE, OBJECT);

        BigDecimal val = BigDecimal.valueOf(11, 1);
        BigDecimal bigValue = BigDecimal.valueOf(Long.MAX_VALUE).add(new BigDecimal("1.1"));

        assertEquals("1.1", converter.asVarchar(val));

        assertEquals(1, converter.asTinyint(val));
        checkDataException(() -> converter.asTinyint(bigValue));
        assertEquals(1, converter.asSmallint(val));
        checkDataException(() -> converter.asSmallint(bigValue));
        assertEquals(1, converter.asInt(val));
        checkDataException(() -> converter.asInt(bigValue));
        assertEquals(1L, converter.asBigint(val));
        checkDataException(() -> converter.asBigint(bigValue));
        assertEquals(val, converter.asDecimal(val));

        assertEquals(1.1f, converter.asReal(val), 0);
        assertEquals(1.1d, converter.asDouble(val), 0);

        assertEquals(val, converter.asObject(val));

        checkConverterSelf(converter);
    }

    @Test
    public void testFloatConverter() {
        FloatConverter converter = FloatConverter.INSTANCE;

        checkConverter(converter, Converter.ID_FLOAT, REAL, Float.class);
        checkConverterConversions(converter, VARCHAR, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, DOUBLE, OBJECT);

        float val = 1.1f;
        float bigValue = Long.MAX_VALUE * 2.5f;

        assertEquals("1.1", converter.asVarchar(val));

        assertEquals(1, converter.asTinyint(val));
        checkDataException(() -> converter.asTinyint(bigValue));
        checkDataException(() -> converter.asTinyint(Byte.MAX_VALUE + 1.0f));
        checkDataException(() -> converter.asTinyint(Float.POSITIVE_INFINITY));
        checkDataException(() -> converter.asTinyint(Float.NEGATIVE_INFINITY));
        checkDataException(() -> converter.asTinyint(Float.NaN));

        assertEquals(1, converter.asSmallint(val));
        checkDataException(() -> converter.asSmallint(bigValue));
        checkDataException(() -> converter.asSmallint(Short.MAX_VALUE + 1.0f));
        checkDataException(() -> converter.asSmallint(Float.POSITIVE_INFINITY));
        checkDataException(() -> converter.asSmallint(Float.NEGATIVE_INFINITY));
        checkDataException(() -> converter.asSmallint(Float.NaN));

        assertEquals(1, converter.asInt(val));
        checkDataException(() -> converter.asInt(bigValue));
        checkDataException(() -> converter.asInt(Integer.MAX_VALUE + 1.0f));
        checkDataException(() -> converter.asInt(Float.POSITIVE_INFINITY));
        checkDataException(() -> converter.asInt(Float.NEGATIVE_INFINITY));
        checkDataException(() -> converter.asInt(Float.NaN));

        assertEquals(1L, converter.asBigint(val));
        checkDataException(() -> converter.asBigint(bigValue));
        checkDataException(() -> converter.asBigint(Long.MAX_VALUE + Math.ulp((float) Long.MAX_VALUE)));
        checkDataException(() -> converter.asBigint(Float.POSITIVE_INFINITY));
        checkDataException(() -> converter.asBigint(Float.NEGATIVE_INFINITY));
        checkDataException(() -> converter.asBigint(Float.NaN));

        assertEquals(new BigDecimal(val, ExpressionMath.DECIMAL_MATH_CONTEXT), converter.asDecimal(val));

        assertEquals(1.1f, converter.asReal(val), 0);
        assertEquals(1.1f, converter.asDouble(val), 0);

        assertEquals(1.1f, converter.asObject(val));

        checkConverterSelf(converter);
    }

    @Test
    public void testDoubleConverter() {
        DoubleConverter converter = DoubleConverter.INSTANCE;

        checkConverter(converter, Converter.ID_DOUBLE, DOUBLE, Double.class);
        checkConverterConversions(converter, VARCHAR, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, REAL, OBJECT);

        double val = 1.1d;
        double bigValue = Long.MAX_VALUE * 2.5d;

        assertEquals("1.1", converter.asVarchar(val));

        assertEquals(1, converter.asTinyint(val));
        checkDataException(() -> converter.asTinyint(bigValue));
        checkDataException(() -> converter.asTinyint(Byte.MAX_VALUE + 1.0d));
        checkDataException(() -> converter.asTinyint(Double.POSITIVE_INFINITY));
        checkDataException(() -> converter.asTinyint(Double.NEGATIVE_INFINITY));
        checkDataException(() -> converter.asTinyint(Double.NaN));

        assertEquals(1, converter.asSmallint(val));
        checkDataException(() -> converter.asSmallint(bigValue));
        checkDataException(() -> converter.asSmallint(Short.MAX_VALUE + 1.0d));
        checkDataException(() -> converter.asSmallint(Double.POSITIVE_INFINITY));
        checkDataException(() -> converter.asSmallint(Double.NEGATIVE_INFINITY));
        checkDataException(() -> converter.asSmallint(Double.NaN));

        assertEquals(1, converter.asInt(val));
        checkDataException(() -> converter.asInt(bigValue));
        checkDataException(() -> converter.asInt(Integer.MAX_VALUE + 1.0d));
        checkDataException(() -> converter.asInt(Double.POSITIVE_INFINITY));
        checkDataException(() -> converter.asInt(Double.NEGATIVE_INFINITY));
        checkDataException(() -> converter.asInt(Double.NaN));

        assertEquals(1L, converter.asBigint(val));
        checkDataException(() -> converter.asBigint(bigValue));
        checkDataException(() -> converter.asBigint(Long.MAX_VALUE + Math.ulp((double) Long.MAX_VALUE)));
        checkDataException(() -> converter.asBigint(Double.POSITIVE_INFINITY));
        checkDataException(() -> converter.asBigint(Double.NEGATIVE_INFINITY));
        checkDataException(() -> converter.asBigint(Double.NaN));

        assertEquals(new BigDecimal(val, ExpressionMath.DECIMAL_MATH_CONTEXT), converter.asDecimal(val));

        assertEquals(1.1f, converter.asReal(val), 0);
        assertEquals(1.1d, converter.asDouble(val), 0);

        assertEquals(1.1d, converter.asObject(val));

        checkConverterSelf(converter);
    }

    @Test
    public void testLocalTimeConverter() {
        LocalTimeConverter converter = LocalTimeConverter.INSTANCE;

        checkConverter(converter, Converter.ID_LOCAL_TIME, TIME, LocalTime.class);
        checkConverterConversions(converter, VARCHAR, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE, OBJECT);

        String timeString = "11:22:33.444";
        LocalTime time = LocalTime.parse(timeString);
        LocalDate date = LocalDate.now();
        LocalDateTime dateTime = LocalDateTime.of(date, time);
        OffsetDateTime globalDateTime = ZonedDateTime.of(dateTime, ZoneId.systemDefault()).toOffsetDateTime();

        assertEquals(timeString, converter.asVarchar(time));
        assertEquals(time, converter.asTime(time));
        assertEquals(dateTime, converter.asTimestamp(time));
        assertEquals(globalDateTime, converter.asTimestampWithTimezone(time));

        checkConverterSelf(converter);
    }

    @Test
    public void testLocalDateConverter() {
        LocalDateConverter converter = LocalDateConverter.INSTANCE;

        checkConverter(converter, Converter.ID_LOCAL_DATE, DATE, LocalDate.class);
        checkConverterConversions(converter, VARCHAR, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE, OBJECT);

        String dateString = "2020-02-02";
        LocalDate date = LocalDate.parse(dateString);
        LocalDateTime dateTime = date.atStartOfDay();
        OffsetDateTime globalDateTime = ZonedDateTime.of(dateTime, ZoneId.systemDefault()).toOffsetDateTime();

        assertEquals(dateString, converter.asVarchar(date));
        assertEquals(date, converter.asDate(date));
        assertEquals(dateTime, converter.asTimestamp(date));
        assertEquals(globalDateTime, converter.asTimestampWithTimezone(date));

        checkConverterSelf(converter);
    }

    @Test
    public void testLocalDateTimeConverter() {
        LocalDateTimeConverter converter = LocalDateTimeConverter.INSTANCE;

        checkConverter(converter, Converter.ID_LOCAL_DATE_TIME, TIMESTAMP, LocalDateTime.class);
        checkConverterConversions(converter, VARCHAR, TIME, DATE, TIMESTAMP_WITH_TIME_ZONE, OBJECT);

        String dateTimeString = "2020-02-02T11:22:33.444";
        LocalDateTime dateTime = LocalDateTime.parse(dateTimeString);
        OffsetDateTime globalDateTime = ZonedDateTime.of(dateTime, ZoneId.systemDefault()).toOffsetDateTime();

        assertEquals(dateTimeString, converter.asVarchar(dateTime));
        assertEquals(dateTime.toLocalDate(), converter.asDate(dateTime));
        assertEquals(dateTime.toLocalTime(), converter.asTime(dateTime));
        assertEquals(dateTime, converter.asTimestamp(dateTime));
        assertEquals(globalDateTime, converter.asTimestampWithTimezone(dateTime));

        checkConverterSelf(converter);
    }

    @Test
    public void testDateConverter() {
        DateConverter converter = DateConverter.INSTANCE;

        checkConverter(converter, Converter.ID_DATE, TIMESTAMP_WITH_TIME_ZONE, Date.class);
        checkConverterConversions(converter, VARCHAR, TIME, DATE, TIMESTAMP, OBJECT);

        Date val = new Date();

        checkTimestampWithTimezone(converter, val, OffsetDateTime.ofInstant(val.toInstant(), ZoneId.systemDefault()));

        checkConverterSelf(converter);
    }

    @Test
    public void testCalendarConverter() {
        CalendarConverter converter = CalendarConverter.INSTANCE;

        checkConverter(converter, Converter.ID_CALENDAR, TIMESTAMP_WITH_TIME_ZONE, Calendar.class);
        checkConverterConversions(converter, VARCHAR, TIME, DATE, TIMESTAMP, OBJECT);

        Calendar val = Calendar.getInstance();

        checkTimestampWithTimezone(converter, val, OffsetDateTime.ofInstant(val.toInstant(), val.getTimeZone().toZoneId()));

        checkConverterSelf(converter);
    }

    @Test
    public void testInstantConverter() {
        InstantConverter converter = InstantConverter.INSTANCE;

        checkConverter(converter, Converter.ID_INSTANT, TIMESTAMP_WITH_TIME_ZONE, Instant.class);
        checkConverterConversions(converter, VARCHAR, TIME, DATE, TIMESTAMP, OBJECT);

        Instant val = Instant.now();

        checkTimestampWithTimezone(converter, val, OffsetDateTime.ofInstant(val, ZoneId.systemDefault()));

        checkConverterSelf(converter);
    }

    @Test
    public void testOffsetDateTimeConverter() {
        OffsetDateTimeConverter converter = OffsetDateTimeConverter.INSTANCE;

        checkConverter(converter, Converter.ID_OFFSET_DATE_TIME, TIMESTAMP_WITH_TIME_ZONE, OffsetDateTime.class);
        checkConverterConversions(converter, VARCHAR, TIME, DATE, TIMESTAMP, OBJECT);

        OffsetDateTime val = OffsetDateTime.now();

        checkTimestampWithTimezone(converter, val, val);

        checkConverterSelf(converter);
    }

    @Test
    public void testZonedDateTimeConverter() {
        ZonedDateTimeConverter converter = ZonedDateTimeConverter.INSTANCE;

        checkConverter(converter, Converter.ID_ZONED_DATE_TIME, TIMESTAMP_WITH_TIME_ZONE, ZonedDateTime.class);
        checkConverterConversions(converter, VARCHAR, TIME, DATE, TIMESTAMP, OBJECT);

        ZonedDateTime val = ZonedDateTime.now();

        checkTimestampWithTimezone(converter, val, val.toOffsetDateTime());

        checkConverterSelf(converter);
    }

    @Test
    public void testStringConverter() {
        StringConverter c = StringConverter.INSTANCE;

        checkConverter(c, Converter.ID_STRING, VARCHAR, String.class);
        checkConverterConversions(
            c,
            BOOLEAN,
            TINYINT,
            SMALLINT,
            INTEGER,
            BIGINT,
            DECIMAL,
            REAL,
            DOUBLE,
            TIME,
            DATE,
            TIMESTAMP,
            TIMESTAMP_WITH_TIME_ZONE,
            OBJECT,
            JSON
        );

        // Boolean
        assertEquals(false, c.asBoolean("false"));
        assertEquals(false, c.asBoolean("False"));
        assertEquals(true, c.asBoolean("true"));
        assertEquals(true, c.asBoolean("True"));
        checkDataException(() -> c.asBoolean("0"));
        checkDataException(() -> c.asBoolean("1"));

        // Numeric
        String invalid = "invalid";
        String bigValue = BigDecimal.valueOf(Long.MAX_VALUE).add(new BigDecimal("1.1")).toPlainString();

        assertEquals((byte) 1, c.asTinyint("1"));
        checkDataException(() -> c.asTinyint(invalid));
        checkDataException(() -> c.asTinyint(bigValue));

        assertEquals((short) 1, c.asSmallint("1"));
        checkDataException(() -> c.asSmallint(invalid));
        checkDataException(() -> c.asSmallint(bigValue));

        assertEquals(1, c.asInt("1"));
        checkDataException(() -> c.asInt(invalid));
        checkDataException(() -> c.asInt(bigValue));

        assertEquals(1L, c.asBigint("1"));
        checkDataException(() -> c.asBigint(invalid));
        checkDataException(() -> c.asBigint(bigValue));

        assertEquals(new BigDecimal("1.1"), c.asDecimal("1.1"));
        checkDataException(() -> c.asDecimal(invalid));

        assertEquals(1.1f, c.asReal("1.1"), 0);
        checkDataException(() -> c.asReal(invalid));

        assertEquals(1.1d, c.asDouble("1.1"), 0);
        checkDataException(() -> c.asDouble(invalid));

        // Temporal
        assertEquals(LocalTime.parse("01:02"), c.asTime("1:2"));
        assertEquals(LocalTime.parse("01:22"), c.asTime("1:22"));
        assertEquals(LocalTime.parse("11:02"), c.asTime("11:2"));
        assertEquals(LocalTime.parse("11:22"), c.asTime("11:22"));
        assertEquals(LocalTime.parse("11:22:33"), c.asTime("11:22:33"));
        assertEquals(LocalTime.parse("11:22:33.444"), c.asTime("11:22:33.444"));
        assertEquals(LocalTime.parse("11:22:33.444444444"), c.asTime("11:22:33.444444444"));
        checkDataException(() -> c.asTime("33:22"));
        checkDataException(() -> c.asTime("11:66"));
        checkDataException(() -> c.asTime("11:22:66"));
        checkDataException(() -> c.asTime("11:22:33.4444444444"));
        checkDataException(() -> c.asTime(invalid));

        assertEquals(LocalDate.parse("2020-01-01"), c.asDate("2020-01-01"));
        assertEquals(LocalDate.parse("2020-01-01"), c.asDate("2020-1-01"));
        assertEquals(LocalDate.parse("2020-01-01"), c.asDate("2020-01-1"));
        assertEquals(LocalDate.parse("2020-01-01"), c.asDate("2020-1-1"));
        assertEquals(LocalDate.parse("2020-12-01"), c.asDate("2020-12-1"));
        assertEquals(LocalDate.parse("2020-01-12"), c.asDate("2020-1-12"));
        assertEquals(LocalDate.parse("2020-09-12"), c.asDate("2020-09-12"));
        assertEquals(LocalDate.parse("2020-09-12"), c.asDate("2020-9-12"));
        checkDataException(() -> c.asDate("2020-13-01"));
        checkDataException(() -> c.asDate("2020-01-35"));
        checkDataException(() -> c.asDate("2020-13-1"));
        checkDataException(() -> c.asDate("2020-1-35"));
        checkDataException(() -> c.asDate(invalid));

        assertEquals(LocalDateTime.parse("2020-01-01T11:22"), c.asTimestamp("2020-01-01T11:22"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33"), c.asTimestamp("2020-01-01T11:22:33"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33.444"), c.asTimestamp("2020-01-01T11:22:33.444"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33.444444444"), c.asTimestamp("2020-01-01T11:22:33.444444444"));
        checkDataException(() -> c.asTimestamp("2020-13-01T11:22"));
        checkDataException(() -> c.asTimestamp("2020-01-35T11:22"));
        checkDataException(() -> c.asTimestamp("2020-01-01T33:22"));
        checkDataException(() -> c.asTimestamp("2020-01-01T11:66"));
        checkDataException(() -> c.asTimestamp("2020-01-01T11:22:66"));
        checkDataException(() -> c.asTimestamp("2020-01-01T11:22:33.4444444444"));
        checkDataException(() -> c.asTimestamp(invalid));

        assertEquals(LocalDateTime.parse("2020-01-01T11:22"), c.asTimestamp("2020-01-01 11:22"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33"), c.asTimestamp("2020-01-01 11:22:33"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33.444"), c.asTimestamp("2020-01-01 11:22:33.444"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33.444444444"), c.asTimestamp("2020-01-01 11:22:33.444444444"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22"), c.asTimestamp("2020-01-01 11:22"));
        assertEquals(LocalDateTime.parse("2020-01-01T01:22:33"), c.asTimestamp("2020-01-01 1:22:33"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33.444"), c.asTimestamp("2020-01-01 11:22:33.444"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33.444444444"), c.asTimestamp("2020-01-01 11:22:33.444444444"));
        checkDataException(() -> c.asTimestamp("2020-13-01 11:22"));
        checkDataException(() -> c.asTimestamp("2020-13-01 11:22"));
        checkDataException(() -> c.asTimestamp("2020-01-35 11:22"));
        checkDataException(() -> c.asTimestamp("2020-01-1 33:22"));
        checkDataException(() -> c.asTimestamp("2020-01-1 11:66"));
        checkDataException(() -> c.asTimestamp("2020-01-1 T11:22:66"));
        checkDataException(() -> c.asTimestamp("2020-01-01 11:22:33.4444444444"));

        assertEquals(OffsetDateTime.parse("2020-01-01T11:22:33.444444444Z"),
            c.asTimestampWithTimezone("2020-01-01T11:22:33.444444444Z"));
        assertEquals(OffsetDateTime.parse("2020-01-01T11:22:33.444444444+01:00"),
            c.asTimestampWithTimezone("2020-01-01T11:22:33.444444444+01:00"));
        checkDataException(() -> c.asTimestampWithTimezone(invalid));

        // Object
        assertEquals("val", c.asObject("val"));

        checkConverterSelf(c);
    }

    @Test
    public void testCharacterConverter() {
        CharacterConverter c = CharacterConverter.INSTANCE;

        checkConverter(c, Converter.ID_CHARACTER, VARCHAR, Character.class);
        checkConverterConversions(
            c,
            BOOLEAN,
            TINYINT,
            SMALLINT,
            INTEGER,
            BIGINT,
            DECIMAL,
            REAL,
            DOUBLE,
            TIME,
            DATE,
            TIMESTAMP,
            TIMESTAMP_WITH_TIME_ZONE,
            OBJECT
        );

        char invalid = 'c';

        assertEquals((byte) 1, c.asTinyint('1'));
        checkDataException(() -> c.asTinyint(invalid));

        assertEquals((short) 1, c.asSmallint('1'));
        checkDataException(() -> c.asSmallint(invalid));

        assertEquals(1, c.asInt('1'));
        checkDataException(() -> c.asInt(invalid));

        assertEquals(1L, c.asBigint('1'));
        checkDataException(() -> c.asBigint(invalid));

        assertEquals(new BigDecimal("1"), c.asDecimal('1'));
        checkDataException(() -> c.asDecimal(invalid));

        assertEquals(1f, c.asReal('1'), 0);
        checkDataException(() -> c.asReal(invalid));

        assertEquals(1d, c.asDouble('1'), 0);
        checkDataException(() -> c.asDouble(invalid));

        assertEquals("c", c.asObject('c'));

        checkConverterSelf(c);
    }

    @Test
    public void testObjectConverter() {
        ObjectConverter c = ObjectConverter.INSTANCE;

        checkConverter(c, Converter.ID_OBJECT, OBJECT, Object.class);
        checkConverterConversions(
            c,
            BOOLEAN,
            TINYINT,
            SMALLINT,
            INTEGER,
            BIGINT,
            DECIMAL,
            REAL,
            DOUBLE,
            TIME,
            DATE,
            TIMESTAMP,
            TIMESTAMP_WITH_TIME_ZONE,
            VARCHAR,
            JSON
        );

        checkObjectConverter(c);

        checkConverterSelf(c);
    }

    @Test
    public void testNullConverter() {
        NullConverter c = NullConverter.INSTANCE;
        checkConverter(c, Converter.ID_NULL, NULL, Void.class);
        checkConverterConversions(c, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, REAL, DOUBLE, TIME, DATE, TIMESTAMP,
                TIMESTAMP_WITH_TIME_ZONE, VARCHAR, OBJECT);

        checkUnsupportedException(() -> c.asBoolean(null));
        checkUnsupportedException(() -> c.asTinyint(null));
        checkUnsupportedException(() -> c.asSmallint(null));
        checkUnsupportedException(() -> c.asInt(null));
        checkUnsupportedException(() -> c.asBigint(null));
        checkUnsupportedException(() -> c.asDecimal(null));
        checkUnsupportedException(() -> c.asReal(null));
        checkUnsupportedException(() -> c.asDouble(null));
        checkUnsupportedException(() -> c.asTime(null));
        checkUnsupportedException(() -> c.asDate(null));
        checkUnsupportedException(() -> c.asTimestamp(null));
        checkUnsupportedException(() -> c.asTimestampWithTimezone(null));
        checkUnsupportedException(() -> c.asVarchar(null));
        checkUnsupportedException(() -> c.asObject(null));

        checkUnsupportedException(() -> c.convertToSelf(c, null));
    }

    private void checkDataException(Runnable runnable) {
        try {
            runnable.run();

            fail("Must fail");
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.DATA_EXCEPTION, e.getCode());
        }
    }

    private void checkUnsupportedException(Runnable runnable) {
        try {
            runnable.run();

            fail("Must fail");
        } catch (UnsupportedOperationException ignore) {
            // do nothing
        }
    }

    private void checkTimestampWithTimezone(Converter converter, Object value, OffsetDateTime timestampWithTimezone) {
        LocalDateTime timestamp = timestampWithTimezone.atZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime();
        LocalDate date = timestamp.toLocalDate();
        LocalTime time = timestamp.toLocalTime();

        assertEquals(timestampWithTimezone.toString(), converter.asVarchar(value));
        assertEquals(date, converter.asDate(value));
        assertEquals(time, converter.asTime(value));
        assertEquals(timestamp, converter.asTimestamp(value));
        assertEquals(timestampWithTimezone, converter.asTimestampWithTimezone(value));
        assertEquals(timestampWithTimezone, converter.asObject(value));
    }

    private void checkObjectConverter(Converter c) {
        // Boolean
        assertEquals(true, c.asBoolean(true));
        assertEquals(true, c.asBoolean("true"));
        assertEquals(false, c.asBoolean(false));
        assertEquals(false, c.asBoolean("false"));
        checkDataException(() -> c.asBoolean("1"));
        checkDataException(() -> c.asBoolean(1));
        checkDataException(() -> c.asBoolean(new Object()));

        // Numeric
        String invalid = "invalid";
        String bigValue = BigDecimal.valueOf(Long.MAX_VALUE).add(new BigDecimal("1.1")).toPlainString();

        assertEquals((byte) 1, c.asTinyint(1));
        assertEquals((byte) 1, c.asTinyint("1"));
        checkDataException(() -> c.asTinyint(invalid));
        checkDataException(() -> c.asTinyint(bigValue));

        assertEquals((short) 1, c.asSmallint(1));
        assertEquals((short) 1, c.asSmallint("1"));
        checkDataException(() -> c.asSmallint(invalid));
        checkDataException(() -> c.asSmallint(bigValue));

        assertEquals(1, c.asInt(1));
        assertEquals(1, c.asInt("1"));
        checkDataException(() -> c.asInt(invalid));
        checkDataException(() -> c.asInt(bigValue));

        assertEquals(1L, c.asBigint(1));
        assertEquals(1L, c.asBigint("1"));
        checkDataException(() -> c.asBigint(invalid));
        checkDataException(() -> c.asBigint(bigValue));

        assertEquals(new BigDecimal("1.1"), c.asDecimal(new BigDecimal("1.1")));
        assertEquals(new BigDecimal("1.1"), c.asDecimal("1.1"));
        checkDataException(() -> c.asDecimal(invalid));

        assertEquals(1.1f, c.asReal(1.1f), 0);
        assertEquals(1.1f, c.asReal("1.1"), 0);
        checkDataException(() -> c.asReal(invalid));

        assertEquals(1.1d, c.asDouble(1.1d), 0);
        assertEquals(1.1d, c.asDouble("1.1"), 0);
        checkDataException(() -> c.asDouble(invalid));

        // Temporal
        assertEquals(LocalTime.parse("11:22"), c.asTime(LocalTime.parse("11:22")));
        assertEquals(LocalTime.parse("11:22"), c.asTime(LocalDateTime.parse("2020-01-01T11:22")));
        assertEquals(LocalTime.parse("11:22"), c.asTime("11:22"));
        checkDataException(() -> c.asTime(invalid));

        assertEquals(LocalDate.parse("2020-01-01"), c.asDate(LocalDate.parse("2020-01-01")));
        assertEquals(LocalDate.parse("2020-01-01"), c.asDate(LocalDateTime.parse("2020-01-01T11:22:33")));
        assertEquals(LocalDate.parse("2020-01-01"), c.asDate("2020-01-01"));
        checkDataException(() -> c.asDate(invalid));

        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33.444444444"),
            c.asTimestamp(LocalDateTime.parse("2020-01-01T11:22:33.444444444")));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33.444444444"), c.asTimestamp("2020-01-01T11:22:33.444444444"));
        checkDataException(() -> c.asTimestamp(invalid));

        assertEquals(OffsetDateTime.parse("2020-01-01T11:22:33.444444444+01:00"),
            c.asTimestampWithTimezone(OffsetDateTime.parse("2020-01-01T11:22:33.444444444+01:00")));
        assertEquals(OffsetDateTime.parse("2020-01-01T11:22:33.444444444+01:00"),
            c.asTimestampWithTimezone("2020-01-01T11:22:33.444444444+01:00"));
        checkDataException(() -> c.asTimestampWithTimezone(invalid));

        // Strings.
        assertEquals("c", c.asVarchar('c'));
        assertEquals("val", c.asVarchar("val"));
        assertEquals("2020-01-01T11:22:33.444444444", c.asVarchar(LocalDateTime.parse("2020-01-01T11:22:33.444444444")));
        assertEquals(new SqlCustomClass(1).toString(), c.asVarchar(new SqlCustomClass(1)));

        // Object
        assertEquals("val", c.asObject("val"));
        assertEquals(1, c.asObject(1));
        assertEquals(new SqlCustomClass(1), c.asObject(new SqlCustomClass(1)));
    }

    private void checkGetById(Converter converter) {
        Converter other = Converters.getConverter(converter.getId());

        assertSame(converter, other);
    }

    private void checkGetByClass(Converter expectedConverter, Class<?>... classes) {
        for (Class<?> clazz : classes) {
            Converter other = Converters.getConverter(clazz);

            assertSame(expectedConverter, other);
        }
    }

    private void checkConverter(
        Converter converter,
        int expectedId,
        QueryDataTypeFamily expectedTypeFamily,
        Class<?> expectedValueClass
    ) {
        assertEquals(expectedId, converter.getId());
        assertEquals(expectedTypeFamily, converter.getTypeFamily());
        assertEquals(expectedValueClass, converter.getValueClass());
    }

    private void checkConverterConversions(Converter converter, QueryDataTypeFamily... expectedSupportedConversions) {
        Set<QueryDataTypeFamily> expectedSupportedConversions0 = new HashSet<>();

        expectedSupportedConversions0.add(converter.getTypeFamily());
        expectedSupportedConversions0.addAll(Arrays.asList(expectedSupportedConversions));

        for (QueryDataTypeFamily typeFamily : values()) {
            checkConverterConversion(converter, typeFamily, expectedSupportedConversions0.contains(typeFamily));
        }
    }

    private void checkConverterConversion(Converter converter, QueryDataTypeFamily typeFamily, boolean expected) {
        assertEquals(typeFamily + ": " + expected, expected, converter.canConvertTo(typeFamily));

        switch (typeFamily) {
            case VARCHAR:
                assertEquals(expected, converter.canConvertToVarchar());

                break;

            case BOOLEAN:
                assertEquals(expected, converter.canConvertToBoolean());

                break;

            case TINYINT:
                assertEquals(expected, converter.canConvertToTinyint());

                break;

            case SMALLINT:
                assertEquals(expected, converter.canConvertToSmallint());

                break;

            case INTEGER:
                assertEquals(expected, converter.canConvertToInt());

                break;

            case BIGINT:
                assertEquals(expected, converter.canConvertToBigint());

                break;

            case DECIMAL:
                assertEquals(expected, converter.canConvertToDecimal());

                break;

            case REAL:
                assertEquals(expected, converter.canConvertToReal());

                break;

            case DOUBLE:
                assertEquals(expected, converter.canConvertToDouble());

                break;

            case TIME:
                assertEquals(expected, converter.canConvertToTime());

                break;

            case DATE:
                assertEquals(expected, converter.canConvertToDate());

                break;

            case TIMESTAMP:
                assertEquals(expected, converter.canConvertToTimestamp());

                break;

            case TIMESTAMP_WITH_TIME_ZONE:
                assertEquals(expected, converter.canConvertToTimestampWithTimezone());

                break;

            case OBJECT:
                assertEquals(expected, converter.canConvertToObject());

                break;

            case JSON:
                assertEquals(expected, converter.canConvertToJson());

                break;
        }

        if (!expected) {
            checkCannotConvert(converter, typeFamily);
        }
    }

    private void checkCannotConvert(Converter converter, QueryDataTypeFamily typeFamily) {
        try {
            Object val = new Object();

            switch (typeFamily) {
                case VARCHAR:
                    converter.asVarchar(val);

                    break;

                case BOOLEAN:
                    converter.asBoolean(val);

                    break;

                case TINYINT:
                    converter.asTinyint(val);

                    break;

                case SMALLINT:
                    converter.asSmallint(val);

                    break;

                case INTEGER:
                    converter.asInt(val);

                    break;

                case BIGINT:
                    converter.asBigint(val);

                    break;

                case DECIMAL:
                    converter.asDecimal(val);

                    break;

                case REAL:
                    converter.asReal(val);

                    break;

                case DOUBLE:
                    converter.asDouble(val);

                    break;

                case TIME:
                    converter.asTime(val);

                    break;

                case DATE:
                    converter.asDate(val);

                    break;

                case TIMESTAMP:
                    converter.asTimestamp(val);

                    break;

                case TIMESTAMP_WITH_TIME_ZONE:
                    converter.asTimestampWithTimezone(val);

                    break;

                case OBJECT:
                    converter.asObject(val);

                    break;

                default:
                    return;
            }

            fail("Must fail: " + typeFamily);
        } catch (QueryException e) {
            assertEquals(SqlErrorCode.DATA_EXCEPTION, e.getCode());
        }
    }

    private void checkConverterSelf(Converter converter) {
        MockConverter mockConverter = new MockConverter();

        converter.convertToSelf(mockConverter, new Object());

        assertEquals(converter.getTypeFamily(), mockConverter.getInvoked());
    }

    private static final class MockConverter extends Converter {

        private QueryDataTypeFamily invoked;

        private MockConverter() {
            super(-1, OBJECT);
        }

        @Override
        public Class<?> getValueClass() {
            return Object.class;
        }

        @Override
        public boolean asBoolean(Object val) {
            invoked = BOOLEAN;

            return true;
        }

        @Override
        public byte asTinyint(Object val) {
            invoked = TINYINT;

            return 0;
        }

        @Override
        public short asSmallint(Object val) {
            invoked = SMALLINT;

            return 0;
        }

        @Override
        public int asInt(Object val) {
            invoked = INTEGER;

            return 0;
        }

        @Override
        public long asBigint(Object val) {
            invoked = BIGINT;

            return 0;
        }

        @Override
        public BigDecimal asDecimal(Object val) {
            invoked = DECIMAL;

            return BigDecimal.ZERO;
        }

        @Override
        public float asReal(Object val) {
            invoked = REAL;

            return 0.0f;
        }

        @Override
        public double asDouble(Object val) {
            invoked = DOUBLE;

            return 0.0d;
        }

        @Override
        public String asVarchar(Object val) {
            invoked = VARCHAR;

            return "";
        }

        @Override
        public LocalDate asDate(Object val) {
            invoked = DATE;

            return LocalDate.now();
        }

        @Override
        public LocalTime asTime(Object val) {
            invoked = TIME;

            return LocalTime.now();
        }

        @Override
        public LocalDateTime asTimestamp(Object val) {
            invoked = TIMESTAMP;

            return LocalDateTime.now();
        }

        @Override
        public OffsetDateTime asTimestampWithTimezone(Object val) {
            invoked = TIMESTAMP_WITH_TIME_ZONE;

            return OffsetDateTime.now();
        }

        @Override
        public Object asObject(Object val) {
            invoked = OBJECT;

            return new Object();
        }

        @Override
        public HazelcastJsonValue asJson(Object val) {
            invoked = JSON;

            return new HazelcastJsonValue("");
        }

        @Override
        public Object convertToSelf(Converter converter, Object val) {
            return val;
        }

        private QueryDataTypeFamily getInvoked() {
            return invoked;
        }
    }
}
