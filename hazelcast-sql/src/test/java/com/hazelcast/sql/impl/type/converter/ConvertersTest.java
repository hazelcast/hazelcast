/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.sql.impl.type.converter;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlCustomClass;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.RowValue;
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
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.BIGINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DATE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DECIMAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.DOUBLE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.JSON;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.MAP;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.NULL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.REAL;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.ROW;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.SMALLINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIME;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.TINYINT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.VARCHAR;
import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.DECIMAL_MATH_CONTEXT;
import static com.hazelcast.test.HazelcastTestSupport.assertContains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConvertersTest {

    @Test
    public void testGetById() {
        checkId(StringConverter.INSTANCE, Converter.ID_STRING);
        checkId(CharacterConverter.INSTANCE, Converter.ID_CHARACTER);

        checkId(BooleanConverter.INSTANCE, Converter.ID_BOOLEAN);

        checkId(ByteConverter.INSTANCE, Converter.ID_BYTE);
        checkId(ShortConverter.INSTANCE, Converter.ID_SHORT);
        checkId(IntegerConverter.INSTANCE, Converter.ID_INTEGER);
        checkId(LongConverter.INSTANCE, Converter.ID_LONG);
        checkId(BigDecimalConverter.INSTANCE, Converter.ID_BIG_DECIMAL);
        checkId(BigIntegerConverter.INSTANCE, Converter.ID_BIG_INTEGER);
        checkId(FloatConverter.INSTANCE, Converter.ID_FLOAT);
        checkId(DoubleConverter.INSTANCE, Converter.ID_DOUBLE);

        checkId(LocalTimeConverter.INSTANCE, Converter.ID_LOCAL_TIME);
        checkId(LocalDateConverter.INSTANCE, Converter.ID_LOCAL_DATE);
        checkId(LocalDateTimeConverter.INSTANCE, Converter.ID_LOCAL_DATE_TIME);
        checkId(DateConverter.INSTANCE, Converter.ID_DATE);
        checkId(CalendarConverter.INSTANCE, Converter.ID_CALENDAR);
        checkId(InstantConverter.INSTANCE, Converter.ID_INSTANT);
        checkId(OffsetDateTimeConverter.INSTANCE, Converter.ID_OFFSET_DATE_TIME);
        checkId(ZonedDateTimeConverter.INSTANCE, Converter.ID_ZONED_DATE_TIME);

        checkId(ObjectConverter.INSTANCE, Converter.ID_OBJECT);

        checkId(NullConverter.INSTANCE, Converter.ID_NULL);

        checkId(MapConverter.INSTANCE, Converter.ID_MAP);
        checkId(JsonConverter.INSTANCE, Converter.ID_JSON);
        checkId(RowConverter.INSTANCE, Converter.ID_ROW);
    }

    @Test
    public void testGetByClass() {
        checkClasses(StringConverter.INSTANCE, String.class);
        checkClasses(CharacterConverter.INSTANCE, char.class, Character.class);

        checkClasses(BooleanConverter.INSTANCE, boolean.class, Boolean.class);

        checkClasses(ByteConverter.INSTANCE, byte.class, Byte.class);
        checkClasses(ShortConverter.INSTANCE, short.class, Short.class);
        checkClasses(IntegerConverter.INSTANCE, int.class, Integer.class);
        checkClasses(LongConverter.INSTANCE, long.class, Long.class);
        checkClasses(BigDecimalConverter.INSTANCE, BigDecimal.class);
        checkClasses(BigIntegerConverter.INSTANCE, BigInteger.class);
        checkClasses(FloatConverter.INSTANCE, float.class, Float.class);
        checkClasses(DoubleConverter.INSTANCE, double.class, Double.class);

        checkClasses(LocalTimeConverter.INSTANCE, LocalTime.class);
        checkClasses(LocalDateConverter.INSTANCE, LocalDate.class);
        checkClasses(LocalDateTimeConverter.INSTANCE, LocalDateTime.class);
        checkClasses(DateConverter.INSTANCE, Date.class);
        checkClasses(CalendarConverter.INSTANCE, Calendar.class, GregorianCalendar.class);
        checkClasses(InstantConverter.INSTANCE, Instant.class);
        checkClasses(OffsetDateTimeConverter.INSTANCE, OffsetDateTime.class);
        checkClasses(ZonedDateTimeConverter.INSTANCE, ZonedDateTime.class);

        checkClasses(ObjectConverter.INSTANCE, Object.class, SqlCustomClass.class);

        checkClasses(NullConverter.INSTANCE, void.class, Void.class);

        checkClasses(MapConverter.INSTANCE, Map.class, HashMap.class);
        checkClasses(JsonConverter.INSTANCE, HazelcastJsonValue.class);
        checkClasses(RowConverter.INSTANCE, RowValue.class);
    }

    @Test
    public void testTypeFamilies() {
        checkFamily(StringConverter.INSTANCE, VARCHAR);
        checkFamily(CharacterConverter.INSTANCE, VARCHAR);

        checkFamily(BooleanConverter.INSTANCE, BOOLEAN);

        checkFamily(ByteConverter.INSTANCE, TINYINT);
        checkFamily(ShortConverter.INSTANCE, SMALLINT);
        checkFamily(IntegerConverter.INSTANCE, INTEGER);
        checkFamily(LongConverter.INSTANCE, BIGINT);
        checkFamily(BigDecimalConverter.INSTANCE, DECIMAL);
        checkFamily(BigIntegerConverter.INSTANCE, DECIMAL);
        checkFamily(FloatConverter.INSTANCE, REAL);
        checkFamily(DoubleConverter.INSTANCE, DOUBLE);

        checkFamily(LocalTimeConverter.INSTANCE, TIME);
        checkFamily(LocalDateConverter.INSTANCE, DATE);
        checkFamily(LocalDateTimeConverter.INSTANCE, TIMESTAMP);
        checkFamily(DateConverter.INSTANCE, TIMESTAMP_WITH_TIME_ZONE);
        checkFamily(CalendarConverter.INSTANCE, TIMESTAMP_WITH_TIME_ZONE);
        checkFamily(InstantConverter.INSTANCE, TIMESTAMP_WITH_TIME_ZONE);
        checkFamily(OffsetDateTimeConverter.INSTANCE, TIMESTAMP_WITH_TIME_ZONE);
        checkFamily(ZonedDateTimeConverter.INSTANCE, TIMESTAMP_WITH_TIME_ZONE);

        checkFamily(ObjectConverter.INSTANCE, OBJECT);

        checkFamily(NullConverter.INSTANCE, NULL);

        checkFamily(MapConverter.INSTANCE, MAP);
        checkFamily(JsonConverter.INSTANCE, JSON);
        checkFamily(RowConverter.INSTANCE, ROW);
    }

    @Test
    @SuppressWarnings("SimplifiableAssertion")
    public void testStringConverter() {
        StringConverter converter = StringConverter.INSTANCE;

        checkConverterConversions(converter, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL,
                REAL, DOUBLE, TIME, DATE, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE, OBJECT, JSON);

        // Boolean
        assertEquals(false, converter.asBoolean("false"));
        assertEquals(false, converter.asBoolean("False"));
        assertEquals(true, converter.asBoolean("true"));
        assertEquals(true, converter.asBoolean("True"));
        checkDataException(() -> converter.asBoolean("0"));
        checkDataException(() -> converter.asBoolean("1"));

        // Numeric
        String invalid = "invalid";
        String bigValue = BigDecimal.valueOf(Long.MAX_VALUE).add(new BigDecimal("1.1")).toPlainString();

        assertEquals((byte) 1, converter.asTinyint("1"));
        checkDataException(() -> converter.asTinyint(invalid));
        checkDataException(() -> converter.asTinyint(bigValue));

        assertEquals((short) 1, converter.asSmallint("1"));
        checkDataException(() -> converter.asSmallint(invalid));
        checkDataException(() -> converter.asSmallint(bigValue));

        assertEquals(1, converter.asInt("1"));
        checkDataException(() -> converter.asInt(invalid));
        checkDataException(() -> converter.asInt(bigValue));

        assertEquals(1L, converter.asBigint("1"));
        checkDataException(() -> converter.asBigint(invalid));
        checkDataException(() -> converter.asBigint(bigValue));

        assertEquals(new BigDecimal("1.1"), converter.asDecimal("1.1"));
        checkDataException(() -> converter.asDecimal(invalid));

        assertEquals(1.1f, converter.asReal("1.1"), 0);
        checkDataException(() -> converter.asReal(invalid));

        assertEquals(1.1d, converter.asDouble("1.1"), 0);
        checkDataException(() -> converter.asDouble(invalid));

        // Temporal
        assertEquals(LocalTime.parse("01:02"), converter.asTime("1:2"));
        assertEquals(LocalTime.parse("01:22"), converter.asTime("1:22"));
        assertEquals(LocalTime.parse("11:02"), converter.asTime("11:2"));
        assertEquals(LocalTime.parse("11:22"), converter.asTime("11:22"));
        assertEquals(LocalTime.parse("11:22:33"), converter.asTime("11:22:33"));
        assertEquals(LocalTime.parse("11:22:33.444"), converter.asTime("11:22:33.444"));
        assertEquals(LocalTime.parse("11:22:33.444444444"), converter.asTime("11:22:33.444444444"));
        checkDataException(() -> converter.asTime("33:22"));
        checkDataException(() -> converter.asTime("11:66"));
        checkDataException(() -> converter.asTime("11:22:66"));
        checkDataException(() -> converter.asTime("11:22:33.4444444444"));
        checkDataException(() -> converter.asTime(invalid));

        assertEquals(LocalDate.parse("2020-01-01"), converter.asDate("2020-01-01"));
        assertEquals(LocalDate.parse("2020-01-01"), converter.asDate("2020-1-01"));
        assertEquals(LocalDate.parse("2020-01-01"), converter.asDate("2020-01-1"));
        assertEquals(LocalDate.parse("2020-01-01"), converter.asDate("2020-1-1"));
        assertEquals(LocalDate.parse("2020-12-01"), converter.asDate("2020-12-1"));
        assertEquals(LocalDate.parse("2020-01-12"), converter.asDate("2020-1-12"));
        assertEquals(LocalDate.parse("2020-09-12"), converter.asDate("2020-09-12"));
        assertEquals(LocalDate.parse("2020-09-12"), converter.asDate("2020-9-12"));
        checkDataException(() -> converter.asDate("2020-13-01"));
        checkDataException(() -> converter.asDate("2020-01-35"));
        checkDataException(() -> converter.asDate("2020-13-1"));
        checkDataException(() -> converter.asDate("2020-1-35"));
        checkDataException(() -> converter.asDate(invalid));

        assertEquals(LocalDateTime.parse("2020-01-01T11:22"), converter.asTimestamp("2020-01-01T11:22"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33"), converter.asTimestamp("2020-01-01T11:22:33"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33.444"), converter.asTimestamp("2020-01-01T11:22:33.444"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33.444444444"), converter.asTimestamp("2020-01-01T11:22:33.444444444"));
        checkDataException(() -> converter.asTimestamp("2020-13-01T11:22"));
        checkDataException(() -> converter.asTimestamp("2020-01-35T11:22"));
        checkDataException(() -> converter.asTimestamp("2020-01-01T33:22"));
        checkDataException(() -> converter.asTimestamp("2020-01-01T11:66"));
        checkDataException(() -> converter.asTimestamp("2020-01-01T11:22:66"));
        checkDataException(() -> converter.asTimestamp("2020-01-01T11:22:33.4444444444"));
        checkDataException(() -> converter.asTimestamp(invalid));

        assertEquals(LocalDateTime.parse("2020-01-01T11:22"), converter.asTimestamp("2020-01-01 11:22"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33"), converter.asTimestamp("2020-01-01 11:22:33"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33.444"), converter.asTimestamp("2020-01-01 11:22:33.444"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33.444444444"), converter.asTimestamp("2020-01-01 11:22:33.444444444"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22"), converter.asTimestamp("2020-01-01 11:22"));
        assertEquals(LocalDateTime.parse("2020-01-01T01:22:33"), converter.asTimestamp("2020-01-01 1:22:33"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33.444"), converter.asTimestamp("2020-01-01 11:22:33.444"));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33.444444444"), converter.asTimestamp("2020-01-01 11:22:33.444444444"));
        checkDataException(() -> converter.asTimestamp("2020-13-01 11:22"));
        checkDataException(() -> converter.asTimestamp("2020-13-01 11:22"));
        checkDataException(() -> converter.asTimestamp("2020-01-35 11:22"));
        checkDataException(() -> converter.asTimestamp("2020-01-1 33:22"));
        checkDataException(() -> converter.asTimestamp("2020-01-1 11:66"));
        checkDataException(() -> converter.asTimestamp("2020-01-1 T11:22:66"));
        checkDataException(() -> converter.asTimestamp("2020-01-01 11:22:33.4444444444"));

        assertEquals(OffsetDateTime.parse("2020-01-01T11:22:33.444444444Z"),
                converter.asTimestampWithTimezone("2020-01-01T11:22:33.444444444Z"));
        assertEquals(OffsetDateTime.parse("2020-01-01T11:22:33.444444444+01:00"),
                converter.asTimestampWithTimezone("2020-01-01T11:22:33.444444444+01:00"));
        checkDataException(() -> converter.asTimestampWithTimezone(invalid));

        // Object
        assertEquals("val", converter.asObject("val"));

        checkConverterSelf(converter);
    }

    @Test
    public void testCharacterConverter() {
        CharacterConverter converter = CharacterConverter.INSTANCE;

        checkConverterConversions(converter, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT,
                DECIMAL, REAL, DOUBLE, TIME, DATE, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE, OBJECT);

        char invalid = 'c';

        assertEquals((byte) 1, converter.asTinyint('1'));
        checkDataException(() -> converter.asTinyint(invalid));

        assertEquals((short) 1, converter.asSmallint('1'));
        checkDataException(() -> converter.asSmallint(invalid));

        assertEquals(1, converter.asInt('1'));
        checkDataException(() -> converter.asInt(invalid));

        assertEquals(1L, converter.asBigint('1'));
        checkDataException(() -> converter.asBigint(invalid));

        assertEquals(new BigDecimal("1"), converter.asDecimal('1'));
        checkDataException(() -> converter.asDecimal(invalid));

        assertEquals(1f, converter.asReal('1'), 0);
        checkDataException(() -> converter.asReal(invalid));

        assertEquals(1d, converter.asDouble('1'), 0);
        checkDataException(() -> converter.asDouble(invalid));

        assertEquals("c", converter.asObject('c'));

        checkConverterSelf(converter);
    }

    @Test
    public void testBooleanConverter() {
        BooleanConverter converter = BooleanConverter.INSTANCE;

        checkConverterConversions(converter, VARCHAR, OBJECT);

        assertEquals("true", converter.asVarchar(true));
        assertEquals("false", converter.asVarchar(false));

        assertEquals(true, converter.asObject(true));

        checkConverterSelf(converter);
    }

    @Test
    public void testByteConverter() {
        ByteConverter converter = ByteConverter.INSTANCE;

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

        assertEquals(new BigDecimal(val, DECIMAL_MATH_CONTEXT), converter.asDecimal(val));

        assertEquals(1.1f, converter.asReal(val), 0);
        assertEquals(1.1f, converter.asDouble(val), 0);

        assertEquals(1.1f, converter.asObject(val));

        checkConverterSelf(converter);
    }

    @Test
    public void testDoubleConverter() {
        DoubleConverter converter = DoubleConverter.INSTANCE;

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

        assertEquals(new BigDecimal(val, DECIMAL_MATH_CONTEXT), converter.asDecimal(val));

        assertEquals(1.1f, converter.asReal(val), 0);
        assertEquals(1.1d, converter.asDouble(val), 0);

        assertEquals(1.1d, converter.asObject(val));

        checkConverterSelf(converter);
    }

    @Test
    public void testLocalTimeConverter() {
        LocalTimeConverter converter = LocalTimeConverter.INSTANCE;

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

        checkConverterConversions(converter, VARCHAR, TIME, DATE, TIMESTAMP, OBJECT);

        Date val = new Date();

        checkTimestampWithTimezone(converter, val, OffsetDateTime.ofInstant(val.toInstant(), ZoneId.systemDefault()));

        checkConverterSelf(converter);
    }

    @Test
    public void testCalendarConverter() {
        CalendarConverter converter = CalendarConverter.INSTANCE;

        checkConverterConversions(converter, VARCHAR, TIME, DATE, TIMESTAMP, OBJECT);

        Calendar val = Calendar.getInstance();

        checkTimestampWithTimezone(converter, val, OffsetDateTime.ofInstant(val.toInstant(), val.getTimeZone().toZoneId()));

        checkConverterSelf(converter);
    }

    @Test
    public void testInstantConverter() {
        InstantConverter converter = InstantConverter.INSTANCE;

        checkConverterConversions(converter, VARCHAR, TIME, DATE, TIMESTAMP, OBJECT);

        Instant val = Instant.now();

        checkTimestampWithTimezone(converter, val, OffsetDateTime.ofInstant(val, ZoneId.systemDefault()));

        checkConverterSelf(converter);
    }

    @Test
    public void testOffsetDateTimeConverter() {
        OffsetDateTimeConverter converter = OffsetDateTimeConverter.INSTANCE;

        checkConverterConversions(converter, VARCHAR, TIME, DATE, TIMESTAMP, OBJECT);

        OffsetDateTime val = OffsetDateTime.now();

        checkTimestampWithTimezone(converter, val, val);

        checkConverterSelf(converter);
    }

    @Test
    public void testZonedDateTimeConverter() {
        ZonedDateTimeConverter converter = ZonedDateTimeConverter.INSTANCE;

        checkConverterConversions(converter, VARCHAR, TIME, DATE, TIMESTAMP, OBJECT);

        ZonedDateTime val = ZonedDateTime.now();

        checkTimestampWithTimezone(converter, val, val.toOffsetDateTime());

        checkConverterSelf(converter);
    }

    @Test
    @SuppressWarnings("SimplifiableAssertion")
    public void testObjectConverter() {
        ObjectConverter converter = ObjectConverter.INSTANCE;

        checkConverterConversions(converter, VARCHAR, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT,
                DECIMAL, REAL, DOUBLE, TIME, DATE, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE, MAP, JSON, ROW);

        // Strings
        assertEquals("c", converter.asVarchar('c'));
        assertEquals("val", converter.asVarchar("val"));
        assertEquals("2020-01-01T11:22:33.444444444", converter.asVarchar(LocalDateTime.parse("2020-01-01T11:22:33.444444444")));
        assertEquals(new SqlCustomClass(1).toString(), converter.asVarchar(new SqlCustomClass(1)));

        // Boolean
        assertEquals(true, converter.asBoolean(true));
        assertEquals(true, converter.asBoolean("true"));
        assertEquals(false, converter.asBoolean(false));
        assertEquals(false, converter.asBoolean("false"));
        checkDataException(() -> converter.asBoolean("1"));
        checkDataException(() -> converter.asBoolean(1));
        checkDataException(() -> converter.asBoolean(new Object()));

        // Numeric
        String invalid = "invalid";
        String bigValue = BigDecimal.valueOf(Long.MAX_VALUE).add(new BigDecimal("1.1")).toPlainString();

        assertEquals((byte) 1, converter.asTinyint(1));
        assertEquals((byte) 1, converter.asTinyint("1"));
        checkDataException(() -> converter.asTinyint(invalid));
        checkDataException(() -> converter.asTinyint(bigValue));

        assertEquals((short) 1, converter.asSmallint(1));
        assertEquals((short) 1, converter.asSmallint("1"));
        checkDataException(() -> converter.asSmallint(invalid));
        checkDataException(() -> converter.asSmallint(bigValue));

        assertEquals(1, converter.asInt(1));
        assertEquals(1, converter.asInt("1"));
        checkDataException(() -> converter.asInt(invalid));
        checkDataException(() -> converter.asInt(bigValue));

        assertEquals(1L, converter.asBigint(1));
        assertEquals(1L, converter.asBigint("1"));
        checkDataException(() -> converter.asBigint(invalid));
        checkDataException(() -> converter.asBigint(bigValue));

        assertEquals(new BigDecimal("1.1"), converter.asDecimal(new BigDecimal("1.1")));
        assertEquals(new BigDecimal("1.1"), converter.asDecimal("1.1"));
        checkDataException(() -> converter.asDecimal(invalid));

        assertEquals(1.1f, converter.asReal(1.1f), 0);
        assertEquals(1.1f, converter.asReal("1.1"), 0);
        checkDataException(() -> converter.asReal(invalid));

        assertEquals(1.1d, converter.asDouble(1.1d), 0);
        assertEquals(1.1d, converter.asDouble("1.1"), 0);
        checkDataException(() -> converter.asDouble(invalid));

        // Temporal
        assertEquals(LocalTime.parse("11:22"), converter.asTime(LocalTime.parse("11:22")));
        assertEquals(LocalTime.parse("11:22"), converter.asTime(LocalDateTime.parse("2020-01-01T11:22")));
        assertEquals(LocalTime.parse("11:22"), converter.asTime("11:22"));
        checkDataException(() -> converter.asTime(invalid));

        assertEquals(LocalDate.parse("2020-01-01"), converter.asDate(LocalDate.parse("2020-01-01")));
        assertEquals(LocalDate.parse("2020-01-01"), converter.asDate(LocalDateTime.parse("2020-01-01T11:22:33")));
        assertEquals(LocalDate.parse("2020-01-01"), converter.asDate("2020-01-01"));
        checkDataException(() -> converter.asDate(invalid));

        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33.444444444"),
                converter.asTimestamp(LocalDateTime.parse("2020-01-01T11:22:33.444444444")));
        assertEquals(LocalDateTime.parse("2020-01-01T11:22:33.444444444"), converter.asTimestamp("2020-01-01T11:22:33.444444444"));
        checkDataException(() -> converter.asTimestamp(invalid));

        assertEquals(OffsetDateTime.parse("2020-01-01T11:22:33.444444444+01:00"),
                converter.asTimestampWithTimezone(OffsetDateTime.parse("2020-01-01T11:22:33.444444444+01:00")));
        assertEquals(OffsetDateTime.parse("2020-01-01T11:22:33.444444444+01:00"),
                converter.asTimestampWithTimezone("2020-01-01T11:22:33.444444444+01:00"));
        checkDataException(() -> converter.asTimestampWithTimezone(invalid));

        // Object
        assertEquals("val", converter.asObject("val"));
        assertEquals(1, converter.asObject(1));
        assertEquals(new SqlCustomClass(1), converter.asObject(new SqlCustomClass(1)));

        // Others
        assertEquals(Map.of("k", 1), converter.asMap(Map.of("k", 1)));
        assertEquals(new HazelcastJsonValue("[1,2,3]"), converter.asJson("[1,2,3]"));
        assertEquals(new RowValue(List.of(1, 2, 3)), converter.asRow(new RowValue(List.of(1, 2, 3))));

        checkConverterSelf(converter);
    }

    @Test
    public void testNullConverter() {
        NullConverter converter = NullConverter.INSTANCE;

        checkConverterConversions(converter, VARCHAR, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL,
                REAL, DOUBLE, TIME, DATE, TIMESTAMP, TIMESTAMP_WITH_TIME_ZONE, OBJECT, MAP, JSON, ROW);

        checkUnsupportedException(() -> converter.asVarchar(null));
        checkUnsupportedException(() -> converter.asBoolean(null));
        checkUnsupportedException(() -> converter.asTinyint(null));
        checkUnsupportedException(() -> converter.asSmallint(null));
        checkUnsupportedException(() -> converter.asInt(null));
        checkUnsupportedException(() -> converter.asBigint(null));
        checkUnsupportedException(() -> converter.asDecimal(null));
        checkUnsupportedException(() -> converter.asReal(null));
        checkUnsupportedException(() -> converter.asDouble(null));
        checkUnsupportedException(() -> converter.asTime(null));
        checkUnsupportedException(() -> converter.asDate(null));
        checkUnsupportedException(() -> converter.asTimestamp(null));
        checkUnsupportedException(() -> converter.asTimestampWithTimezone(null));
        checkUnsupportedException(() -> converter.asObject(null));
        checkUnsupportedException(() -> converter.asMap(null));
        checkUnsupportedException(() -> converter.asJson(null));
        checkUnsupportedException(() -> converter.asRow(null));

        checkUnsupportedException(() -> converter.convertToSelf(converter, null));
    }

    @Test
    public void testMapConverter() {
        MapConverter converter = MapConverter.INSTANCE;

        checkConverterConversions(converter, VARCHAR, OBJECT, MAP);

        assertEquals("{k=1}", converter.asVarchar(Map.of("k", 1)));

        checkConverterSelf(converter);
    }

    @Test
    public void testJsonConverter() {
        JsonConverter converter = JsonConverter.INSTANCE;

        checkConverterConversions(converter, VARCHAR, OBJECT, JSON);

        assertEquals("[1,2,3]", converter.asVarchar(new HazelcastJsonValue("[1,2,3]")));

        assertEquals(new HazelcastJsonValue("[1,2,3]"), converter.asObject(new HazelcastJsonValue("[1,2,3]")));

        checkConverterSelf(converter);
    }

    @Test
    public void testRowConverter() {
        RowConverter converter = RowConverter.INSTANCE;

        checkConverterConversions(converter, VARCHAR, OBJECT, ROW);

        assertEquals("[1, 2, 3]", converter.asVarchar(new RowValue(List.of(1, 2, 3))));

        checkConverterSelf(converter);
    }

    private void checkId(Converter converter, int id) {
        assertSame(id, converter.getId());
        assertSame(converter, Converters.getConverter(id));
    }

    private void checkClasses(Converter converter, Class<?>... classes) {
        assertContains(List.of(classes), converter.getValueClass());
        for (Class<?> clazz : classes) {
            assertSame(converter, Converters.getConverter(clazz));
        }
    }

    private void checkFamily(Converter converter, QueryDataTypeFamily expectedTypeFamily) {
        assertSame(expectedTypeFamily, converter.getTypeFamily());
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
        } catch (UnsupportedOperationException ignore) { }
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

    private void checkConverterConversions(Converter converter, QueryDataTypeFamily... expectedSupportedConversions) {
        Set<QueryDataTypeFamily> expectedSupportedConversions0 = new HashSet<>();

        expectedSupportedConversions0.add(converter.getTypeFamily());
        expectedSupportedConversions0.addAll(Arrays.asList(expectedSupportedConversions));

        for (QueryDataTypeFamily typeFamily : QueryDataTypeFamily.values()) {
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

            case MAP:
                assertEquals(expected, converter.canConvertToMap());
                break;

            case JSON:
                assertEquals(expected, converter.canConvertToJson());
                break;

            case ROW:
                assertEquals(expected, converter.canConvertToRow());
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

                case MAP:
                    converter.asMap(val);
                    break;

                case JSON:
                    converter.asJson(val);
                    break;

                case ROW:
                    converter.asRow(val);
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
        public Map<?, ?> asMap(Object val) {
            invoked = MAP;
            return Collections.emptyMap();
        }

        @Override
        public HazelcastJsonValue asJson(Object val) {
            invoked = JSON;
            return new HazelcastJsonValue("");
        }

        @Override
        public RowValue asRow(Object val) {
            invoked = ROW;
            return new RowValue();
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
