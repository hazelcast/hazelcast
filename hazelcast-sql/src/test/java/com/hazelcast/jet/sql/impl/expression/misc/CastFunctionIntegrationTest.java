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

package com.hazelcast.jet.sql.impl.expression.misc;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.CastExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.LocalDateConverter;
import com.hazelcast.sql.impl.type.converter.LocalDateTimeConverter;
import com.hazelcast.sql.impl.type.converter.LocalTimeConverter;
import com.hazelcast.sql.impl.type.converter.OffsetDateTimeConverter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static com.hazelcast.sql.SqlColumnType.BIGINT;
import static com.hazelcast.sql.SqlColumnType.BOOLEAN;
import static com.hazelcast.sql.SqlColumnType.DATE;
import static com.hazelcast.sql.SqlColumnType.DECIMAL;
import static com.hazelcast.sql.SqlColumnType.DOUBLE;
import static com.hazelcast.sql.SqlColumnType.INTEGER;
import static com.hazelcast.sql.SqlColumnType.JSON;
import static com.hazelcast.sql.SqlColumnType.OBJECT;
import static com.hazelcast.sql.SqlColumnType.REAL;
import static com.hazelcast.sql.SqlColumnType.SMALLINT;
import static com.hazelcast.sql.SqlColumnType.TIME;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.SqlColumnType.TINYINT;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;
import static com.hazelcast.sql.impl.SqlErrorCode.DATA_EXCEPTION;
import static com.hazelcast.sql.impl.SqlErrorCode.PARSING;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CastFunctionIntegrationTest extends ExpressionTestSupport {
    @Test
    public void testVarchar_char() {
        putAndCheckValue(new ExpressionValue.CharacterVal(), sql("field1", VARCHAR), VARCHAR, null);
        putAndCheckValue('b', sql("this", VARCHAR), VARCHAR, "b");
        putAndCheckFailure('b', sql("this", BOOLEAN), DATA_EXCEPTION, "Cannot parse VARCHAR value to BOOLEAN");
        putAndCheckFailure('b', sql("this", TINYINT), DATA_EXCEPTION, "Cannot parse VARCHAR value to TINYINT");
        putAndCheckFailure('b', sql("this", SMALLINT), DATA_EXCEPTION, "Cannot parse VARCHAR value to SMALLINT");
        putAndCheckFailure('b', sql("this", INTEGER), DATA_EXCEPTION, "Cannot parse VARCHAR value to INTEGER");
        putAndCheckFailure('b', sql("this", BIGINT), DATA_EXCEPTION, "Cannot parse VARCHAR value to BIGINT");
        putAndCheckFailure('b', sql("this", DECIMAL), DATA_EXCEPTION, "Cannot parse VARCHAR value to DECIMAL");
        putAndCheckFailure('b', sql("this", REAL), DATA_EXCEPTION, "Cannot parse VARCHAR value to REAL");
        putAndCheckFailure('b', sql("this", DOUBLE), DATA_EXCEPTION, "Cannot parse VARCHAR value to DOUBLE");
        putAndCheckFailure('b', sql("this", DATE), DATA_EXCEPTION, "Cannot parse VARCHAR value to DATE");
        putAndCheckFailure('b', sql("this", TIME), DATA_EXCEPTION, "Cannot parse VARCHAR value to TIME");
        putAndCheckFailure('b', sql("this", TIMESTAMP), DATA_EXCEPTION, "Cannot parse VARCHAR value to TIMESTAMP");
        putAndCheckFailure('b', sql("this", TIMESTAMP_WITH_TIME_ZONE), DATA_EXCEPTION, "Cannot parse VARCHAR value to TIMESTAMP WITH TIME ZONE");
        putAndCheckValue('b', sql("this", OBJECT), OBJECT, "b");
        putAndCheckValue('b', sql("this", JSON), JSON, new HazelcastJsonValue("b"));
    }

    @Test
    public void testVarchar_string() {
        // Character
        putAndCheckValue('f', sql("this", VARCHAR), VARCHAR, "f");

        // String
        putAndCheckValue(new ExpressionValue.StringVal(), sql("field1", VARCHAR), VARCHAR, null);
        putAndCheckValue("foo", sql("this", VARCHAR), VARCHAR, "foo");

        // Boolean
        putAndCheckValue(new ExpressionValue.StringVal(), sql("field1", BOOLEAN), BOOLEAN, null);
        putAndCheckValue("true", sql("this", BOOLEAN), BOOLEAN, true);
        putAndCheckValue("false", sql("this", BOOLEAN), BOOLEAN, false);
        putAndCheckFailure("bad", sql("this", BOOLEAN), DATA_EXCEPTION, "Cannot parse VARCHAR value to BOOLEAN");

        // Byte
        putAndCheckValue(new ExpressionValue.StringVal(), sql("field1", TINYINT), TINYINT, null);
        putAndCheckValue(string(0), sql("this", TINYINT), TINYINT, (byte) 0);
        putAndCheckValue(string(Byte.MAX_VALUE), sql("this", TINYINT), TINYINT, Byte.MAX_VALUE);
        putAndCheckValue(string(Byte.MIN_VALUE), sql("this", TINYINT), TINYINT, Byte.MIN_VALUE);
        putAndCheckFailure(string(Short.MAX_VALUE), sql("this", TINYINT), DATA_EXCEPTION, "Cannot parse VARCHAR value to TINYINT");
        putAndCheckFailure(string(Short.MIN_VALUE), sql("this", TINYINT), DATA_EXCEPTION, "Cannot parse VARCHAR value to TINYINT");
        putAndCheckFailure("bad", sql("this", TINYINT), DATA_EXCEPTION, "Cannot parse VARCHAR value to TINYINT");

        // Short
        putAndCheckValue(new ExpressionValue.StringVal(), sql("field1", SMALLINT), SMALLINT, null);
        putAndCheckValue(string(0), sql("this", SMALLINT), SMALLINT, (short) 0);
        putAndCheckValue(string(Short.MAX_VALUE), sql("this", SMALLINT), SMALLINT, Short.MAX_VALUE);
        putAndCheckValue(string(Short.MIN_VALUE), sql("this", SMALLINT), SMALLINT, Short.MIN_VALUE);
        putAndCheckFailure(string(Integer.MAX_VALUE), sql("this", SMALLINT), DATA_EXCEPTION, "Cannot parse VARCHAR value to SMALLINT");
        putAndCheckFailure(string(Integer.MIN_VALUE), sql("this", SMALLINT), DATA_EXCEPTION, "Cannot parse VARCHAR value to SMALLINT");
        putAndCheckFailure("bad", sql("this", SMALLINT), DATA_EXCEPTION, "Cannot parse VARCHAR value to SMALLINT");

        // Integer
        putAndCheckValue(new ExpressionValue.StringVal(), sql("field1", INTEGER), INTEGER, null);
        putAndCheckValue(string(0), sql("this", INTEGER), INTEGER, 0);
        putAndCheckValue(string(Integer.MAX_VALUE), sql("this", INTEGER), INTEGER, Integer.MAX_VALUE);
        putAndCheckValue(string(Integer.MIN_VALUE), sql("this", INTEGER), INTEGER, Integer.MIN_VALUE);
        putAndCheckFailure(string(Long.MAX_VALUE), sql("this", INTEGER), DATA_EXCEPTION, "Cannot parse VARCHAR value to INTEGER");
        putAndCheckFailure(string(Long.MIN_VALUE), sql("this", INTEGER), DATA_EXCEPTION, "Cannot parse VARCHAR value to INTEGER");
        putAndCheckFailure("bad", sql("this", INTEGER), DATA_EXCEPTION, "Cannot parse VARCHAR value to INTEGER");

        // Long
        putAndCheckValue(new ExpressionValue.StringVal(), sql("field1", BIGINT), BIGINT, null);
        putAndCheckValue(string(0), sql("this", BIGINT), BIGINT, 0L);
        putAndCheckValue(string(Long.MAX_VALUE), sql("this", BIGINT), BIGINT, Long.MAX_VALUE);
        putAndCheckValue(string(Long.MIN_VALUE), sql("this", BIGINT), BIGINT, Long.MIN_VALUE);
        putAndCheckFailure(string(decimal(Long.MAX_VALUE).multiply(decimal(2))), sql("this", BIGINT), DATA_EXCEPTION, "Cannot parse VARCHAR value to BIGINT");
        putAndCheckFailure(string(decimal(Long.MIN_VALUE).multiply(decimal(2))), sql("this", BIGINT), DATA_EXCEPTION, "Cannot parse VARCHAR value to BIGINT");
        putAndCheckFailure("bad", sql("this", BIGINT), DATA_EXCEPTION, "Cannot parse VARCHAR value to BIGINT");

        // BigInteger/BigDecimal
        putAndCheckValue(new ExpressionValue.StringVal(), sql("field1", DECIMAL), DECIMAL, null);
        putAndCheckValue(string(1), sql("this", DECIMAL), DECIMAL, BigDecimal.ONE);
        putAndCheckValue(string("1.1"), sql("this", DECIMAL), DECIMAL, decimal("1.1"));
        putAndCheckFailure("bad", sql("this", DECIMAL), DATA_EXCEPTION, "Cannot parse VARCHAR value to DECIMAL");


        // Float
        putAndCheckValue(new ExpressionValue.StringVal(), sql("field1", REAL), REAL, null);
        putAndCheckValue(string("1.1"), sql("this", REAL), REAL, 1.1f);
        putAndCheckFailure("bad", sql("this", REAL), DATA_EXCEPTION, "Cannot parse VARCHAR value to REAL");

        // Double
        putAndCheckValue(new ExpressionValue.StringVal(), sql("field1", DOUBLE), DOUBLE, null);
        putAndCheckValue(string("1.1"), sql("this", DOUBLE), DOUBLE, 1.1d);
        putAndCheckFailure("bad", sql("this", DOUBLE), DATA_EXCEPTION, "Cannot parse VARCHAR value to DOUBLE");

        // LocalDate
        putAndCheckValue(new ExpressionValue.StringVal(), sql("field1", DATE), DATE, null);
        putAndCheckValue(LOCAL_DATE_VAL.toString(), sql("this", DATE), DATE, LOCAL_DATE_VAL);
        // SQL standard also accepts DATE without leading zeroes to DATE.
        putAndCheckValue(STANDARD_LOCAL_DATE_VAL, sql("this", DATE), DATE, LOCAL_DATE_VAL);
        putAndCheckFailure("bad", sql("this", DATE), DATA_EXCEPTION, "Cannot parse VARCHAR value to DATE");

        // LocalTime
        putAndCheckValue(new ExpressionValue.StringVal(), sql("field1", TIME), TIME, null);
        putAndCheckValue(LOCAL_TIME_VAL.toString(), sql("this", TIME), TIME, LOCAL_TIME_VAL);
        putAndCheckValue(STANDARD_LOCAL_TIME_VAL, sql("this", TIME), TIME, LOCAL_TIME_VAL);
        putAndCheckFailure("bad", sql("this", TIME), DATA_EXCEPTION, "Cannot parse VARCHAR value to TIME");

        // LocalDateTime
        putAndCheckValue(new ExpressionValue.StringVal(), sql("field1", TIMESTAMP), TIMESTAMP, null);
        putAndCheckValue(LOCAL_DATE_TIME_VAL.toString(), sql("this", TIMESTAMP), TIMESTAMP, LOCAL_DATE_TIME_VAL);
        putAndCheckValue(STANDARD_LOCAL_DATE_TIME_VAL, sql("this", TIMESTAMP), TIMESTAMP, LOCAL_DATE_TIME_VAL);
        putAndCheckFailure("bad", sql("this", TIMESTAMP), DATA_EXCEPTION, "Cannot parse VARCHAR value to TIMESTAMP");

        // OffsetDateTime
        putAndCheckValue(new ExpressionValue.StringVal(), sql("field1", TIMESTAMP_WITH_TIME_ZONE), TIMESTAMP_WITH_TIME_ZONE, null);
        putAndCheckValue(OFFSET_DATE_TIME_VAL.toString(), sql("this", TIMESTAMP_WITH_TIME_ZONE), TIMESTAMP_WITH_TIME_ZONE, OFFSET_DATE_TIME_VAL);
        putAndCheckValue(STANDARD_LOCAL_OFFSET_TIME_VAL, sql("this", TIMESTAMP_WITH_TIME_ZONE), TIMESTAMP_WITH_TIME_ZONE, OFFSET_DATE_TIME_VAL);
        putAndCheckFailure("bad", sql("this", TIMESTAMP_WITH_TIME_ZONE), DATA_EXCEPTION, "Cannot parse VARCHAR value to TIMESTAMP WITH TIME ZONE");

        // Object
        putAndCheckValue(new ExpressionValue.StringVal(), sql("field1", OBJECT), OBJECT, null);
        putAndCheckValue("foo", sql("this", OBJECT), OBJECT, "foo");

        // JSON
        putAndCheckValue("[1,2,3]", sql("this", JSON), JSON, new HazelcastJsonValue("[1,2,3]"));
    }

    @Test
    public void testVarchar_literal() {
        put(1);

        // VARCHAR -> VARCHAR
        checkValue0(sql(stringLiteral("foo"), VARCHAR), VARCHAR, "foo");
        checkValue0(sql(stringLiteral("true"), VARCHAR), VARCHAR, "true");
        checkValue0(sql(stringLiteral("false"), VARCHAR), VARCHAR, "false");
        checkValue0(sql(stringLiteral(LOCAL_DATE_VAL), VARCHAR), VARCHAR, LOCAL_DATE_VAL.toString());
        checkValue0(sql(stringLiteral(LOCAL_TIME_VAL), VARCHAR), VARCHAR, LOCAL_TIME_VAL.toString());
        checkValue0(sql(stringLiteral(LOCAL_DATE_TIME_VAL), VARCHAR), VARCHAR, LOCAL_DATE_TIME_VAL.toString());
        checkValue0(sql(stringLiteral(OFFSET_DATE_TIME_VAL), VARCHAR), VARCHAR, OFFSET_DATE_TIME_VAL.toString());

        // VARCHAR -> BOOLEAN
        checkFailure0(sql(stringLiteral("foo"), BOOLEAN), PARSING, "CAST function cannot convert literal 'foo' to type BOOLEAN: Cannot parse VARCHAR value to BOOLEAN");
        checkFailure0(sql(stringLiteral("null"), BOOLEAN), PARSING, "CAST function cannot convert literal 'null' to type BOOLEAN: Cannot parse VARCHAR value to BOOLEAN");
        checkValue0(sql(stringLiteral("true"), BOOLEAN), BOOLEAN, true);
        checkValue0(sql(stringLiteral("True"), BOOLEAN), BOOLEAN, true);
        checkValue0(sql(stringLiteral("false"), BOOLEAN), BOOLEAN, false);
        checkValue0(sql(stringLiteral("False"), BOOLEAN), BOOLEAN, false);

        // VARCHAR -> TINYINT
        checkValue0(sql(stringLiteral(1), TINYINT), TINYINT, (byte) 1);
        checkValue0(sql(stringLiteral(Byte.MAX_VALUE), TINYINT), TINYINT, Byte.MAX_VALUE);
        checkValue0(sql(stringLiteral(Byte.MIN_VALUE), TINYINT), TINYINT, Byte.MIN_VALUE);
        checkFailure0(sql(stringLiteral(Short.MAX_VALUE), TINYINT), PARSING, "CAST function cannot convert literal '32767' to type TINYINT: Cannot parse VARCHAR value to TINYINT");
        checkFailure0(sql(stringLiteral(Short.MIN_VALUE), TINYINT), PARSING, "CAST function cannot convert literal '-32768' to type TINYINT: Cannot parse VARCHAR value to TINYINT");
        checkFailure0(sql(stringLiteral("foo"), TINYINT), PARSING, "CAST function cannot convert literal 'foo' to type TINYINT: Cannot parse VARCHAR value to TINYINT");
        checkFailure0(sql(stringLiteral("true"), TINYINT), PARSING, "CAST function cannot convert literal 'true' to type TINYINT: Cannot parse VARCHAR value to TINYINT");
        checkFailure0(sql(stringLiteral("false"), TINYINT), PARSING, "CAST function cannot convert literal 'false' to type TINYINT: Cannot parse VARCHAR value to TINYINT");
        checkFailure0(sql(stringLiteral("1.1"), TINYINT), PARSING, "CAST function cannot convert literal '1.1' to type TINYINT: Cannot parse VARCHAR value to TINYINT");

        // VARCHAR -> SMALLINT
        checkValue0(sql(stringLiteral(1), SMALLINT), SMALLINT, (short) 1);
        checkValue0(sql(stringLiteral(Short.MAX_VALUE), SMALLINT), SMALLINT, Short.MAX_VALUE);
        checkValue0(sql(stringLiteral(Short.MIN_VALUE), SMALLINT), SMALLINT, Short.MIN_VALUE);
        checkFailure0(sql(stringLiteral(Integer.MAX_VALUE), SMALLINT), PARSING, "CAST function cannot convert literal '2147483647' to type SMALLINT: Cannot parse VARCHAR value to SMALLINT");
        checkFailure0(sql(stringLiteral(Integer.MIN_VALUE), SMALLINT), PARSING, "CAST function cannot convert literal '-2147483648' to type SMALLINT: Cannot parse VARCHAR value to SMALLINT");
        checkFailure0(sql(stringLiteral("foo"), SMALLINT), PARSING, "CAST function cannot convert literal 'foo' to type SMALLINT: Cannot parse VARCHAR value to SMALLINT");
        checkFailure0(sql(stringLiteral("true"), SMALLINT), PARSING, "CAST function cannot convert literal 'true' to type SMALLINT: Cannot parse VARCHAR value to SMALLINT");
        checkFailure0(sql(stringLiteral("false"), SMALLINT), PARSING, "CAST function cannot convert literal 'false' to type SMALLINT: Cannot parse VARCHAR value to SMALLINT");
        checkFailure0(sql(stringLiteral("1.1"), SMALLINT), PARSING, "CAST function cannot convert literal '1.1' to type SMALLINT: Cannot parse VARCHAR value to SMALLINT");

        // VARCHAR -> INTEGER
        checkValue0(sql(stringLiteral(1), INTEGER), INTEGER, 1);
        checkValue0(sql(stringLiteral(Integer.MAX_VALUE), INTEGER), INTEGER, Integer.MAX_VALUE);
        checkValue0(sql(stringLiteral(Integer.MIN_VALUE), INTEGER), INTEGER, Integer.MIN_VALUE);
        checkFailure0(sql(stringLiteral(Long.MAX_VALUE), INTEGER), PARSING, "CAST function cannot convert literal '9223372036854775807' to type INTEGER: Cannot parse VARCHAR value to INTEGER");
        checkFailure0(sql(stringLiteral(Long.MIN_VALUE), INTEGER), PARSING, "CAST function cannot convert literal '-9223372036854775808' to type INTEGER: Cannot parse VARCHAR value to INTEGER");
        checkFailure0(sql(stringLiteral("foo"), INTEGER), PARSING, "CAST function cannot convert literal 'foo' to type INTEGER: Cannot parse VARCHAR value to INTEGER");
        checkFailure0(sql(stringLiteral("true"), INTEGER), PARSING, "CAST function cannot convert literal 'true' to type INTEGER: Cannot parse VARCHAR value to INTEGER");
        checkFailure0(sql(stringLiteral("false"), INTEGER), PARSING, "CAST function cannot convert literal 'false' to type INTEGER: Cannot parse VARCHAR value to INTEGER");
        checkFailure0(sql(stringLiteral("1.1"), INTEGER), PARSING, "CAST function cannot convert literal '1.1' to type INTEGER: Cannot parse VARCHAR value to INTEGER");

        // VARCHAR -> BIGINT
        checkValue0(sql(stringLiteral(1), BIGINT), BIGINT, 1L);
        checkValue0(sql(stringLiteral(Long.MAX_VALUE), BIGINT), BIGINT, Long.MAX_VALUE);
        checkValue0(sql(stringLiteral(Long.MIN_VALUE), BIGINT), BIGINT, Long.MIN_VALUE);
        checkFailure0(sql(stringLiteral(Long.MAX_VALUE + "0"), BIGINT), PARSING, "CAST function cannot convert literal '92233720368547758070' to type BIGINT: Cannot parse VARCHAR value to BIGINT");
        checkFailure0(sql(stringLiteral(Long.MIN_VALUE + "0"), BIGINT), PARSING, "CAST function cannot convert literal '-92233720368547758080' to type BIGINT: Cannot parse VARCHAR value to BIGINT");
        checkFailure0(sql(stringLiteral("foo"), BIGINT), PARSING, "CAST function cannot convert literal 'foo' to type BIGINT: Cannot parse VARCHAR value to BIGINT");
        checkFailure0(sql(stringLiteral("true"), BIGINT), PARSING, "CAST function cannot convert literal 'true' to type BIGINT: Cannot parse VARCHAR value to BIGINT");
        checkFailure0(sql(stringLiteral("false"), BIGINT), PARSING, "CAST function cannot convert literal 'false' to type BIGINT: Cannot parse VARCHAR value to BIGINT");
        checkFailure0(sql(stringLiteral("1.1"), BIGINT), PARSING, "CAST function cannot convert literal '1.1' to type BIGINT: Cannot parse VARCHAR value to BIGINT");

        // VARCHAR -> DECIMAL
        checkValue0(sql(stringLiteral(1), DECIMAL), DECIMAL, decimal(1));
        checkValue0(sql(stringLiteral("1.1"), DECIMAL), DECIMAL, decimal("1.1"));
        checkValue0(sql(stringLiteral(Long.MAX_VALUE + "0"), DECIMAL), DECIMAL, decimal(Long.MAX_VALUE + "0"));
        checkValue0(sql(stringLiteral(Long.MIN_VALUE + "0"), DECIMAL), DECIMAL, decimal(Long.MIN_VALUE + "0"));
        checkFailure0(sql(stringLiteral("foo"), DECIMAL), PARSING, "CAST function cannot convert literal 'foo' to type DECIMAL: Cannot parse VARCHAR value to DECIMAL");
        checkFailure0(sql(stringLiteral("true"), DECIMAL), PARSING, "CAST function cannot convert literal 'true' to type DECIMAL: Cannot parse VARCHAR value to DECIMAL");
        checkFailure0(sql(stringLiteral("false"), DECIMAL), PARSING, "CAST function cannot convert literal 'false' to type DECIMAL: Cannot parse VARCHAR value to DECIMAL");

        // VARCHAR -> REAL
        checkValue0(sql(stringLiteral(1), REAL), REAL, 1f);
        checkValue0(sql(stringLiteral("1.1"), REAL), REAL, 1.1f);
        checkFailure0(sql(stringLiteral("foo"), REAL), PARSING, "CAST function cannot convert literal 'foo' to type REAL: Cannot parse VARCHAR value to REAL");
        checkFailure0(sql(stringLiteral("true"), REAL), PARSING, "CAST function cannot convert literal 'true' to type REAL: Cannot parse VARCHAR value to REAL");
        checkFailure0(sql(stringLiteral("false"), REAL), PARSING, "CAST function cannot convert literal 'false' to type REAL: Cannot parse VARCHAR value to REAL");

        // VARCHAR -> DOUBLE
        checkValue0(sql(stringLiteral(1), DOUBLE), DOUBLE, 1d);
        checkValue0(sql(stringLiteral("1.1"), DOUBLE), DOUBLE, 1.1d);
        checkFailure0(sql(stringLiteral("foo"), DOUBLE), PARSING, "CAST function cannot convert literal 'foo' to type DOUBLE: Cannot parse VARCHAR value to DOUBLE");
        checkFailure0(sql(stringLiteral("true"), DOUBLE), PARSING, "CAST function cannot convert literal 'true' to type DOUBLE: Cannot parse VARCHAR value to DOUBLE");
        checkFailure0(sql(stringLiteral("false"), DOUBLE), PARSING, "CAST function cannot convert literal 'false' to type DOUBLE: Cannot parse VARCHAR value to DOUBLE");

        // VARCHAR -> DATE
        checkValue0(sql(stringLiteral("2020-1-01"), DATE), DATE, LocalDate.parse("2020-01-01"));
        checkValue0(sql(stringLiteral("2020-9-1"), DATE), DATE, LocalDate.parse("2020-09-01"));
        checkValue0(sql(stringLiteral("2020-01-01"), DATE), DATE, LocalDate.parse("2020-01-01"));
        checkFailure0(sql(stringLiteral("foo"), DATE), PARSING, "CAST function cannot convert literal 'foo' to type DATE: Cannot parse VARCHAR value to DATE");

        // VARCHAR -> TIME
        checkValue0(sql(stringLiteral("00:00"), TIME), TIME, LocalTime.parse("00:00"));
        checkValue0(sql(stringLiteral("0:0"), TIME), TIME, LocalTime.parse("00:00"));
        checkValue0(sql(stringLiteral("9:0"), TIME), TIME, LocalTime.parse("09:00"));
        checkValue0(sql(stringLiteral("00:01"), TIME), TIME, LocalTime.parse("00:01"));
        checkValue0(sql(stringLiteral("00:1"), TIME), TIME, LocalTime.parse("00:01"));
        checkValue0(sql(stringLiteral("02:01"), TIME), TIME, LocalTime.parse("02:01"));
        checkValue0(sql(stringLiteral("00:00:00"), TIME), TIME, LocalTime.parse("00:00:00"));
        checkValue0(sql(stringLiteral("00:00:01"), TIME), TIME, LocalTime.parse("00:00:01"));
        checkValue0(sql(stringLiteral("00:02:01"), TIME), TIME, LocalTime.parse("00:02:01"));
        checkValue0(sql(stringLiteral("03:02:01"), TIME), TIME, LocalTime.parse("03:02:01"));

        checkFailure0(sql(stringLiteral("9"), TIME), PARSING, "CAST function cannot convert literal '9' to type TIME: Cannot parse VARCHAR value to TIME");
        checkFailure0(sql(stringLiteral("00:60"), TIME), PARSING, "CAST function cannot convert literal '00:60' to type TIME: Cannot parse VARCHAR value to TIME");
        checkFailure0(sql(stringLiteral("00:00:60"), TIME), PARSING, "CAST function cannot convert literal '00:00:60' to type TIME: Cannot parse VARCHAR value to TIME");
        checkFailure0(sql(stringLiteral("25:00"), TIME), PARSING, "CAST function cannot convert literal '25:00' to type TIME: Cannot parse VARCHAR value to TIME");
        checkFailure0(sql(stringLiteral("25:00:00"), TIME), PARSING, "CAST function cannot convert literal '25:00:00' to type TIME: Cannot parse VARCHAR value to TIME");

        checkFailure0(sql(stringLiteral("foo"), TIME), PARSING, "CAST function cannot convert literal 'foo' to type TIME: Cannot parse VARCHAR value to TIME");

        // VARCHAR -> TIMESTAMP
        checkValue0(sql(stringLiteral("2020-02-01T00:00:00"), TIMESTAMP), TIMESTAMP, LocalDateTime.parse("2020-02-01T00:00:00"));
        checkValue0(sql(stringLiteral("2020-02-01T00:00:01"), TIMESTAMP), TIMESTAMP, LocalDateTime.parse("2020-02-01T00:00:01"));
        checkValue0(sql(stringLiteral("2020-02-01T00:02:01"), TIMESTAMP), TIMESTAMP, LocalDateTime.parse("2020-02-01T00:02:01"));
        checkValue0(sql(stringLiteral("2020-02-01T03:02:01"), TIMESTAMP), TIMESTAMP, LocalDateTime.parse("2020-02-01T03:02:01"));
        checkFailure0(sql(stringLiteral("foo"), TIMESTAMP), PARSING, "CAST function cannot convert literal 'foo' to type TIMESTAMP: Cannot parse VARCHAR value to TIMESTAMP");

        // VARCHAR -> TIMESTAMP_WITH_TIME_ZONE
        checkValue0(sql(stringLiteral(OFFSET_DATE_TIME_VAL), TIMESTAMP_WITH_TIME_ZONE), TIMESTAMP_WITH_TIME_ZONE, OFFSET_DATE_TIME_VAL);
        checkFailure0(sql(stringLiteral("foo"), TIMESTAMP_WITH_TIME_ZONE), PARSING, "CAST function cannot convert literal 'foo' to type TIMESTAMP WITH TIME ZONE: Cannot parse VARCHAR value to TIMESTAMP WITH TIME ZONE");

        // VARCHAR -> OBJECT
        checkValue0(sql(stringLiteral("foo"), OBJECT), OBJECT, "foo");

        // VARCHAR -> JSON
        checkValue0(sql(stringLiteral("[1,2,3]"), JSON), JSON, new HazelcastJsonValue("[1,2,3]"));
    }

    @Test
    public void testBoolean() {
        putAndCheckValue(new ExpressionValue.BooleanVal(), sql("field1", VARCHAR), VARCHAR, null);
        putAndCheckValue(new ExpressionValue.BooleanVal(), sql("field1", BOOLEAN), BOOLEAN, null);
        putAndCheckValue(new ExpressionValue.BooleanVal(), sql("field1", OBJECT), OBJECT, null);

        putAndCheckValue(true, sql("this", VARCHAR), VARCHAR, "true");
        putAndCheckValue(true, sql("this", BOOLEAN), BOOLEAN, true);
        putAndCheckValue(true, sql("this", OBJECT), OBJECT, true);

        putAndCheckFailure(true, sql("this", TINYINT), PARSING, castError(BOOLEAN, TINYINT));
        putAndCheckFailure(true, sql("this", SMALLINT), PARSING, castError(BOOLEAN, SMALLINT));
        putAndCheckFailure(true, sql("this", INTEGER), PARSING, castError(BOOLEAN, INTEGER));
        putAndCheckFailure(true, sql("this", BIGINT), PARSING, castError(BOOLEAN, BIGINT));
        putAndCheckFailure(true, sql("this", DECIMAL), PARSING, castError(BOOLEAN, DECIMAL));
        putAndCheckFailure(true, sql("this", REAL), PARSING, castError(BOOLEAN, REAL));
        putAndCheckFailure(true, sql("this", DOUBLE), PARSING, castError(BOOLEAN, DOUBLE));
        putAndCheckFailure(true, sql("this", DATE), PARSING, castError(BOOLEAN, DATE));
        putAndCheckFailure(true, sql("this", TIME), PARSING, castError(BOOLEAN, TIME));
        putAndCheckFailure(true, sql("this", TIMESTAMP), PARSING, castError(BOOLEAN, TIMESTAMP));
        putAndCheckFailure(true, sql("this", TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(BOOLEAN, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(true, sql("this", JSON), PARSING, castError(BOOLEAN, JSON));
    }

    @Test
    public void testBoolean_literal() {
        put(1);

        checkValue0(sql(literal(true), VARCHAR), VARCHAR, "true");
        checkValue0(sql(literal(false), VARCHAR), VARCHAR, "false");
        checkValue0(sql(literal(true), BOOLEAN), BOOLEAN, true);
        checkValue0(sql(literal(false), BOOLEAN), BOOLEAN, false);
        checkValue0(sql(literal(true), OBJECT), OBJECT, true);

        checkFailure0(sql(literal(true), TINYINT), PARSING, castError(BOOLEAN, TINYINT));
        checkFailure0(sql(literal(true), SMALLINT), PARSING, castError(BOOLEAN, SMALLINT));
        checkFailure0(sql(literal(true), INTEGER), PARSING, castError(BOOLEAN, INTEGER));
        checkFailure0(sql(literal(true), BIGINT), PARSING, castError(BOOLEAN, BIGINT));
        checkFailure0(sql(literal(true), DECIMAL), PARSING, castError(BOOLEAN, DECIMAL));
        checkFailure0(sql(literal(true), REAL), PARSING, castError(BOOLEAN, REAL));
        checkFailure0(sql(literal(true), DOUBLE), PARSING, castError(BOOLEAN, DOUBLE));
        checkFailure0(sql(literal(true), DATE), PARSING, castError(BOOLEAN, DATE));
        checkFailure0(sql(literal(true), TIME), PARSING, castError(BOOLEAN, TIME));
        checkFailure0(sql(literal(true), TIMESTAMP), PARSING, castError(BOOLEAN, TIMESTAMP));
        checkFailure0(sql(literal(true), TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(BOOLEAN, TIMESTAMP_WITH_TIME_ZONE));
        checkFailure0(sql(literal(true), JSON), PARSING, castError(BOOLEAN, JSON));
    }

    @Test
    public void testTinyint() {
        putAndCheckValue(new ExpressionValue.ByteVal(), sql("field1", VARCHAR), VARCHAR, null);
        putAndCheckValue(new ExpressionValue.ByteVal(), sql("field1", TINYINT), TINYINT, null);
        putAndCheckValue(new ExpressionValue.ByteVal(), sql("field1", SMALLINT), SMALLINT, null);
        putAndCheckValue(new ExpressionValue.ByteVal(), sql("field1", INTEGER), INTEGER, null);
        putAndCheckValue(new ExpressionValue.ByteVal(), sql("field1", BIGINT), BIGINT, null);
        putAndCheckValue(new ExpressionValue.ByteVal(), sql("field1", DECIMAL), DECIMAL, null);
        putAndCheckValue(new ExpressionValue.ByteVal(), sql("field1", REAL), REAL, null);
        putAndCheckValue(new ExpressionValue.ByteVal(), sql("field1", DOUBLE), DOUBLE, null);
        putAndCheckValue(new ExpressionValue.ByteVal(), sql("field1", OBJECT), OBJECT, null);

        putAndCheckValue((byte) 0, sql("this", VARCHAR), VARCHAR, "" + 0);
        putAndCheckValue((byte) 0, sql("this", TINYINT), TINYINT, (byte) 0);
        putAndCheckValue((byte) 0, sql("this", SMALLINT), SMALLINT, (short) 0);
        putAndCheckValue((byte) 0, sql("this", INTEGER), INTEGER, 0);
        putAndCheckValue((byte) 0, sql("this", BIGINT), BIGINT, 0L);
        putAndCheckValue((byte) 0, sql("this", DECIMAL), DECIMAL, BigDecimal.ZERO);
        putAndCheckValue((byte) 0, sql("this", REAL), REAL, 0f);
        putAndCheckValue((byte) 0, sql("this", DOUBLE), DOUBLE, 0d);
        putAndCheckValue((byte) 0, sql("this", OBJECT), OBJECT, (byte) 0);

        putAndCheckValue(Byte.MIN_VALUE, sql("this", VARCHAR), VARCHAR, "" + Byte.MIN_VALUE);
        putAndCheckValue(Byte.MIN_VALUE, sql("this", TINYINT), TINYINT, Byte.MIN_VALUE);
        putAndCheckValue(Byte.MIN_VALUE, sql("this", SMALLINT), SMALLINT, (short) Byte.MIN_VALUE);
        putAndCheckValue(Byte.MIN_VALUE, sql("this", INTEGER), INTEGER, (int) Byte.MIN_VALUE);
        putAndCheckValue(Byte.MIN_VALUE, sql("this", BIGINT), BIGINT, (long) Byte.MIN_VALUE);
        putAndCheckValue(Byte.MIN_VALUE, sql("this", DECIMAL), DECIMAL, new BigDecimal(Byte.MIN_VALUE));
        putAndCheckValue(Byte.MIN_VALUE, sql("this", REAL), REAL, (float) Byte.MIN_VALUE);
        putAndCheckValue(Byte.MIN_VALUE, sql("this", DOUBLE), DOUBLE, (double) Byte.MIN_VALUE);
        putAndCheckValue(Byte.MIN_VALUE, sql("this", OBJECT), OBJECT, Byte.MIN_VALUE);

        putAndCheckValue(Byte.MAX_VALUE, sql("this", VARCHAR), VARCHAR, "" + Byte.MAX_VALUE);
        putAndCheckValue(Byte.MAX_VALUE, sql("this", TINYINT), TINYINT, Byte.MAX_VALUE);
        putAndCheckValue(Byte.MAX_VALUE, sql("this", SMALLINT), SMALLINT, (short) Byte.MAX_VALUE);
        putAndCheckValue(Byte.MAX_VALUE, sql("this", INTEGER), INTEGER, (int) Byte.MAX_VALUE);
        putAndCheckValue(Byte.MAX_VALUE, sql("this", BIGINT), BIGINT, (long) Byte.MAX_VALUE);
        putAndCheckValue(Byte.MAX_VALUE, sql("this", DECIMAL), DECIMAL, new BigDecimal(Byte.MAX_VALUE));
        putAndCheckValue(Byte.MAX_VALUE, sql("this", REAL), REAL, (float) Byte.MAX_VALUE);
        putAndCheckValue(Byte.MAX_VALUE, sql("this", DOUBLE), DOUBLE, (double) Byte.MAX_VALUE);
        putAndCheckValue(Byte.MAX_VALUE, sql("this", OBJECT), OBJECT, Byte.MAX_VALUE);

        putAndCheckFailure((byte) 0, sql("this", BOOLEAN), PARSING, castError(TINYINT, BOOLEAN));
        putAndCheckFailure((byte) 0, sql("this", DATE), PARSING, castError(TINYINT, DATE));
        putAndCheckFailure((byte) 0, sql("this", TIME), PARSING, castError(TINYINT, TIME));
        putAndCheckFailure((byte) 0, sql("this", TIMESTAMP), PARSING, castError(TINYINT, TIMESTAMP));
        putAndCheckFailure((byte) 0, sql("this", TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(TINYINT, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure((byte) 0, sql("this", JSON), PARSING, castError(TINYINT, JSON));
    }

    @Test
    public void testTinyint_literal() {
        put(1);

        checkValue0(sql(literal(1), VARCHAR), VARCHAR, "1");

        checkFailure0(sql(literal(1), BOOLEAN), PARSING, castError(TINYINT, BOOLEAN));

        checkValue0(sql(literal(1), TINYINT), TINYINT, (byte) 1);
        checkValue0(sql(literal(Byte.MIN_VALUE), TINYINT), TINYINT, Byte.MIN_VALUE);
        checkValue0(sql(literal(Byte.MAX_VALUE), TINYINT), TINYINT, Byte.MAX_VALUE);
        checkFailure0(sql(literal(128), TINYINT), PARSING, "Numeric overflow while converting SMALLINT to TINYINT");

        checkValue0(sql(literal(1), SMALLINT), SMALLINT, (short) 1);
        checkValue0(sql(literal(1), INTEGER), INTEGER, 1);
        checkValue0(sql(literal(1), BIGINT), BIGINT, 1L);
        checkValue0(sql(literal(1), DECIMAL), DECIMAL, decimal(1));
        checkValue0(sql(literal(1), REAL), REAL, 1f);
        checkValue0(sql(literal(1), DOUBLE), DOUBLE, 1d);

        checkFailure0(sql(literal(1), DATE), PARSING, castError(TINYINT, DATE));
        checkFailure0(sql(literal(1), TIME), PARSING, castError(TINYINT, TIME));
        checkFailure0(sql(literal(1), TIMESTAMP), PARSING, castError(TINYINT, TIMESTAMP));
        checkFailure0(sql(literal(1), TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(TINYINT, TIMESTAMP_WITH_TIME_ZONE));
        checkFailure0(sql(literal(1), JSON), PARSING, castError(TINYINT, JSON));

        checkValue0(sql(literal(1), OBJECT), OBJECT, (byte) 1);
    }

    @Test
    public void testSmallint() {
        putAndCheckValue(new ExpressionValue.ShortVal(), sql("field1", VARCHAR), VARCHAR, null);
        putAndCheckValue(new ExpressionValue.ShortVal(), sql("field1", TINYINT), TINYINT, null);
        putAndCheckValue(new ExpressionValue.ShortVal(), sql("field1", SMALLINT), SMALLINT, null);
        putAndCheckValue(new ExpressionValue.ShortVal(), sql("field1", INTEGER), INTEGER, null);
        putAndCheckValue(new ExpressionValue.ShortVal(), sql("field1", BIGINT), BIGINT, null);
        putAndCheckValue(new ExpressionValue.ShortVal(), sql("field1", DECIMAL), DECIMAL, null);
        putAndCheckValue(new ExpressionValue.ShortVal(), sql("field1", REAL), REAL, null);
        putAndCheckValue(new ExpressionValue.ShortVal(), sql("field1", DOUBLE), DOUBLE, null);
        putAndCheckValue(new ExpressionValue.ShortVal(), sql("field1", OBJECT), OBJECT, null);

        putAndCheckValue((short) 0, sql("this", VARCHAR), VARCHAR, "" + 0L);
        putAndCheckValue((short) 0, sql("this", TINYINT), TINYINT, (byte) 0);
        putAndCheckValue((short) 0, sql("this", SMALLINT), SMALLINT, (short) 0);
        putAndCheckValue((short) 0, sql("this", INTEGER), INTEGER, 0);
        putAndCheckValue((short) 0, sql("this", BIGINT), BIGINT, 0L);
        putAndCheckValue((short) 0, sql("this", DECIMAL), DECIMAL, BigDecimal.ZERO);
        putAndCheckValue((short) 0, sql("this", REAL), REAL, 0f);
        putAndCheckValue((short) 0, sql("this", DOUBLE), DOUBLE, 0d);
        putAndCheckValue((short) 0, sql("this", OBJECT), OBJECT, (short) 0);

        putAndCheckValue(Short.MIN_VALUE, sql("this", VARCHAR), VARCHAR, "" + Short.MIN_VALUE);
        putAndCheckFailure(Short.MIN_VALUE, sql("this", TINYINT), DATA_EXCEPTION, "Numeric overflow while converting SMALLINT to TINYINT");
        putAndCheckValue(Short.MIN_VALUE, sql("this", SMALLINT), SMALLINT, Short.MIN_VALUE);
        putAndCheckValue(Short.MIN_VALUE, sql("this", INTEGER), INTEGER, (int) Short.MIN_VALUE);
        putAndCheckValue(Short.MIN_VALUE, sql("this", BIGINT), BIGINT, (long) Short.MIN_VALUE);
        putAndCheckValue(Short.MIN_VALUE, sql("this", DECIMAL), DECIMAL, new BigDecimal(Short.MIN_VALUE));
        putAndCheckValue(Short.MIN_VALUE, sql("this", REAL), REAL, (float) Short.MIN_VALUE);
        putAndCheckValue(Short.MIN_VALUE, sql("this", DOUBLE), DOUBLE, (double) Short.MIN_VALUE);
        putAndCheckValue(Short.MIN_VALUE, sql("this", OBJECT), OBJECT, Short.MIN_VALUE);

        putAndCheckValue(Short.MAX_VALUE, sql("this", VARCHAR), VARCHAR, "" + Short.MAX_VALUE);
        putAndCheckFailure(Short.MAX_VALUE, sql("this", TINYINT), DATA_EXCEPTION, "Numeric overflow while converting SMALLINT to TINYINT");
        putAndCheckValue(Short.MAX_VALUE, sql("this", SMALLINT), SMALLINT, Short.MAX_VALUE);
        putAndCheckValue(Short.MAX_VALUE, sql("this", INTEGER), INTEGER, (int) Short.MAX_VALUE);
        putAndCheckValue(Short.MAX_VALUE, sql("this", BIGINT), BIGINT, (long) Short.MAX_VALUE);
        putAndCheckValue(Short.MAX_VALUE, sql("this", DECIMAL), DECIMAL, new BigDecimal(Short.MAX_VALUE));
        putAndCheckValue(Short.MAX_VALUE, sql("this", REAL), REAL, (float) Short.MAX_VALUE);
        putAndCheckValue(Short.MAX_VALUE, sql("this", DOUBLE), DOUBLE, (double) Short.MAX_VALUE);
        putAndCheckValue(Short.MAX_VALUE, sql("this", OBJECT), OBJECT, Short.MAX_VALUE);

        putAndCheckFailure((short) 0, sql("this", BOOLEAN), PARSING, castError(SMALLINT, BOOLEAN));
        putAndCheckFailure((short) 0, sql("this", DATE), PARSING, castError(SMALLINT, DATE));
        putAndCheckFailure((short) 0, sql("this", TIME), PARSING, castError(SMALLINT, TIME));
        putAndCheckFailure((short) 0, sql("this", TIMESTAMP), PARSING, castError(SMALLINT, TIMESTAMP));
        putAndCheckFailure((short) 0, sql("this", TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(SMALLINT, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure((short) 0, sql("this", JSON), PARSING, castError(SMALLINT, JSON));
    }

    @Test
    public void testSmallint_literal() {
        put(1);

        checkValue0(sql(literal(Short.MAX_VALUE), VARCHAR), VARCHAR, Short.MAX_VALUE + "");

        checkFailure0(sql(literal(Short.MAX_VALUE), BOOLEAN), PARSING, castError(SMALLINT, BOOLEAN));

        checkFailure0(sql(literal(Short.MAX_VALUE), TINYINT), PARSING, "CAST function cannot convert literal 32767 to type TINYINT: Numeric overflow while converting SMALLINT to TINYINT");

        checkValue0(sql(literal(Short.MAX_VALUE), SMALLINT), SMALLINT, Short.MAX_VALUE);
        checkValue0(sql(literal(Short.MAX_VALUE), INTEGER), INTEGER, (int) Short.MAX_VALUE);
        checkValue0(sql(literal(Short.MAX_VALUE), BIGINT), BIGINT, (long) Short.MAX_VALUE);
        checkValue0(sql(literal(Short.MAX_VALUE), DECIMAL), DECIMAL, decimal(Short.MAX_VALUE));
        checkValue0(sql(literal(Short.MAX_VALUE), REAL), REAL, (float) Short.MAX_VALUE);
        checkValue0(sql(literal(Short.MAX_VALUE), DOUBLE), DOUBLE, (double) Short.MAX_VALUE);

        checkFailure0(sql(literal(Short.MAX_VALUE), DATE), PARSING, castError(SMALLINT, DATE));
        checkFailure0(sql(literal(Short.MAX_VALUE), TIME), PARSING, castError(SMALLINT, TIME));
        checkFailure0(sql(literal(Short.MAX_VALUE), TIMESTAMP), PARSING, castError(SMALLINT, TIMESTAMP));
        checkFailure0(sql(literal(Short.MAX_VALUE), TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(SMALLINT, TIMESTAMP_WITH_TIME_ZONE));
        checkFailure0(sql(literal(Short.MAX_VALUE), JSON), PARSING, castError(SMALLINT, JSON));

        checkValue0(sql(literal(Short.MAX_VALUE), OBJECT), OBJECT, Short.MAX_VALUE);
    }

    @Test
    public void testInteger() {
        putAndCheckValue(new ExpressionValue.IntegerVal(), sql("field1", VARCHAR), VARCHAR, null);
        putAndCheckValue(new ExpressionValue.IntegerVal(), sql("field1", TINYINT), TINYINT, null);
        putAndCheckValue(new ExpressionValue.IntegerVal(), sql("field1", SMALLINT), SMALLINT, null);
        putAndCheckValue(new ExpressionValue.IntegerVal(), sql("field1", INTEGER), INTEGER, null);
        putAndCheckValue(new ExpressionValue.IntegerVal(), sql("field1", BIGINT), BIGINT, null);
        putAndCheckValue(new ExpressionValue.IntegerVal(), sql("field1", DECIMAL), DECIMAL, null);
        putAndCheckValue(new ExpressionValue.IntegerVal(), sql("field1", REAL), REAL, null);
        putAndCheckValue(new ExpressionValue.IntegerVal(), sql("field1", DOUBLE), DOUBLE, null);
        putAndCheckValue(new ExpressionValue.IntegerVal(), sql("field1", OBJECT), OBJECT, null);

        putAndCheckValue(0, sql("this", VARCHAR), VARCHAR, "" + 0L);
        putAndCheckValue(0, sql("this", TINYINT), TINYINT, (byte) 0);
        putAndCheckValue(0, sql("this", SMALLINT), SMALLINT, (short) 0);
        putAndCheckValue(0, sql("this", INTEGER), INTEGER, 0);
        putAndCheckValue(0, sql("this", BIGINT), BIGINT, 0L);
        putAndCheckValue(0, sql("this", DECIMAL), DECIMAL, BigDecimal.ZERO);
        putAndCheckValue(0, sql("this", REAL), REAL, 0f);
        putAndCheckValue(0, sql("this", DOUBLE), DOUBLE, 0d);
        putAndCheckValue(0, sql("this", OBJECT), OBJECT, 0);

        putAndCheckValue(Integer.MIN_VALUE, sql("this", VARCHAR), VARCHAR, "" + Integer.MIN_VALUE);
        putAndCheckFailure(Integer.MIN_VALUE, sql("this", TINYINT), DATA_EXCEPTION, "Numeric overflow while converting INTEGER to TINYINT");
        putAndCheckFailure(Integer.MIN_VALUE, sql("this", SMALLINT), DATA_EXCEPTION, "Numeric overflow while converting INTEGER to SMALLINT");
        putAndCheckValue(Integer.MIN_VALUE, sql("this", INTEGER), INTEGER, Integer.MIN_VALUE);
        putAndCheckValue(Integer.MIN_VALUE, sql("this", BIGINT), BIGINT, (long) Integer.MIN_VALUE);
        putAndCheckValue(Integer.MIN_VALUE, sql("this", DECIMAL), DECIMAL, new BigDecimal(Integer.MIN_VALUE));
        putAndCheckValue(Integer.MIN_VALUE, sql("this", REAL), REAL, (float) Integer.MIN_VALUE);
        putAndCheckValue(Integer.MIN_VALUE, sql("this", DOUBLE), DOUBLE, (double) Integer.MIN_VALUE);
        putAndCheckValue(Integer.MIN_VALUE, sql("this", OBJECT), OBJECT, Integer.MIN_VALUE);

        putAndCheckValue(Integer.MAX_VALUE, sql("this", VARCHAR), VARCHAR, "" + Integer.MAX_VALUE);
        putAndCheckFailure(Integer.MAX_VALUE, sql("this", TINYINT), DATA_EXCEPTION, "Numeric overflow while converting INTEGER to TINYINT");
        putAndCheckFailure(Integer.MAX_VALUE, sql("this", SMALLINT), DATA_EXCEPTION, "Numeric overflow while converting INTEGER to SMALLINT");
        putAndCheckValue(Integer.MAX_VALUE, sql("this", INTEGER), INTEGER, Integer.MAX_VALUE);
        putAndCheckValue(Integer.MAX_VALUE, sql("this", BIGINT), BIGINT, (long) Integer.MAX_VALUE);
        putAndCheckValue(Integer.MAX_VALUE, sql("this", DECIMAL), DECIMAL, new BigDecimal(Integer.MAX_VALUE));
        putAndCheckValue(Integer.MAX_VALUE, sql("this", REAL), REAL, (float) Integer.MAX_VALUE);
        putAndCheckValue(Integer.MAX_VALUE, sql("this", DOUBLE), DOUBLE, (double) Integer.MAX_VALUE);
        putAndCheckValue(Integer.MAX_VALUE, sql("this", OBJECT), OBJECT, Integer.MAX_VALUE);

        putAndCheckFailure(0, sql("this", BOOLEAN), PARSING, castError(INTEGER, BOOLEAN));
        putAndCheckFailure(0, sql("this", DATE), PARSING, castError(INTEGER, DATE));
        putAndCheckFailure(0, sql("this", TIME), PARSING, castError(INTEGER, TIME));
        putAndCheckFailure(0, sql("this", TIMESTAMP), PARSING, castError(INTEGER, TIMESTAMP));
        putAndCheckFailure(0, sql("this", TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(INTEGER, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(0, sql("this", JSON), PARSING, castError(INTEGER, JSON));
    }

    @Test
    public void testInteger_literal() {
        put(1);

        checkValue0(sql(literal(Integer.MAX_VALUE), VARCHAR), VARCHAR, Integer.MAX_VALUE + "");

        checkFailure0(sql(literal(Integer.MAX_VALUE), BOOLEAN), PARSING, castError(INTEGER, BOOLEAN));

        checkFailure0(sql(literal(Integer.MAX_VALUE), TINYINT), PARSING, "CAST function cannot convert literal 2147483647 to type TINYINT: Numeric overflow while converting INTEGER to TINYINT");
        checkFailure0(sql(literal(Integer.MAX_VALUE), SMALLINT), PARSING, "CAST function cannot convert literal 2147483647 to type SMALLINT: Numeric overflow while converting INTEGER to SMALLINT");

        checkValue0(sql(literal(Integer.MAX_VALUE), INTEGER), INTEGER, Integer.MAX_VALUE);
        checkValue0(sql(literal(Integer.MAX_VALUE), BIGINT), BIGINT, (long) Integer.MAX_VALUE);
        checkValue0(sql(literal(Integer.MAX_VALUE), DECIMAL), DECIMAL, decimal(Integer.MAX_VALUE));
        checkValue0(sql(literal(Integer.MAX_VALUE), REAL), REAL, (float) Integer.MAX_VALUE);
        checkValue0(sql(literal(Integer.MAX_VALUE), DOUBLE), DOUBLE, (double) Integer.MAX_VALUE);

        checkFailure0(sql(literal(Integer.MAX_VALUE), DATE), PARSING, castError(INTEGER, DATE));
        checkFailure0(sql(literal(Integer.MAX_VALUE), TIME), PARSING, castError(INTEGER, TIME));
        checkFailure0(sql(literal(Integer.MAX_VALUE), TIMESTAMP), PARSING, castError(INTEGER, TIMESTAMP));
        checkFailure0(sql(literal(Integer.MAX_VALUE), TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(INTEGER, TIMESTAMP_WITH_TIME_ZONE));
        checkFailure0(sql(literal(Integer.MAX_VALUE), JSON), PARSING, castError(INTEGER, JSON));

        checkValue0(sql(literal(Integer.MAX_VALUE), OBJECT), OBJECT, Integer.MAX_VALUE);
    }

    @Test
    public void testBigint() {
        putAndCheckValue(new ExpressionValue.LongVal(), sql("field1", VARCHAR), VARCHAR, null);
        putAndCheckValue(new ExpressionValue.LongVal(), sql("field1", TINYINT), TINYINT, null);
        putAndCheckValue(new ExpressionValue.LongVal(), sql("field1", SMALLINT), SMALLINT, null);
        putAndCheckValue(new ExpressionValue.LongVal(), sql("field1", INTEGER), INTEGER, null);
        putAndCheckValue(new ExpressionValue.LongVal(), sql("field1", BIGINT), BIGINT, null);
        putAndCheckValue(new ExpressionValue.LongVal(), sql("field1", DECIMAL), DECIMAL, null);
        putAndCheckValue(new ExpressionValue.LongVal(), sql("field1", REAL), REAL, null);
        putAndCheckValue(new ExpressionValue.LongVal(), sql("field1", DOUBLE), DOUBLE, null);
        putAndCheckValue(new ExpressionValue.LongVal(), sql("field1", OBJECT), OBJECT, null);

        putAndCheckValue(0L, sql("this", VARCHAR), VARCHAR, "" + 0L);
        putAndCheckValue(0L, sql("this", TINYINT), TINYINT, (byte) 0);
        putAndCheckValue(0L, sql("this", SMALLINT), SMALLINT, (short) 0);
        putAndCheckValue(0L, sql("this", INTEGER), INTEGER, 0);
        putAndCheckValue(0L, sql("this", BIGINT), BIGINT, 0L);
        putAndCheckValue(0L, sql("this", DECIMAL), DECIMAL, BigDecimal.ZERO);
        putAndCheckValue(0L, sql("this", REAL), REAL, 0f);
        putAndCheckValue(0L, sql("this", DOUBLE), DOUBLE, 0d);
        putAndCheckValue(0L, sql("this", OBJECT), OBJECT, 0L);

        putAndCheckValue(Long.MIN_VALUE, sql("this", VARCHAR), VARCHAR, "" + Long.MIN_VALUE);
        putAndCheckFailure(Long.MIN_VALUE, sql("this", TINYINT), DATA_EXCEPTION, "Numeric overflow while converting BIGINT to TINYINT");
        putAndCheckFailure(Long.MIN_VALUE, sql("this", SMALLINT), DATA_EXCEPTION, "Numeric overflow while converting BIGINT to SMALLINT");
        putAndCheckFailure(Long.MIN_VALUE, sql("this", INTEGER), DATA_EXCEPTION, "Numeric overflow while converting BIGINT to INTEGER");
        putAndCheckValue(Long.MIN_VALUE, sql("this", BIGINT), BIGINT, Long.MIN_VALUE);
        putAndCheckValue(Long.MIN_VALUE, sql("this", DECIMAL), DECIMAL, new BigDecimal(Long.MIN_VALUE));
        putAndCheckValue(Long.MIN_VALUE, sql("this", REAL), REAL, (float) Long.MIN_VALUE);
        putAndCheckValue(Long.MIN_VALUE, sql("this", DOUBLE), DOUBLE, (double) Long.MIN_VALUE);
        putAndCheckValue(Long.MIN_VALUE, sql("this", OBJECT), OBJECT, Long.MIN_VALUE);

        putAndCheckValue(Long.MAX_VALUE, sql("this", VARCHAR), VARCHAR, "" + Long.MAX_VALUE);
        putAndCheckFailure(Long.MAX_VALUE, sql("this", TINYINT), DATA_EXCEPTION, "Numeric overflow while converting BIGINT to TINYINT");
        putAndCheckFailure(Long.MAX_VALUE, sql("this", SMALLINT), DATA_EXCEPTION, "Numeric overflow while converting BIGINT to SMALLINT");
        putAndCheckFailure(Long.MAX_VALUE, sql("this", INTEGER), DATA_EXCEPTION, "Numeric overflow while converting BIGINT to INTEGER");
        putAndCheckValue(Long.MAX_VALUE, sql("this", BIGINT), BIGINT, Long.MAX_VALUE);
        putAndCheckValue(Long.MAX_VALUE, sql("this", DECIMAL), DECIMAL, new BigDecimal(Long.MAX_VALUE));
        putAndCheckValue(Long.MAX_VALUE, sql("this", REAL), REAL, (float) Long.MAX_VALUE);
        putAndCheckValue(Long.MAX_VALUE, sql("this", DOUBLE), DOUBLE, (double) Long.MAX_VALUE);
        putAndCheckValue(Long.MAX_VALUE, sql("this", OBJECT), OBJECT, Long.MAX_VALUE);

        putAndCheckFailure(0L, sql("this", BOOLEAN), PARSING, castError(BIGINT, BOOLEAN));
        putAndCheckFailure(0L, sql("this", DATE), PARSING, castError(BIGINT, DATE));
        putAndCheckFailure(0L, sql("this", TIME), PARSING, castError(BIGINT, TIME));
        putAndCheckFailure(0L, sql("this", TIMESTAMP), PARSING, castError(BIGINT, TIMESTAMP));
        putAndCheckFailure(0L, sql("this", TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(BIGINT, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(0L, sql("this", JSON), PARSING, castError(BIGINT, JSON));
    }

    @Test
    public void testBigint_literal() {
        put(1);

        checkValue0(sql(literal(Long.MAX_VALUE), VARCHAR), VARCHAR, Long.MAX_VALUE + "");

        checkFailure0(sql(literal(Long.MAX_VALUE), BOOLEAN), PARSING, castError(BIGINT, BOOLEAN));

        checkFailure0(sql(literal(Long.MAX_VALUE), TINYINT), PARSING, "CAST function cannot convert literal 9223372036854775807 to type TINYINT: Numeric overflow while converting BIGINT to TINYINT");
        checkFailure0(sql(literal(Long.MAX_VALUE), SMALLINT), PARSING, "CAST function cannot convert literal 9223372036854775807 to type SMALLINT: Numeric overflow while converting BIGINT to SMALLINT");
        checkFailure0(sql(literal(Long.MAX_VALUE), INTEGER), PARSING, "CAST function cannot convert literal 9223372036854775807 to type INTEGER: Numeric overflow while converting BIGINT to INTEGER");

        checkValue0(sql(literal(Long.MAX_VALUE), BIGINT), BIGINT, Long.MAX_VALUE);
        checkValue0(sql(literal(Long.MAX_VALUE), DECIMAL), DECIMAL, decimal(Long.MAX_VALUE));
        checkValue0(sql(literal(Long.MAX_VALUE), REAL), REAL, (float) Long.MAX_VALUE);
        checkValue0(sql(literal(Long.MAX_VALUE), DOUBLE), DOUBLE, (double) Long.MAX_VALUE);

        checkFailure0(sql(literal(Long.MAX_VALUE), DATE), PARSING, castError(BIGINT, DATE));
        checkFailure0(sql(literal(Long.MAX_VALUE), TIME), PARSING, castError(BIGINT, TIME));
        checkFailure0(sql(literal(Long.MAX_VALUE), TIMESTAMP), PARSING, castError(BIGINT, TIMESTAMP));
        checkFailure0(sql(literal(Long.MAX_VALUE), TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(BIGINT, TIMESTAMP_WITH_TIME_ZONE));
        checkFailure0(sql(literal(Long.MAX_VALUE), JSON), PARSING, castError(BIGINT, JSON));

        checkValue0(sql(literal(Long.MAX_VALUE), OBJECT), OBJECT, Long.MAX_VALUE);
    }

    @Test
    public void testDecimal_BigInteger() {
        putAndCheckValue(new ExpressionValue.BigIntegerVal(), sql("field1", VARCHAR), VARCHAR, null);
        putAndCheckValue(new ExpressionValue.BigIntegerVal(), sql("field1", TINYINT), TINYINT, null);
        putAndCheckValue(new ExpressionValue.BigIntegerVal(), sql("field1", SMALLINT), SMALLINT, null);
        putAndCheckValue(new ExpressionValue.BigIntegerVal(), sql("field1", INTEGER), INTEGER, null);
        putAndCheckValue(new ExpressionValue.BigIntegerVal(), sql("field1", BIGINT), BIGINT, null);
        putAndCheckValue(new ExpressionValue.BigIntegerVal(), sql("field1", DECIMAL), DECIMAL, null);
        putAndCheckValue(new ExpressionValue.BigIntegerVal(), sql("field1", REAL), REAL, null);
        putAndCheckValue(new ExpressionValue.BigIntegerVal(), sql("field1", DOUBLE), DOUBLE, null);
        putAndCheckValue(new ExpressionValue.BigIntegerVal(), sql("field1", OBJECT), OBJECT, null);

        putAndCheckValue(BigInteger.ZERO, sql("this", TINYINT), TINYINT, (byte) 0);
        putAndCheckValue(new BigInteger(Byte.MAX_VALUE + ""), sql("this", TINYINT), TINYINT, Byte.MAX_VALUE);
        putAndCheckValue(new BigInteger(Byte.MIN_VALUE + ""), sql("this", TINYINT), TINYINT, Byte.MIN_VALUE);
        putAndCheckFailure(new BigInteger(Short.MAX_VALUE + ""), sql("this", TINYINT), DATA_EXCEPTION, "Numeric overflow while converting DECIMAL to TINYINT");
        putAndCheckFailure(new BigInteger(Short.MIN_VALUE + ""), sql("this", TINYINT), DATA_EXCEPTION, "Numeric overflow while converting DECIMAL to TINYINT");

        putAndCheckValue(BigInteger.ZERO, sql("this", SMALLINT), SMALLINT, (short) 0);
        putAndCheckValue(new BigInteger(Short.MAX_VALUE + ""), sql("this", SMALLINT), SMALLINT, Short.MAX_VALUE);
        putAndCheckValue(new BigInteger(Short.MIN_VALUE + ""), sql("this", SMALLINT), SMALLINT, Short.MIN_VALUE);
        putAndCheckFailure(new BigInteger(Integer.MAX_VALUE + ""), sql("this", SMALLINT), DATA_EXCEPTION, "Numeric overflow while converting DECIMAL to SMALLINT");
        putAndCheckFailure(new BigInteger(Integer.MIN_VALUE + ""), sql("this", SMALLINT), DATA_EXCEPTION, "Numeric overflow while converting DECIMAL to SMALLINT");

        putAndCheckValue(BigInteger.ZERO, sql("this", INTEGER), INTEGER, 0);
        putAndCheckValue(new BigInteger(Integer.MAX_VALUE + ""), sql("this", INTEGER), INTEGER, Integer.MAX_VALUE);
        putAndCheckValue(new BigInteger(Integer.MIN_VALUE + ""), sql("this", INTEGER), INTEGER, Integer.MIN_VALUE);
        putAndCheckFailure(new BigInteger(Long.MAX_VALUE + ""), sql("this", INTEGER), DATA_EXCEPTION, "Numeric overflow while converting DECIMAL to INTEGER");
        putAndCheckFailure(new BigInteger(Long.MIN_VALUE + ""), sql("this", INTEGER), DATA_EXCEPTION, "Numeric overflow while converting DECIMAL to INTEGER");

        putAndCheckValue(BigInteger.ZERO, sql("this", BIGINT), BIGINT, 0L);
        putAndCheckValue(new BigInteger(Long.MAX_VALUE + ""), sql("this", BIGINT), BIGINT, Long.MAX_VALUE);
        putAndCheckValue(new BigInteger(Long.MIN_VALUE + ""), sql("this", BIGINT), BIGINT, Long.MIN_VALUE);
        putAndCheckFailure(new BigInteger(Long.MAX_VALUE + "0"), sql("this", BIGINT), DATA_EXCEPTION, "Numeric overflow while converting DECIMAL to BIGINT");
        putAndCheckFailure(new BigInteger(Long.MIN_VALUE + "0"), sql("this", BIGINT), DATA_EXCEPTION, "Numeric overflow while converting DECIMAL to BIGINT");

        putAndCheckValue(new BigInteger("1"), sql("this", DECIMAL), DECIMAL, decimal("1"));
        putAndCheckValue(new BigInteger("1"), sql("this", REAL), REAL, 1f);
        putAndCheckValue(new BigInteger("1"), sql("this", DOUBLE), DOUBLE, 1d);
        putAndCheckValue(new BigInteger("1"), sql("this", OBJECT), OBJECT, decimal("1"));

        putAndCheckFailure(BigDecimal.ZERO, sql("this", BOOLEAN), PARSING, castError(DECIMAL, BOOLEAN));
        putAndCheckFailure(BigDecimal.ZERO, sql("this", DATE), PARSING, castError(DECIMAL, DATE));
        putAndCheckFailure(BigDecimal.ZERO, sql("this", TIME), PARSING, castError(DECIMAL, TIME));
        putAndCheckFailure(BigDecimal.ZERO, sql("this", TIMESTAMP), PARSING, castError(DECIMAL, TIMESTAMP));
        putAndCheckFailure(BigDecimal.ZERO, sql("this", TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(DECIMAL, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(BigDecimal.ZERO, sql("this", JSON), PARSING, castError(DECIMAL, JSON));
    }

    @Test
    public void testDecimal_BigDecimal() {
        putAndCheckValue(new ExpressionValue.BigDecimalVal(), sql("field1", VARCHAR), VARCHAR, null);
        putAndCheckValue(new ExpressionValue.BigDecimalVal(), sql("field1", TINYINT), TINYINT, null);
        putAndCheckValue(new ExpressionValue.BigDecimalVal(), sql("field1", SMALLINT), SMALLINT, null);
        putAndCheckValue(new ExpressionValue.BigDecimalVal(), sql("field1", INTEGER), INTEGER, null);
        putAndCheckValue(new ExpressionValue.BigDecimalVal(), sql("field1", BIGINT), BIGINT, null);
        putAndCheckValue(new ExpressionValue.BigDecimalVal(), sql("field1", DECIMAL), DECIMAL, null);
        putAndCheckValue(new ExpressionValue.BigDecimalVal(), sql("field1", REAL), REAL, null);
        putAndCheckValue(new ExpressionValue.BigDecimalVal(), sql("field1", DOUBLE), DOUBLE, null);
        putAndCheckValue(new ExpressionValue.BigDecimalVal(), sql("field1", OBJECT), OBJECT, null);

        putAndCheckValue(decimal(0), sql("this", TINYINT), TINYINT, (byte) 0);
        putAndCheckValue(decimal("1.1"), sql("this", TINYINT), TINYINT, (byte) 1);
        putAndCheckValue(decimal(Byte.MAX_VALUE), sql("this", TINYINT), TINYINT, Byte.MAX_VALUE);
        putAndCheckValue(decimal(Byte.MIN_VALUE), sql("this", TINYINT), TINYINT, Byte.MIN_VALUE);
        putAndCheckFailure(decimal(Short.MAX_VALUE), sql("this", TINYINT), DATA_EXCEPTION, "Numeric overflow while converting DECIMAL to TINYINT");
        putAndCheckFailure(decimal(Short.MIN_VALUE), sql("this", TINYINT), DATA_EXCEPTION, "Numeric overflow while converting DECIMAL to TINYINT");

        putAndCheckValue(decimal(0), sql("this", SMALLINT), SMALLINT, (short) 0);
        putAndCheckValue(decimal("1.1"), sql("this", SMALLINT), SMALLINT, (short) 1);
        putAndCheckValue(decimal(Short.MAX_VALUE), sql("this", SMALLINT), SMALLINT, Short.MAX_VALUE);
        putAndCheckValue(decimal(Short.MIN_VALUE), sql("this", SMALLINT), SMALLINT, Short.MIN_VALUE);
        putAndCheckFailure(decimal(Integer.MAX_VALUE), sql("this", SMALLINT), DATA_EXCEPTION, "Numeric overflow while converting DECIMAL to SMALLINT");
        putAndCheckFailure(decimal(Integer.MIN_VALUE), sql("this", SMALLINT), DATA_EXCEPTION, "Numeric overflow while converting DECIMAL to SMALLINT");

        putAndCheckValue(decimal(0), sql("this", INTEGER), INTEGER, 0);
        putAndCheckValue(decimal("1.1"), sql("this", INTEGER), INTEGER, 1);
        putAndCheckValue(decimal(Integer.MAX_VALUE), sql("this", INTEGER), INTEGER, Integer.MAX_VALUE);
        putAndCheckValue(decimal(Integer.MIN_VALUE), sql("this", INTEGER), INTEGER, Integer.MIN_VALUE);
        putAndCheckFailure(decimal(Long.MAX_VALUE), sql("this", INTEGER), DATA_EXCEPTION, "Numeric overflow while converting DECIMAL to INTEGER");
        putAndCheckFailure(decimal(Long.MIN_VALUE), sql("this", INTEGER), DATA_EXCEPTION, "Numeric overflow while converting DECIMAL to INTEGER");

        putAndCheckValue(decimal(0), sql("this", BIGINT), BIGINT, 0L);
        putAndCheckValue(decimal("1.1"), sql("this", BIGINT), BIGINT, 1L);
        putAndCheckValue(decimal(Long.MAX_VALUE), sql("this", BIGINT), BIGINT, Long.MAX_VALUE);
        putAndCheckValue(decimal(Long.MIN_VALUE), sql("this", BIGINT), BIGINT, Long.MIN_VALUE);
        putAndCheckFailure(decimal(Long.MAX_VALUE + "0"), sql("this", BIGINT), DATA_EXCEPTION, "Numeric overflow while converting DECIMAL to BIGINT");
        putAndCheckFailure(decimal(Long.MIN_VALUE + "0"), sql("this", BIGINT), DATA_EXCEPTION, "Numeric overflow while converting DECIMAL to BIGINT");

        putAndCheckValue(decimal("1.1"), sql("this", DECIMAL), DECIMAL, decimal("1.1"));
        putAndCheckValue(decimal("1.1"), sql("this", REAL), REAL, 1.1f);
        putAndCheckValue(decimal("1.1"), sql("this", DOUBLE), DOUBLE, 1.1d);
        putAndCheckValue(decimal("1.1"), sql("this", OBJECT), OBJECT, decimal("1.1"));

        putAndCheckFailure(BigDecimal.ZERO, sql("this", BOOLEAN), PARSING, castError(DECIMAL, BOOLEAN));
        putAndCheckFailure(BigDecimal.ZERO, sql("this", DATE), PARSING, castError(DECIMAL, DATE));
        putAndCheckFailure(BigDecimal.ZERO, sql("this", TIME), PARSING, castError(DECIMAL, TIME));
        putAndCheckFailure(BigDecimal.ZERO, sql("this", TIMESTAMP), PARSING, castError(DECIMAL, TIMESTAMP));
        putAndCheckFailure(BigDecimal.ZERO, sql("this", TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(DECIMAL, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(BigDecimal.ZERO, sql("this", JSON), PARSING, castError(DECIMAL, JSON));
    }

    @Test
    public void testDecimal_literal_small() {
        put(1);

        String literal = literal("1.1");
        BigDecimal decimalValue = decimal(literal);

        checkValue0(sql(literal, VARCHAR), VARCHAR, literal);
        checkFailure0(sql(literal, BOOLEAN), PARSING, castError(DECIMAL, BOOLEAN));

        checkValue0(sql(literal, TINYINT), TINYINT, (byte) 1);
        checkValue0(sql(literal, SMALLINT), SMALLINT, (short) 1);
        checkValue0(sql(literal, INTEGER), INTEGER, 1);
        checkValue0(sql(literal, BIGINT), BIGINT, 1L);

        checkValue0(sql(literal, DECIMAL), DECIMAL, decimalValue);
        checkValue0(sql(literal, REAL), REAL, decimalValue.floatValue());
        checkValue0(sql(literal, DOUBLE), DOUBLE, decimalValue.doubleValue());

        checkFailure0(sql(literal, DATE), PARSING, castError(DECIMAL, DATE));
        checkFailure0(sql(literal, TIME), PARSING, castError(DECIMAL, TIME));
        checkFailure0(sql(literal, TIMESTAMP), PARSING, castError(DECIMAL, TIMESTAMP));
        checkFailure0(sql(literal, TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(DECIMAL, TIMESTAMP_WITH_TIME_ZONE));
        checkFailure0(sql(literal, JSON), PARSING, castError(DECIMAL, JSON));

        checkValue0(sql(literal, OBJECT), OBJECT, decimalValue);
    }

    @Test
    public void testDecimal_literal_big() {
        put(1);

        String literal = literal(Long.MAX_VALUE + "0.1");
        BigDecimal decimalValue = decimal(literal);

        checkValue0(sql(literal, VARCHAR), VARCHAR, literal);
        checkFailure0(sql(literal, BOOLEAN), PARSING, castError(DECIMAL, BOOLEAN));

        checkFailure0(sql(literal, TINYINT), PARSING, "CAST function cannot convert literal 92233720368547758070.1 to type TINYINT: Numeric overflow while converting DECIMAL to TINYINT");
        checkFailure0(sql(literal, SMALLINT), PARSING, "CAST function cannot convert literal 92233720368547758070.1 to type SMALLINT: Numeric overflow while converting DECIMAL to SMALLINT");
        checkFailure0(sql(literal, INTEGER), PARSING, "CAST function cannot convert literal 92233720368547758070.1 to type INTEGER: Numeric overflow while converting DECIMAL to INTEGER");
        checkFailure0(sql(literal, BIGINT), PARSING, "CAST function cannot convert literal 92233720368547758070.1 to type BIGINT: Numeric overflow while converting DECIMAL to BIGINT");

        checkValue0(sql(literal, DECIMAL), DECIMAL, decimalValue);
        checkValue0(sql(literal, REAL), REAL, decimalValue.floatValue());
        checkValue0(sql(literal, DOUBLE), DOUBLE, decimalValue.doubleValue());

        checkFailure0(sql(literal, DATE), PARSING, castError(DECIMAL, DATE));
        checkFailure0(sql(literal, TIME), PARSING, castError(DECIMAL, TIME));
        checkFailure0(sql(literal, TIMESTAMP), PARSING, castError(DECIMAL, TIMESTAMP));
        checkFailure0(sql(literal, TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(DECIMAL, TIMESTAMP_WITH_TIME_ZONE));
        checkFailure0(sql(literal, JSON), PARSING, castError(DECIMAL, JSON));

        checkValue0(sql(literal, OBJECT), OBJECT, decimalValue);
    }

    @Test
    public void testReal() {
        putAndCheckValue(new ExpressionValue.FloatVal(), sql("field1", VARCHAR), VARCHAR, null);
        putAndCheckValue(new ExpressionValue.FloatVal(), sql("field1", TINYINT), TINYINT, null);
        putAndCheckValue(new ExpressionValue.FloatVal(), sql("field1", SMALLINT), SMALLINT, null);
        putAndCheckValue(new ExpressionValue.FloatVal(), sql("field1", INTEGER), INTEGER, null);
        putAndCheckValue(new ExpressionValue.FloatVal(), sql("field1", BIGINT), BIGINT, null);
        putAndCheckValue(new ExpressionValue.FloatVal(), sql("field1", DECIMAL), DECIMAL, null);
        putAndCheckValue(new ExpressionValue.FloatVal(), sql("field1", REAL), REAL, null);
        putAndCheckValue(new ExpressionValue.FloatVal(), sql("field1", DOUBLE), DOUBLE, null);
        putAndCheckValue(new ExpressionValue.FloatVal(), sql("field1", OBJECT), OBJECT, null);

        putAndCheckValue(1.1f, sql("this", TINYINT), TINYINT, (byte) 1);
        putAndCheckValue(1.1f, sql("this", SMALLINT), SMALLINT, (short) 1);
        putAndCheckValue(1.1f, sql("this", INTEGER), INTEGER, 1);
        putAndCheckValue(1.1f, sql("this", BIGINT), BIGINT, 1L);
        putAndCheckValue(1f, sql("this", DECIMAL), DECIMAL, decimal("1"));
        putAndCheckValue(1.1f, sql("this", REAL), REAL, 1.1f);
        putAndCheckValue(1f, sql("this", DOUBLE), DOUBLE, 1d);
        putAndCheckValue(1.1f, sql("this", OBJECT), OBJECT, 1.1f);

        putAndCheckFailure(0f, sql("this", BOOLEAN), PARSING, castError(REAL, BOOLEAN));
        putAndCheckFailure(0f, sql("this", DATE), PARSING, castError(REAL, DATE));
        putAndCheckFailure(0f, sql("this", TIME), PARSING, castError(REAL, TIME));
        putAndCheckFailure(0f, sql("this", TIMESTAMP), PARSING, castError(REAL, TIMESTAMP));
        putAndCheckFailure(0f, sql("this", TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(REAL, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(0f, sql("this", JSON), PARSING, castError(REAL, JSON));
    }

    @Test
    public void testDouble() {
        putAndCheckValue(new ExpressionValue.DoubleVal(), sql("field1", VARCHAR), VARCHAR, null);
        putAndCheckValue(new ExpressionValue.DoubleVal(), sql("field1", TINYINT), TINYINT, null);
        putAndCheckValue(new ExpressionValue.DoubleVal(), sql("field1", SMALLINT), SMALLINT, null);
        putAndCheckValue(new ExpressionValue.DoubleVal(), sql("field1", INTEGER), INTEGER, null);
        putAndCheckValue(new ExpressionValue.DoubleVal(), sql("field1", BIGINT), BIGINT, null);
        putAndCheckValue(new ExpressionValue.DoubleVal(), sql("field1", DECIMAL), DECIMAL, null);
        putAndCheckValue(new ExpressionValue.DoubleVal(), sql("field1", REAL), REAL, null);
        putAndCheckValue(new ExpressionValue.DoubleVal(), sql("field1", DOUBLE), DOUBLE, null);
        putAndCheckValue(new ExpressionValue.DoubleVal(), sql("field1", OBJECT), OBJECT, null);

        putAndCheckValue(1.1d, sql("this", TINYINT), TINYINT, (byte) 1);
        putAndCheckValue(1.1d, sql("this", SMALLINT), SMALLINT, (short) 1);
        putAndCheckValue(1.1d, sql("this", INTEGER), INTEGER, 1);
        putAndCheckValue(1.1d, sql("this", BIGINT), BIGINT, 1L);
        putAndCheckValue(1d, sql("this", DECIMAL), DECIMAL, decimal("1"));
        putAndCheckValue(1.1d, sql("this", REAL), REAL, 1.1f);
        putAndCheckValue(1d, sql("this", DOUBLE), DOUBLE, 1d);
        putAndCheckValue(1.1d, sql("this", OBJECT), OBJECT, 1.1d);

        putAndCheckFailure(0d, sql("this", BOOLEAN), PARSING, castError(DOUBLE, BOOLEAN));
        putAndCheckFailure(0d, sql("this", DATE), PARSING, castError(DOUBLE, DATE));
        putAndCheckFailure(0d, sql("this", TIME), PARSING, castError(DOUBLE, TIME));
        putAndCheckFailure(0d, sql("this", TIMESTAMP), PARSING, castError(DOUBLE, TIMESTAMP));
        putAndCheckFailure(0d, sql("this", TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(DOUBLE, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(0d, sql("this", JSON), PARSING, castError(DOUBLE, JSON));
    }

    /**
     * Tests that ensure tha our workaround to simplification of casts with approximate literals works fine.
     * See {@code HazelcastSqlToRelConverter.convertCast(...)}.
     */
    @Test
    public void testApproximateTypeSimplification() {
        put(1);

        checkValue0("select 1 = cast(1.0000001 as real) from map", BOOLEAN, false);
        checkValue0("select 1.0E0 = cast(1.0000001 as real) from map", BOOLEAN, false);
        checkValue0("select cast(1.0 as real) = cast(1.0000001 as real) from map", BOOLEAN, false);

        checkValue0("select 1 = cast(1.00000001 as real) from map", BOOLEAN, true);
        checkValue0("select 1.0E0 = cast(1.00000001 as real) from map", BOOLEAN, true);
        checkValue0("select cast(1.0 as real) = cast(1.00000001 as real) from map", BOOLEAN, true);

        checkValue0("select 1 = cast(1.000000000000001 as double) from map", BOOLEAN, false);
        checkValue0("select 1.0E0 = cast(1.000000000000001 as double) from map", BOOLEAN, false);
        checkValue0("select cast(1.0 as double) = cast(1.000000000000001 as double) from map", BOOLEAN, false);

        checkValue0("select 1 = cast(1.0000000000000001 as double) from map", BOOLEAN, true);
        checkValue0("select 1.0E0 = cast(1.0000000000000001 as double) from map", BOOLEAN, true);
        checkValue0("select cast(1.0 as double) = cast(1.0000000000000001 as double) from map", BOOLEAN, true);
    }

    @Test
    public void testReal_literal() {
        put(1);

        checkValue0(sql("CAST(1.5 AS REAL)", OBJECT), OBJECT, 1.5F);
    }

    @Test
    public void testDouble_literal_small() {
        put(1);

        String literal = literal("1.1E1");

        checkValue0(sql(literal, VARCHAR), VARCHAR, literal);

        checkFailure0(sql(literal, BOOLEAN), PARSING, castError(DOUBLE, BOOLEAN));

        checkValue0(sql(literal, TINYINT), TINYINT, (byte) 11);
        checkValue0(sql(literal, SMALLINT), SMALLINT, (short) 11);
        checkValue0(sql(literal, INTEGER), INTEGER, 11);
        checkValue0(sql(literal, BIGINT), BIGINT, 11L);

        checkValue0(sql(literal, DECIMAL), DECIMAL, decimal("11"));

        checkValue0(sql(literal, REAL), REAL, 11f);
        checkValue0(sql(literal, DOUBLE), DOUBLE, 11d);

        checkFailure0(sql(literal, DATE), PARSING, castError(DOUBLE, DATE));
        checkFailure0(sql(literal, TIME), PARSING, castError(DOUBLE, TIME));
        checkFailure0(sql(literal, TIMESTAMP), PARSING, castError(DOUBLE, TIMESTAMP));
        checkFailure0(sql(literal, TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(DOUBLE, TIMESTAMP_WITH_TIME_ZONE));
        checkFailure0(sql(literal, JSON), PARSING, castError(DOUBLE, JSON));

        checkValue0(sql(literal, OBJECT), OBJECT, 11d);
    }

    @Test
    public void testDouble_literal_big() {
        put(1);

        String literal = literal("1.1E35");

        checkValue0(sql(literal, VARCHAR), VARCHAR, literal);
        checkFailure0(sql(literal, BOOLEAN), PARSING, castError(DOUBLE, BOOLEAN));

        checkFailure0(sql(literal, TINYINT), PARSING, "CAST function cannot convert literal 1.1E35 to type TINYINT: Numeric overflow while converting DOUBLE to TINYINT");
        checkFailure0(sql(literal, SMALLINT), PARSING, "CAST function cannot convert literal 1.1E35 to type SMALLINT: Numeric overflow while converting DOUBLE to SMALLINT");
        checkFailure0(sql(literal, INTEGER), PARSING, "CAST function cannot convert literal 1.1E35 to type INTEGER: Numeric overflow while converting DOUBLE to INTEGER");
        checkFailure0(sql(literal, BIGINT), PARSING, "CAST function cannot convert literal 1.1E35 to type BIGINT: Numeric overflow while converting DOUBLE to BIGINT");
        checkValue0(sql(literal, DECIMAL), DECIMAL, decimal("1.1E35"));
        checkValue0(sql(literal, REAL), REAL, 1.1E35F);
        checkValue0(sql(literal, DOUBLE), DOUBLE, 1.1E35D);

        checkFailure0(sql(literal, DATE), PARSING, castError(DOUBLE, DATE));
        checkFailure0(sql(literal, TIME), PARSING, castError(DOUBLE, TIME));
        checkFailure0(sql(literal, TIMESTAMP), PARSING, castError(DOUBLE, TIMESTAMP));
        checkFailure0(sql(literal, TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(DOUBLE, TIMESTAMP_WITH_TIME_ZONE));
        checkFailure0(sql(literal, JSON), PARSING, castError(DOUBLE, JSON));

        checkValue0(sql(literal, OBJECT), OBJECT, 1.1E35D);
    }

    @Test
    public void testDouble_literal_huge() {
        put(1);

        String literal = literal("1.1E100");

        checkValue0(sql(literal, VARCHAR), VARCHAR, literal);
        checkFailure0(sql(literal, BOOLEAN), PARSING, castError(DOUBLE, BOOLEAN));

        checkFailure0(sql(literal, TINYINT), PARSING, "CAST function cannot convert literal 1.1E100 to type TINYINT: Numeric overflow while converting DOUBLE to TINYINT");
        checkFailure0(sql(literal, SMALLINT), PARSING, "CAST function cannot convert literal 1.1E100 to type SMALLINT: Numeric overflow while converting DOUBLE to SMALLINT");
        checkFailure0(sql(literal, INTEGER), PARSING, "CAST function cannot convert literal 1.1E100 to type INTEGER: Numeric overflow while converting DOUBLE to INTEGER");
        checkFailure0(sql(literal, BIGINT), PARSING, "CAST function cannot convert literal 1.1E100 to type BIGINT: Numeric overflow while converting DOUBLE to BIGINT");
        checkValue0(sql(literal, DECIMAL), DECIMAL, decimal("1.100000000000000036919869142993200560714308010269170019300014421873657477457E+100"));
        checkFailure0(sql(literal, REAL), PARSING, "CAST function cannot convert literal 1.1E100 to type REAL: Numeric overflow while converting DOUBLE to REAL");
        checkValue0(sql(literal, DOUBLE), DOUBLE, 1.1E100D);

        checkFailure0(sql(literal, DATE), PARSING, castError(DOUBLE, DATE));
        checkFailure0(sql(literal, TIME), PARSING, castError(DOUBLE, TIME));
        checkFailure0(sql(literal, TIMESTAMP), PARSING, castError(DOUBLE, TIMESTAMP));
        checkFailure0(sql(literal, TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(DOUBLE, TIMESTAMP_WITH_TIME_ZONE));
        checkFailure0(sql(literal, JSON), PARSING, castError(DOUBLE, JSON));

        checkValue0(sql(literal, OBJECT), OBJECT, 1.1E100D);
    }

    @Test
    public void testDate() {
        putAndCheckValue(new ExpressionValue.LocalDateVal(), sql("field1", VARCHAR), VARCHAR, null);
        putAndCheckValue(new ExpressionValue.LocalDateVal(), sql("field1", DATE), DATE, null);
        putAndCheckValue(new ExpressionValue.LocalDateVal(), sql("field1", TIMESTAMP), TIMESTAMP, null);
        putAndCheckValue(new ExpressionValue.LocalDateVal(), sql("field1", TIMESTAMP_WITH_TIME_ZONE), TIMESTAMP_WITH_TIME_ZONE, null);
        putAndCheckValue(new ExpressionValue.LocalDateVal(), sql("field1", OBJECT), OBJECT, null);

        putAndCheckValue(LOCAL_DATE_VAL, sql("this", VARCHAR), VARCHAR, LOCAL_DATE_VAL.toString());
        putAndCheckValue(LOCAL_DATE_VAL, sql("this", DATE), DATE, LOCAL_DATE_VAL);
        putAndCheckValue(LOCAL_DATE_VAL, sql("this", TIMESTAMP), TIMESTAMP, LocalDateConverter.INSTANCE.asTimestamp(LOCAL_DATE_VAL));
        putAndCheckValue(LOCAL_DATE_VAL, sql("this", TIMESTAMP_WITH_TIME_ZONE), TIMESTAMP_WITH_TIME_ZONE, LocalDateConverter.INSTANCE.asTimestampWithTimezone(LOCAL_DATE_VAL));
        putAndCheckValue(LOCAL_DATE_VAL, sql("this", OBJECT), OBJECT, LOCAL_DATE_VAL);

        putAndCheckFailure(LOCAL_DATE_VAL, sql("this", BOOLEAN), PARSING, castError(DATE, BOOLEAN));
        putAndCheckFailure(LOCAL_DATE_VAL, sql("this", TINYINT), PARSING, castError(DATE, TINYINT));
        putAndCheckFailure(LOCAL_DATE_VAL, sql("this", SMALLINT), PARSING, castError(DATE, SMALLINT));
        putAndCheckFailure(LOCAL_DATE_VAL, sql("this", INTEGER), PARSING, castError(DATE, INTEGER));
        putAndCheckFailure(LOCAL_DATE_VAL, sql("this", BIGINT), PARSING, castError(DATE, BIGINT));
        putAndCheckFailure(LOCAL_DATE_VAL, sql("this", DECIMAL), PARSING, castError(DATE, DECIMAL));
        putAndCheckFailure(LOCAL_DATE_VAL, sql("this", REAL), PARSING, castError(DATE, REAL));
        putAndCheckFailure(LOCAL_DATE_VAL, sql("this", DOUBLE), PARSING, castError(DATE, DOUBLE));
        putAndCheckFailure(LOCAL_DATE_VAL, sql("this", TIME), PARSING, castError(DATE, TIME));
        putAndCheckFailure(LOCAL_DATE_VAL, sql("this", JSON), PARSING, castError(DATE, JSON));
    }

    @Test
    public void testTime() {
        putAndCheckValue(new ExpressionValue.LocalTimeVal(), sql("field1", VARCHAR), VARCHAR, null);
        putAndCheckValue(new ExpressionValue.LocalTimeVal(), sql("field1", TIME), TIME, null);
        putAndCheckValue(new ExpressionValue.LocalTimeVal(), sql("field1", TIMESTAMP), TIMESTAMP, null);
        putAndCheckValue(new ExpressionValue.LocalTimeVal(), sql("field1", TIMESTAMP_WITH_TIME_ZONE), TIMESTAMP_WITH_TIME_ZONE, null);
        putAndCheckValue(new ExpressionValue.LocalTimeVal(), sql("field1", OBJECT), OBJECT, null);

        putAndCheckValue(LOCAL_TIME_VAL, sql("this", VARCHAR), VARCHAR, LOCAL_TIME_VAL.toString());
        putAndCheckValue(LOCAL_TIME_VAL, sql("this", TIME), TIME, LOCAL_TIME_VAL);
        putAndCheckValue(LOCAL_TIME_VAL, sql("this", TIMESTAMP), TIMESTAMP, LocalTimeConverter.INSTANCE.asTimestamp(LOCAL_TIME_VAL));
        putAndCheckValue(LOCAL_TIME_VAL, sql("this", TIMESTAMP_WITH_TIME_ZONE), TIMESTAMP_WITH_TIME_ZONE, LocalTimeConverter.INSTANCE.asTimestampWithTimezone(LOCAL_TIME_VAL));
        putAndCheckValue(LOCAL_TIME_VAL, sql("this", OBJECT), OBJECT, LOCAL_TIME_VAL);

        putAndCheckFailure(LOCAL_TIME_VAL, sql("this", BOOLEAN), PARSING, castError(TIME, BOOLEAN));
        putAndCheckFailure(LOCAL_TIME_VAL, sql("this", TINYINT), PARSING, castError(TIME, TINYINT));
        putAndCheckFailure(LOCAL_TIME_VAL, sql("this", SMALLINT), PARSING, castError(TIME, SMALLINT));
        putAndCheckFailure(LOCAL_TIME_VAL, sql("this", INTEGER), PARSING, castError(TIME, INTEGER));
        putAndCheckFailure(LOCAL_TIME_VAL, sql("this", BIGINT), PARSING, castError(TIME, BIGINT));
        putAndCheckFailure(LOCAL_TIME_VAL, sql("this", DECIMAL), PARSING, castError(TIME, DECIMAL));
        putAndCheckFailure(LOCAL_TIME_VAL, sql("this", REAL), PARSING, castError(TIME, REAL));
        putAndCheckFailure(LOCAL_TIME_VAL, sql("this", DOUBLE), PARSING, castError(TIME, DOUBLE));
        putAndCheckFailure(LOCAL_TIME_VAL, sql("this", DATE), PARSING, castError(TIME, DATE));
        putAndCheckFailure(LOCAL_TIME_VAL, sql("this", JSON), PARSING, castError(TIME, JSON));
    }

    @Test
    public void testTimestamp() {
        putAndCheckValue(new ExpressionValue.LocalDateTimeVal(), sql("field1", VARCHAR), VARCHAR, null);
        putAndCheckValue(new ExpressionValue.LocalDateTimeVal(), sql("field1", DATE), DATE, null);
        putAndCheckValue(new ExpressionValue.LocalDateTimeVal(), sql("field1", TIME), TIME, null);
        putAndCheckValue(new ExpressionValue.LocalDateTimeVal(), sql("field1", TIMESTAMP), TIMESTAMP, null);
        putAndCheckValue(new ExpressionValue.LocalDateTimeVal(), sql("field1", TIMESTAMP_WITH_TIME_ZONE), TIMESTAMP_WITH_TIME_ZONE, null);
        putAndCheckValue(new ExpressionValue.LocalDateTimeVal(), sql("field1", OBJECT), OBJECT, null);

        putAndCheckValue(LOCAL_DATE_TIME_VAL, sql("this", VARCHAR), VARCHAR, LOCAL_DATE_TIME_VAL.toString());
        putAndCheckValue(LOCAL_DATE_TIME_VAL, sql("this", DATE), DATE, LocalDateTimeConverter.INSTANCE.asDate(LOCAL_DATE_TIME_VAL));
        putAndCheckValue(LOCAL_DATE_TIME_VAL, sql("this", TIME), TIME, LocalDateTimeConverter.INSTANCE.asTime(LOCAL_DATE_TIME_VAL));
        putAndCheckValue(LOCAL_DATE_TIME_VAL, sql("this", TIMESTAMP), TIMESTAMP, LOCAL_DATE_TIME_VAL);
        putAndCheckValue(LOCAL_DATE_TIME_VAL, sql("this", TIMESTAMP_WITH_TIME_ZONE), TIMESTAMP_WITH_TIME_ZONE, LocalDateTimeConverter.INSTANCE.asTimestampWithTimezone(LOCAL_DATE_TIME_VAL));
        putAndCheckValue(LOCAL_DATE_TIME_VAL, sql("this", OBJECT), OBJECT, LOCAL_DATE_TIME_VAL);

        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql("this", BOOLEAN), PARSING, castError(TIMESTAMP, BOOLEAN));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql("this", TINYINT), PARSING, castError(TIMESTAMP, TINYINT));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql("this", SMALLINT), PARSING, castError(TIMESTAMP, SMALLINT));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql("this", INTEGER), PARSING, castError(TIMESTAMP, INTEGER));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql("this", BIGINT), PARSING, castError(TIMESTAMP, BIGINT));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql("this", DECIMAL), PARSING, castError(TIMESTAMP, DECIMAL));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql("this", REAL), PARSING, castError(TIMESTAMP, REAL));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql("this", DOUBLE), PARSING, castError(TIMESTAMP, DOUBLE));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql("this", JSON), PARSING, castError(TIMESTAMP, JSON));
    }

    @Test
    public void testTimestampWithTimezone() {
        putAndCheckValue(new ExpressionValue.OffsetDateTimeVal(), sql("field1", VARCHAR), VARCHAR, null);
        putAndCheckValue(new ExpressionValue.OffsetDateTimeVal(), sql("field1", DATE), DATE, null);
        putAndCheckValue(new ExpressionValue.OffsetDateTimeVal(), sql("field1", TIME), TIME, null);
        putAndCheckValue(new ExpressionValue.OffsetDateTimeVal(), sql("field1", TIMESTAMP), TIMESTAMP, null);
        putAndCheckValue(new ExpressionValue.OffsetDateTimeVal(), sql("field1", TIMESTAMP_WITH_TIME_ZONE), TIMESTAMP_WITH_TIME_ZONE, null);
        putAndCheckValue(new ExpressionValue.OffsetDateTimeVal(), sql("field1", OBJECT), OBJECT, null);

        putAndCheckValue(OFFSET_DATE_TIME_VAL, sql("this", VARCHAR), VARCHAR, OFFSET_DATE_TIME_VAL.toString());
        putAndCheckValue(OFFSET_DATE_TIME_VAL, sql("this", DATE), DATE, OffsetDateTimeConverter.INSTANCE.asDate(OFFSET_DATE_TIME_VAL));
        putAndCheckValue(OFFSET_DATE_TIME_VAL, sql("this", TIME), TIME, OffsetDateTimeConverter.INSTANCE.asTime(OFFSET_DATE_TIME_VAL));
        putAndCheckValue(OFFSET_DATE_TIME_VAL, sql("this", TIMESTAMP), TIMESTAMP, OffsetDateTimeConverter.INSTANCE.asTimestamp(OFFSET_DATE_TIME_VAL));
        putAndCheckValue(OFFSET_DATE_TIME_VAL, sql("this", TIMESTAMP_WITH_TIME_ZONE), TIMESTAMP_WITH_TIME_ZONE, OFFSET_DATE_TIME_VAL);
        putAndCheckValue(OFFSET_DATE_TIME_VAL, sql("this", OBJECT), OBJECT, OFFSET_DATE_TIME_VAL);

        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql("this", BOOLEAN), PARSING, castError(TIMESTAMP_WITH_TIME_ZONE, BOOLEAN));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql("this", TINYINT), PARSING, castError(TIMESTAMP_WITH_TIME_ZONE, TINYINT));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql("this", SMALLINT), PARSING, castError(TIMESTAMP_WITH_TIME_ZONE, SMALLINT));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql("this", INTEGER), PARSING, castError(TIMESTAMP_WITH_TIME_ZONE, INTEGER));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql("this", BIGINT), PARSING, castError(TIMESTAMP_WITH_TIME_ZONE, BIGINT));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql("this", DECIMAL), PARSING, castError(TIMESTAMP_WITH_TIME_ZONE, DECIMAL));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql("this", REAL), PARSING, castError(TIMESTAMP_WITH_TIME_ZONE, REAL));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql("this", DOUBLE), PARSING, castError(TIMESTAMP_WITH_TIME_ZONE, DOUBLE));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql("this", JSON), PARSING, castError(TIMESTAMP_WITH_TIME_ZONE, JSON));
    }

    @Test
    public void testObject() {
        putAndCheckValue(new ExpressionValue.ObjectVal(), sql("field1", VARCHAR), VARCHAR, null);
        putAndCheckValue(new ExpressionValue.ObjectVal(), sql("field1", BOOLEAN), BOOLEAN, null);
        putAndCheckValue(new ExpressionValue.ObjectVal(), sql("field1", TINYINT), TINYINT, null);
        putAndCheckValue(new ExpressionValue.ObjectVal(), sql("field1", SMALLINT), SMALLINT, null);
        putAndCheckValue(new ExpressionValue.ObjectVal(), sql("field1", INTEGER), INTEGER, null);
        putAndCheckValue(new ExpressionValue.ObjectVal(), sql("field1", BIGINT), BIGINT, null);
        putAndCheckValue(new ExpressionValue.ObjectVal(), sql("field1", DECIMAL), DECIMAL, null);
        putAndCheckValue(new ExpressionValue.ObjectVal(), sql("field1", REAL), REAL, null);
        putAndCheckValue(new ExpressionValue.ObjectVal(), sql("field1", DOUBLE), DOUBLE, null);
        putAndCheckValue(new ExpressionValue.ObjectVal(), sql("field1", DATE), DATE, null);
        putAndCheckValue(new ExpressionValue.ObjectVal(), sql("field1", TIME), TIME, null);
        putAndCheckValue(new ExpressionValue.ObjectVal(), sql("field1", TIMESTAMP), TIMESTAMP, null);
        putAndCheckValue(new ExpressionValue.ObjectVal(), sql("field1", TIMESTAMP_WITH_TIME_ZONE), TIMESTAMP_WITH_TIME_ZONE, null);
        putAndCheckValue(new ExpressionValue.ObjectVal(), sql("field1", OBJECT), OBJECT, null);

        putAndCheckValue(object(STRING_VAL), sql("field1", VARCHAR), VARCHAR, STRING_VAL);
        putAndCheckValue(object(BOOLEAN_VAL), sql("field1", BOOLEAN), BOOLEAN, BOOLEAN_VAL);
        putAndCheckValue(object(BYTE_VAL), sql("field1", TINYINT), TINYINT, BYTE_VAL);
        putAndCheckValue(object(SHORT_VAL), sql("field1", SMALLINT), SMALLINT, SHORT_VAL);
        putAndCheckValue(object(INTEGER_VAL), sql("field1", INTEGER), INTEGER, INTEGER_VAL);
        putAndCheckValue(object(LONG_VAL), sql("field1", BIGINT), BIGINT, LONG_VAL);
        putAndCheckValue(object(BIG_DECIMAL_VAL), sql("field1", DECIMAL), DECIMAL, BIG_DECIMAL_VAL);
        putAndCheckValue(object(FLOAT_VAL), sql("field1", REAL), REAL, FLOAT_VAL);
        putAndCheckValue(object(DOUBLE_VAL), sql("field1", DOUBLE), DOUBLE, DOUBLE_VAL);
        putAndCheckValue(object(LOCAL_DATE_VAL), sql("field1", DATE), DATE, LOCAL_DATE_VAL);
        putAndCheckValue(object(LOCAL_TIME_VAL), sql("field1", TIME), TIME, LOCAL_TIME_VAL);
        putAndCheckValue(object(LOCAL_DATE_TIME_VAL), sql("field1", TIMESTAMP), TIMESTAMP, LOCAL_DATE_TIME_VAL);
        putAndCheckValue(object(OFFSET_DATE_TIME_VAL), sql("field1", TIMESTAMP_WITH_TIME_ZONE), TIMESTAMP_WITH_TIME_ZONE, OFFSET_DATE_TIME_VAL);
        putAndCheckValue(object(STRING_VAL), sql("this", OBJECT), OBJECT, object(STRING_VAL));
    }

    @Test
    public void testJson() {
        final HazelcastJsonValue value = new HazelcastJsonValue("[1,2,3]");
        putAndCheckValue(value, sql("this", VARCHAR), VARCHAR, "[1,2,3]");
        putAndCheckValue(value, sql("this", OBJECT), OBJECT, value);

        putAndCheckFailure(value, sql("this", TINYINT), PARSING, castError(JSON, TINYINT));
        putAndCheckFailure(value, sql("this", SMALLINT), PARSING, castError(JSON, SMALLINT));
        putAndCheckFailure(value, sql("this", INTEGER), PARSING, castError(JSON, INTEGER));
        putAndCheckFailure(value, sql("this", BIGINT), PARSING, castError(JSON, BIGINT));
        putAndCheckFailure(value, sql("this", DECIMAL), PARSING, castError(JSON, DECIMAL));
        putAndCheckFailure(value, sql("this", REAL), PARSING, castError(JSON, REAL));
        putAndCheckFailure(value, sql("this", DOUBLE), PARSING, castError(JSON, DOUBLE));
        putAndCheckFailure(value, sql("this", DATE), PARSING, castError(JSON, DATE));
        putAndCheckFailure(value, sql("this", TIME), PARSING, castError(JSON, TIME));
        putAndCheckFailure(value, sql("this", TIMESTAMP), PARSING, castError(JSON, TIMESTAMP));
        putAndCheckFailure(value, sql("this", TIMESTAMP_WITH_TIME_ZONE), PARSING, castError(JSON, TIMESTAMP_WITH_TIME_ZONE));
    }

    @Test
    public void testNullLiteral() {
        put(1);

        checkValue0(sql("null", VARCHAR), VARCHAR, null);
        checkValue0(sql("null", BOOLEAN), BOOLEAN, null);
        checkValue0(sql("null", TINYINT), TINYINT, null);
        checkValue0(sql("null", SMALLINT), SMALLINT, null);
        checkValue0(sql("null", INTEGER), INTEGER, null);
        checkValue0(sql("null", BIGINT), BIGINT, null);
        checkValue0(sql("null", DECIMAL), DECIMAL, null);
        checkValue0(sql("null", REAL), REAL, null);
        checkValue0(sql("null", DOUBLE), DOUBLE, null);
        checkValue0(sql("null", DATE), DATE, null);
        checkValue0(sql("null", TIME), TIME, null);
        checkValue0(sql("null", TIMESTAMP), TIMESTAMP, null);
        checkValue0(sql("null", TIMESTAMP_WITH_TIME_ZONE), TIMESTAMP_WITH_TIME_ZONE, null);
        checkValue0(sql("null", OBJECT), OBJECT, null);
    }

    @Test
    public void testNestedCastsOfLiterals() {
        // tests for https://github.com/hazelcast/hazelcast/issues/18155
        put(1);
        checkFailure0(sql("CAST(128 AS INTEGER)", TINYINT), PARSING, "Numeric overflow while converting SMALLINT to TINYINT");
        checkFailure0(sql("CAST(128 AS SMALLINT)", TINYINT), PARSING, "Numeric overflow while converting SMALLINT to TINYINT");
        checkValue0(sql("CAST(42 AS SMALLINT)", TINYINT), TINYINT, (byte) 42);
    }

    @Test
    public void testParameter() {
        put(0);

        checkValue0(sql("?", VARCHAR), VARCHAR, "1", 1);
        checkValue0(sql("?", INTEGER), INTEGER, 1, "1");
        checkValue0(sql("?", JSON), JSON, new HazelcastJsonValue("[1]"), "[1]");
    }

    @Test
    public void testEquality() {
        checkEquals(CastExpression.create(ConstantExpression.create(1, INT), QueryDataType.BIGINT),
                CastExpression.create(ConstantExpression.create(1, INT), QueryDataType.BIGINT), true);

        checkEquals(CastExpression.create(ConstantExpression.create(1, INT), QueryDataType.BIGINT),
                CastExpression.create(ConstantExpression.create(1, INT), QueryDataType.DOUBLE), false);

        checkEquals(CastExpression.create(ConstantExpression.create(1, INT), QueryDataType.BIGINT),
                CastExpression.create(ConstantExpression.create(2, INT), QueryDataType.BIGINT), false);
    }

    @Test
    public void testSerialization() {
        CastExpression<?> original = CastExpression.create(ConstantExpression.create(1, INT), QueryDataType.BIGINT);
        CastExpression<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_CAST);

        checkEquals(original, restored, true);
    }

    private static String sql(String source, SqlColumnType targetType) {
        return "SELECT CAST(" + source + " AS " + targetType + ") FROM map";
    }

    protected String castError(SqlColumnType type1, SqlColumnType type2) {
        return "CAST function cannot convert value of type " + type1 + " to type " + type2;
    }

    private static String string(Object value) {
        return value.toString();
    }

    private static BigDecimal decimal(Object value) {
        return new BigDecimal(value.toString());
    }

    private static ExpressionValue object(Object value) {
        return new ExpressionValue.ObjectVal().field1(value);
    }

    private static String stringLiteral(Object value) {
        return "'" + value + "'";
    }

    private static String literal(Object value) {
        return "" + value;
    }
}
