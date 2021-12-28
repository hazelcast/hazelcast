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

package com.hazelcast.jet.sql.impl.expression.math;

import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.ByteIntegerVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.IntegerIntegerVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.IntegerObjectVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.LongIntegerVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.ShortIntegerVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue.BigIntegerVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue.ByteVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue.IntegerVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue.LongVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue.ShortVal;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BigDecimalIntegerVal;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BigDecimalVal;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BigIntegerIntegerVal;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.DoubleIntegerVal;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.DoubleVal;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.FloatIntegerVal;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.FloatVal;
import static com.hazelcast.sql.SqlColumnType.BIGINT;
import static com.hazelcast.sql.SqlColumnType.BOOLEAN;
import static com.hazelcast.sql.SqlColumnType.DATE;
import static com.hazelcast.sql.SqlColumnType.DECIMAL;
import static com.hazelcast.sql.SqlColumnType.DOUBLE;
import static com.hazelcast.sql.SqlColumnType.INTEGER;
import static com.hazelcast.sql.SqlColumnType.OBJECT;
import static com.hazelcast.sql.SqlColumnType.REAL;
import static com.hazelcast.sql.SqlColumnType.SMALLINT;
import static com.hazelcast.sql.SqlColumnType.TIME;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.SqlColumnType.TINYINT;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RoundFunctionIntegrationTest extends ExpressionTestSupport {
    @Test
    public void test_byte() {
        checkColumn_1(new ByteVal().field1((byte) 127), TINYINT, (byte) 127);
        checkColumn_1(new ByteVal().field1((byte) -128), TINYINT, (byte) -128);

        checkColumn_2(new ByteIntegerVal().fields((byte) 127, 1), TINYINT, (byte) 127);
        checkColumn_2(new ByteIntegerVal().fields((byte) 127, 0), TINYINT, (byte) 127);
        checkColumnFailure_2(new ByteIntegerVal().fields((byte) 127, -1), SqlErrorCode.DATA_EXCEPTION, "TINYINT overflow in ROUND function (consider adding an explicit CAST to SMALLINT)");
        checkColumn_2(new ByteIntegerVal().fields((byte) 127, -2), TINYINT, (byte) 100);
        checkColumn_2(new ByteIntegerVal().fields((byte) 127, -3), TINYINT, (byte) 0);
        checkColumn_2(new ByteIntegerVal().fields((byte) 127, -4), TINYINT, (byte) 0);

        checkColumn_2(new ByteIntegerVal().fields((byte) -128, 1), TINYINT, (byte) -128);
        checkColumn_2(new ByteIntegerVal().fields((byte) -128, 0), TINYINT, (byte) -128);
        checkColumnFailure_2(new ByteIntegerVal().fields((byte) -128, -1), SqlErrorCode.DATA_EXCEPTION, "TINYINT overflow in ROUND function (consider adding an explicit CAST to SMALLINT)");
        checkColumn_2(new ByteIntegerVal().fields((byte) -128, -2), TINYINT, (byte) -100);
        checkColumn_2(new ByteIntegerVal().fields((byte) -128, -3), TINYINT, (byte) 0);
        checkColumn_2(new ByteIntegerVal().fields((byte) -128, -4), TINYINT, (byte) 0);
    }

    @Test
    public void test_short() {
        checkColumn_1(new ShortVal().field1((short) 32767), SMALLINT, (short) 32767);
        checkColumn_1(new ShortVal().field1((short) -32768), SMALLINT, (short) -32768);

        checkColumn_2(new ShortIntegerVal().fields((short) 32767, 1), SMALLINT, (short) 32767);
        checkColumn_2(new ShortIntegerVal().fields((short) 32767, 0), SMALLINT, (short) 32767);
        checkColumnFailure_2(new ShortIntegerVal().fields((short) 32767, -1), SqlErrorCode.DATA_EXCEPTION, "SMALLINT overflow in ROUND function (consider adding an explicit CAST to INTEGER)");
        checkColumn_2(new ShortIntegerVal().fields((short) 32767, -4), SMALLINT, (short) 30000);
        checkColumn_2(new ShortIntegerVal().fields((short) 32767, -5), SMALLINT, (short) 0);
        checkColumn_2(new ShortIntegerVal().fields((short) 32767, -6), SMALLINT, (short) 0);

        checkColumn_2(new ShortIntegerVal().fields((short) -32768, 1), SMALLINT, (short) -32768);
        checkColumn_2(new ShortIntegerVal().fields((short) -32768, 0), SMALLINT, (short) -32768);
        checkColumnFailure_2(new ShortIntegerVal().fields((short) -32768, -1), SqlErrorCode.DATA_EXCEPTION, "SMALLINT overflow in ROUND function (consider adding an explicit CAST to INTEGER)");
        checkColumn_2(new ShortIntegerVal().fields((short) -32768, -4), SMALLINT, (short) -30000);
        checkColumn_2(new ShortIntegerVal().fields((short) -32768, -5), SMALLINT, (short) 0);
        checkColumn_2(new ShortIntegerVal().fields((short) -32768, -6), SMALLINT, (short) 0);
    }

    @Test
    public void test_int() {
        checkColumn_1(new IntegerVal().field1(2_147_483_647), INTEGER, 2_147_483_647);
        checkColumn_1(new IntegerVal().field1(-2_147_483_648), INTEGER, -2_147_483_648);

        checkColumn_2(new IntegerIntegerVal().fields(2_147_483_647, 1), INTEGER, 2_147_483_647);
        checkColumn_2(new IntegerIntegerVal().fields(2_147_483_647, 0), INTEGER, 2_147_483_647);
        checkColumnFailure_2(new IntegerIntegerVal().fields(2_147_483_647, -1), SqlErrorCode.DATA_EXCEPTION, "INTEGER overflow in ROUND function (consider adding an explicit CAST to BIGINT)");
        checkColumn_2(new IntegerIntegerVal().fields(2_147_483_647, -2), INTEGER, 2_147_483_600);
        checkColumn_2(new IntegerIntegerVal().fields(2_147_483_647, -10), INTEGER, 0);
        checkColumn_2(new IntegerIntegerVal().fields(2_147_483_647, -11), INTEGER, 0);

        checkColumn_2(new IntegerIntegerVal().fields(-2_147_483_648, 1), INTEGER, -2_147_483_648);
        checkColumn_2(new IntegerIntegerVal().fields(-2_147_483_648, 0), INTEGER, -2_147_483_648);
        checkColumnFailure_2(new IntegerIntegerVal().fields(-2_147_483_648, -1), SqlErrorCode.DATA_EXCEPTION, "INTEGER overflow in ROUND function (consider adding an explicit CAST to BIGINT)");
        checkColumn_2(new IntegerIntegerVal().fields(-2_147_483_648, -2), INTEGER, -2_147_483_600);
        checkColumn_2(new IntegerIntegerVal().fields(2_147_483_647, -10), INTEGER, 0);
        checkColumn_2(new IntegerIntegerVal().fields(2_147_483_647, -11), INTEGER, 0);
    }

    @Test
    public void test_long() {
        checkColumn_1(new LongVal().field1(9_223_372_036_854_775_807L), BIGINT, 9_223_372_036_854_775_807L);
        checkColumn_1(new LongVal().field1(-9_223_372_036_854_775_808L), BIGINT, -9_223_372_036_854_775_808L);

        checkColumn_2(new LongIntegerVal().fields(9_223_372_036_854_775_807L, 1), BIGINT, 9_223_372_036_854_775_807L);
        checkColumn_2(new LongIntegerVal().fields(9_223_372_036_854_775_807L, 0), BIGINT, 9_223_372_036_854_775_807L);
        checkColumnFailure_2(new LongIntegerVal().fields(9_223_372_036_854_775_807L, -1), SqlErrorCode.DATA_EXCEPTION, "BIGINT overflow in ROUND function (consider adding an explicit CAST to DECIMAL)");
        checkColumn_2(new LongIntegerVal().fields(9_223_372_036_854_775_807L, -2), BIGINT, 9_223_372_036_854_775_800L);
        checkColumnFailure_2(new LongIntegerVal().fields(9_223_372_036_854_775_807L, -19), SqlErrorCode.DATA_EXCEPTION, "BIGINT overflow in ROUND function (consider adding an explicit CAST to DECIMAL)");
        checkColumn_2(new LongIntegerVal().fields(9_223_372_036_854_775_807L, -20), BIGINT, 0L);

        checkColumn_2(new LongIntegerVal().fields(-9_223_372_036_854_775_808L, 1), BIGINT, -9_223_372_036_854_775_808L);
        checkColumn_2(new LongIntegerVal().fields(-9_223_372_036_854_775_808L, 0), BIGINT, -9_223_372_036_854_775_808L);
        checkColumnFailure_2(new LongIntegerVal().fields(-9_223_372_036_854_775_808L, -1), SqlErrorCode.DATA_EXCEPTION, "BIGINT overflow in ROUND function (consider adding an explicit CAST to DECIMAL)");
        checkColumn_2(new LongIntegerVal().fields(-9_223_372_036_854_775_808L, -2), BIGINT, -9_223_372_036_854_775_800L);
        checkColumnFailure_2(new LongIntegerVal().fields(-9_223_372_036_854_775_808L, -19), SqlErrorCode.DATA_EXCEPTION, "BIGINT overflow in ROUND function (consider adding an explicit CAST to DECIMAL)");
        checkColumn_2(new LongIntegerVal().fields(-9_223_372_036_854_775_808L, -20), BIGINT, 0L);
    }

    @Test
    public void test_BigInteger() {
        checkColumn_1(new BigIntegerVal().field1(new BigInteger("15")), DECIMAL, new BigDecimal("15"));
        checkColumn_1(new BigIntegerVal().field1(new BigInteger("-15")), DECIMAL, new BigDecimal("-15"));

        checkColumn_2(new BigIntegerIntegerVal().fields(new BigInteger("15"), -1), DECIMAL, new BigDecimal("20"));
        checkColumn_2(new BigIntegerIntegerVal().fields(new BigInteger("15"), -2), DECIMAL, new BigDecimal("0"));

        checkColumn_2(new BigIntegerIntegerVal().fields(new BigInteger("-15"), -1), DECIMAL, new BigDecimal("-20"));
        checkColumn_2(new BigIntegerIntegerVal().fields(new BigInteger("-15"), -2), DECIMAL, new BigDecimal("0"));
    }

    @Test
    public void test_BigDecimal() {
        checkColumn_1(new BigDecimalVal().field1(new BigDecimal("15.4")), DECIMAL, new BigDecimal("15"));
        checkColumn_1(new BigDecimalVal().field1(new BigDecimal("15.5")), DECIMAL, new BigDecimal("16"));
        checkColumn_1(new BigDecimalVal().field1(new BigDecimal("-15.4")), DECIMAL, new BigDecimal("-15"));
        checkColumn_1(new BigDecimalVal().field1(new BigDecimal("-15.5")), DECIMAL, new BigDecimal("-16"));

        checkColumn_2(new BigDecimalIntegerVal().fields(new BigDecimal("15.5"), -1), DECIMAL, new BigDecimal("20"));
        checkColumn_2(new BigDecimalIntegerVal().fields(new BigDecimal("15.5"), -2), DECIMAL, new BigDecimal("0"));

        checkColumn_2(new BigDecimalIntegerVal().fields(new BigDecimal("-15.5"), -1), DECIMAL, new BigDecimal("-20"));
        checkColumn_2(new BigDecimalIntegerVal().fields(new BigDecimal("-15.5"), -2), DECIMAL, new BigDecimal("0"));
    }

    @Test
    public void test_float() {
        checkColumn_1(new FloatVal().field1(15.4f), REAL, 15f);
        checkColumn_1(new FloatVal().field1(15.5f), REAL, 16f);
        checkColumn_1(new FloatVal().field1(-15.4f), REAL, -15f);
        checkColumn_1(new FloatVal().field1(-15.5f), REAL, -16f);

        checkColumn_2(new FloatIntegerVal().fields(15.5f, -1), REAL, 20f);
        checkColumn_2(new FloatIntegerVal().fields(15.5f, -2), REAL, 0f);

        checkColumn_2(new FloatIntegerVal().fields(-15.5f, -1), REAL, -20f);
        checkColumn_2(new FloatIntegerVal().fields(-15.5f, -2), REAL, 0f);

        checkColumn_2(new FloatIntegerVal().fields(Float.POSITIVE_INFINITY, -1), REAL, Float.POSITIVE_INFINITY);
        checkColumn_2(new FloatIntegerVal().fields(Float.NEGATIVE_INFINITY, -1), REAL, Float.NEGATIVE_INFINITY);
        checkColumn_2(new FloatIntegerVal().fields(Float.NaN, -1), REAL, Float.NaN);
    }

    @Test
    public void test_double() {
        checkColumn_1(new DoubleVal().field1(15.4d), DOUBLE, 15d);
        checkColumn_1(new DoubleVal().field1(15.5d), DOUBLE, 16d);
        checkColumn_1(new DoubleVal().field1(-15.4d), DOUBLE, -15d);
        checkColumn_1(new DoubleVal().field1(-15.5d), DOUBLE, -16d);

        checkColumn_2(new DoubleIntegerVal().fields(15.5d, -1), DOUBLE, 20d);
        checkColumn_2(new DoubleIntegerVal().fields(15.5d, -2), DOUBLE, 0d);

        checkColumn_2(new DoubleIntegerVal().fields(-15.5d, -1), DOUBLE, -20d);
        checkColumn_2(new DoubleIntegerVal().fields(-15.5d, -2), DOUBLE, 0d);

        checkColumn_2(new DoubleIntegerVal().fields(Double.POSITIVE_INFINITY, -1), DOUBLE, Double.POSITIVE_INFINITY);
        checkColumn_2(new DoubleIntegerVal().fields(Double.NEGATIVE_INFINITY, -1), DOUBLE, Double.NEGATIVE_INFINITY);
        checkColumn_2(new DoubleIntegerVal().fields(Double.NaN, -1), DOUBLE, Double.NaN);
    }

    @Test
    public void test_boolean() {
        checkColumnFailure_2(new ExpressionBiValue.BooleanIntegerVal().fields(true, 127), SqlErrorCode.PARSING, signatureErorr(BOOLEAN, INTEGER));
        checkColumnFailure_2(new ExpressionBiValue.IntegerBooleanVal().fields(127, true), SqlErrorCode.PARSING, signatureErorr(INTEGER, BOOLEAN));
    }

    @Test
    public void test_temporal() {
        checkColumnFailure_2(new ExpressionBiValue.IntegerLocalDateVal().fields(127, LOCAL_DATE_VAL), SqlErrorCode.PARSING, signatureErorr(INTEGER, DATE));
        checkColumnFailure_2(new ExpressionBiValue.IntegerLocalTimeVal().fields(127, LOCAL_TIME_VAL), SqlErrorCode.PARSING, signatureErorr(INTEGER, TIME));
        checkColumnFailure_2(new ExpressionBiValue.IntegerLocalDateTimeVal().fields(127, LOCAL_DATE_TIME_VAL), SqlErrorCode.PARSING, signatureErorr(INTEGER, TIMESTAMP));
        checkColumnFailure_2(new ExpressionBiValue.IntegerOffsetDateTimeVal().fields(127, OFFSET_DATE_TIME_VAL), SqlErrorCode.PARSING, signatureErorr(INTEGER, TIMESTAMP_WITH_TIME_ZONE));
    }

    @Test
    public void test_object() {
        checkColumnFailure_2(new IntegerObjectVal().fields(127, "bad"), SqlErrorCode.PARSING, signatureErorr(INTEGER, OBJECT));
    }

    @Test
    public void testParameters() {
        // One operand
        put(new IntegerVal().field1(0));
        check_1("?", DECIMAL, new BigDecimal("10"), (byte) 10);
        check_1("?", DECIMAL, new BigDecimal("10"), (short) 10);
        check_1("?", DECIMAL, new BigDecimal("10"), 10);
        check_1("?", DECIMAL, new BigDecimal("10"), 10L);
        check_1("?", DECIMAL, new BigDecimal("10"), new BigInteger("10"));
        check_1("?", DECIMAL, new BigDecimal("10"), new BigDecimal("9.5"));
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, REAL), 9.5f);
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, DOUBLE), 9.5d);
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), "9.5d");
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, DATE), LOCAL_DATE_VAL);
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, TIME), LOCAL_TIME_VAL);
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, OBJECT), OBJECT_VAL);
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, BOOLEAN), true);

        // Two operands, first operand
        check_2("?", "0", DECIMAL, new BigDecimal("10"), (byte) 10);
        check_2("?", "0", DECIMAL, new BigDecimal("10"), (short) 10);
        check_2("?", "0", DECIMAL, new BigDecimal("10"), 10);
        check_2("?", "0", DECIMAL, new BigDecimal("10"), 10L);
        check_2("?", "0", DECIMAL, new BigDecimal("10"), new BigInteger("10"));
        check_2("?", "0", DECIMAL, new BigDecimal("10"), new BigDecimal("9.5"));
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, REAL), 9.5f);
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, DOUBLE), 9.5d);
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), "9.5");
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, DATE), LOCAL_DATE_VAL);
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, TIME), LOCAL_TIME_VAL);
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, OBJECT), OBJECT_VAL);
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, BOOLEAN), true);

        // Two operands, second operand
        check_2("15", "?", TINYINT, (byte) 20, (byte) -1);
        check_2("15", "?", TINYINT, (byte) 20, (short) -1);
        check_2("15", "?", TINYINT, (byte) 20, -1);
        check_2("15", "?", TINYINT, (byte) 20, -1L);
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, INTEGER, DECIMAL), BigInteger.ONE.negate());
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, INTEGER, DECIMAL), BigDecimal.ONE.negate());
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, INTEGER, VARCHAR), "-1");
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, INTEGER, DATE), LOCAL_DATE_VAL);
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, INTEGER, TIME), LOCAL_TIME_VAL);
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, INTEGER, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, INTEGER, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, INTEGER, OBJECT), OBJECT_VAL);
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, INTEGER, BOOLEAN), true);

        // Two operands, both
        check_2("?", "?", DECIMAL, new BigDecimal("20"), 15, -1);
    }

    @Test
    public void testLiterals() {
        // Single operand
        put(new IntegerVal().field1(0));

        check_1("null", DECIMAL, null);

        check_1("15.1", DECIMAL, new BigDecimal("15"));
        check_1("15.5", DECIMAL, new BigDecimal("16"));
        check_1("-15.1", DECIMAL, new BigDecimal("-15"));
        check_1("-15.5", DECIMAL, new BigDecimal("-16"));

        check_1("15.1E0", DOUBLE, 15d);
        check_1("15.5E0", DOUBLE, 16d);
        check_1("-15.1E0", DOUBLE, -15d);
        check_1("-15.5E0", DOUBLE, -16d);

        // First operand
        put(new IntegerVal().field1(-1));
        check_2("15", "field1", TINYINT, (byte) 20);
        check_2("15.1", "field1", DECIMAL, new BigDecimal("20"));
        checkFailure_2("'15.1'", "field1", SqlErrorCode.PARSING, signatureErorr(VARCHAR, INTEGER));
        checkFailure_2("true", "field1", SqlErrorCode.PARSING, signatureErorr(BOOLEAN, INTEGER));

        // Second operand
        put(new IntegerVal().field1(15));
        check_2("field1", "-1", INTEGER, 20);
        checkFailure_2("field1", "'-1'", SqlErrorCode.PARSING, signatureErorr(INTEGER, VARCHAR));
        checkFailure_2("field1", "true", SqlErrorCode.PARSING, signatureErorr(INTEGER, BOOLEAN));
    }

    private void checkColumn_1(ExpressionValue value, SqlColumnType expectedType, Object expectedValue) {
        put(value);

        check_1("field1", expectedType, expectedValue);
    }

    private void checkColumn_2(ExpressionBiValue value, SqlColumnType expectedType, Object expectedValue) {
        put(value);

        check_2("field1", "field2", expectedType, expectedValue);
    }

    private void checkColumnFailure_2(ExpressionBiValue value, int expectedErrorCode, String expectedErrorMessage) {
        put(value);

        String sql = sql("field1", "field2");

        checkFailure0(sql, expectedErrorCode, expectedErrorMessage);
    }

    private void checkFailure_1(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = sql(operand);

        checkFailure0(sql, expectedErrorCode, expectedErrorMessage, params);
    }

    private void checkFailure_2(Object operand1, Object operand2, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = sql(operand1, operand2);

        checkFailure0(sql, expectedErrorCode, expectedErrorMessage, params);
    }

    private void check_1(Object operand, SqlColumnType expectedType, Object expectedValue, Object... params) {
        checkValue0(sql(operand), expectedType, expectedValue, params);
    }

    private void check_2(Object operand1, Object operand2, SqlColumnType expectedType, Object expectedValue, Object... params) {
        checkValue0(sql(operand1, operand2), expectedType, expectedValue, params);
    }

    private String signatureErorr(SqlColumnType... columnTypes) {
        return signatureErrorFunction("ROUND", columnTypes);
    }

    private static String sql(Object operand1, Object... operand2) {
        assert operand2 == null || operand2.length <= 1;

        if (operand2 != null && operand2.length == 1) {
            return "SELECT ROUND(" + operand1 + ", " + operand2[0] + ") FROM map";
        } else {
            return "SELECT ROUND(" + operand1 + ") FROM map";
        }
    }
}
