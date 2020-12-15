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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.SqlExpressionIntegrationTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionBiValue;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.ByteIntegerVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.IntegerIntegerVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.IntegerObjectVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.LongIntegerVal;
import com.hazelcast.sql.support.expressions.ExpressionBiValue.ShortIntegerVal;
import com.hazelcast.sql.support.expressions.ExpressionValue;
import com.hazelcast.sql.support.expressions.ExpressionValue.BigIntegerVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.ByteVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.IntegerVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.LongVal;
import com.hazelcast.sql.support.expressions.ExpressionValue.ShortVal;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.hazelcast.sql.support.expressions.ExpressionBiValue.BigDecimalIntegerVal;
import static com.hazelcast.sql.support.expressions.ExpressionBiValue.BigIntegerIntegerVal;
import static com.hazelcast.sql.support.expressions.ExpressionBiValue.DoubleIntegerVal;
import static com.hazelcast.sql.support.expressions.ExpressionBiValue.FloatIntegerVal;
import static com.hazelcast.sql.support.expressions.ExpressionValue.BigDecimalVal;
import static com.hazelcast.sql.support.expressions.ExpressionValue.DoubleVal;
import static com.hazelcast.sql.support.expressions.ExpressionValue.FloatVal;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TruncateFunctionIntegrationTest extends SqlExpressionIntegrationTestSupport {
    @Test
    public void testLengthOverflow() {
        put(new ExpressionBiValue.IntegerLongVal().fields(1, Long.MAX_VALUE));
        checkFailure_2("field1", "field2", SqlErrorCode.DATA_EXCEPTION, "Cannot convert the second operand of TRUNCATE function to INT");
    }

    @Test
    public void test_byte() {
        checkColumn_1(new ByteVal().field1((byte) 127), SqlColumnType.TINYINT, (byte) 127);
        checkColumn_1(new ByteVal().field1((byte) -128), SqlColumnType.TINYINT, (byte) -128);

        checkColumn_2(new ByteIntegerVal().fields((byte) 127, 1), SqlColumnType.TINYINT, (byte) 127);
        checkColumn_2(new ByteIntegerVal().fields((byte) 127, 0), SqlColumnType.TINYINT, (byte) 127);
        checkColumn_2(new ByteIntegerVal().fields((byte) 127, -1), SqlColumnType.TINYINT, (byte) 120);
        checkColumn_2(new ByteIntegerVal().fields((byte) 127, -2), SqlColumnType.TINYINT, (byte) 100);
        checkColumn_2(new ByteIntegerVal().fields((byte) 127, -3), SqlColumnType.TINYINT, (byte) 0);
        checkColumn_2(new ByteIntegerVal().fields((byte) 127, -4), SqlColumnType.TINYINT, (byte) 0);

        checkColumn_2(new ByteIntegerVal().fields((byte) -128, 1), SqlColumnType.TINYINT, (byte) -128);
        checkColumn_2(new ByteIntegerVal().fields((byte) -128, 0), SqlColumnType.TINYINT, (byte) -128);
        checkColumn_2(new ByteIntegerVal().fields((byte) -128, -1), SqlColumnType.TINYINT, (byte) -120);
        checkColumn_2(new ByteIntegerVal().fields((byte) -128, -2), SqlColumnType.TINYINT, (byte) -100);
        checkColumn_2(new ByteIntegerVal().fields((byte) -128, -3), SqlColumnType.TINYINT, (byte) 0);
        checkColumn_2(new ByteIntegerVal().fields((byte) -128, -4), SqlColumnType.TINYINT, (byte) 0);
    }

    @Test
    public void test_short() {
        checkColumn_1(new ShortVal().field1((short) 32767), SqlColumnType.SMALLINT, (short) 32767);
        checkColumn_1(new ShortVal().field1((short) -32768), SqlColumnType.SMALLINT, (short) -32768);

        checkColumn_2(new ShortIntegerVal().fields((short) 32767, 1), SqlColumnType.SMALLINT, (short) 32767);
        checkColumn_2(new ShortIntegerVal().fields((short) 32767, 0), SqlColumnType.SMALLINT, (short) 32767);
        checkColumn_2(new ShortIntegerVal().fields((short) 32767, -1), SqlColumnType.SMALLINT, (short) 32760);
        checkColumn_2(new ShortIntegerVal().fields((short) 32767, -5), SqlColumnType.SMALLINT, (short) 0);
        checkColumn_2(new ShortIntegerVal().fields((short) 32767, -6), SqlColumnType.SMALLINT, (short) 0);

        checkColumn_2(new ShortIntegerVal().fields((short) -32768, 1), SqlColumnType.SMALLINT, (short) -32768);
        checkColumn_2(new ShortIntegerVal().fields((short) -32768, 0), SqlColumnType.SMALLINT, (short) -32768);
        checkColumn_2(new ShortIntegerVal().fields((short) -32768, -1), SqlColumnType.SMALLINT, (short) -32760);
        checkColumn_2(new ShortIntegerVal().fields((short) -32768, -5), SqlColumnType.SMALLINT, (short) 0);
        checkColumn_2(new ShortIntegerVal().fields((short) -32768, -6), SqlColumnType.SMALLINT, (short) 0);
    }

    @Test
    public void test_int() {
        checkColumn_1(new IntegerVal().field1(2_147_483_647), SqlColumnType.INTEGER, 2_147_483_647);
        checkColumn_1(new IntegerVal().field1(-2_147_483_648), SqlColumnType.INTEGER, -2_147_483_648);

        checkColumn_2(new IntegerIntegerVal().fields(2_147_483_647, 1), SqlColumnType.INTEGER, 2_147_483_647);
        checkColumn_2(new IntegerIntegerVal().fields(2_147_483_647, 0), SqlColumnType.INTEGER, 2_147_483_647);
        checkColumn_2(new IntegerIntegerVal().fields(2_147_483_647, -1), SqlColumnType.INTEGER, 2_147_483_640);
        checkColumn_2(new IntegerIntegerVal().fields(2_147_483_647, -10), SqlColumnType.INTEGER, 0);
        checkColumn_2(new IntegerIntegerVal().fields(2_147_483_647, -11), SqlColumnType.INTEGER, 0);

        checkColumn_2(new IntegerIntegerVal().fields(-2_147_483_648, 1), SqlColumnType.INTEGER, -2_147_483_648);
        checkColumn_2(new IntegerIntegerVal().fields(-2_147_483_648, 0), SqlColumnType.INTEGER, -2_147_483_648);
        checkColumn_2(new IntegerIntegerVal().fields(-2_147_483_648, -1), SqlColumnType.INTEGER, -2_147_483_640);
        checkColumn_2(new IntegerIntegerVal().fields(2_147_483_647, -10), SqlColumnType.INTEGER, 0);
        checkColumn_2(new IntegerIntegerVal().fields(2_147_483_647, -11), SqlColumnType.INTEGER, 0);
    }

    @Test
    public void test_long() {
        checkColumn_1(new LongVal().field1(9_223_372_036_854_775_807L), SqlColumnType.BIGINT, 9_223_372_036_854_775_807L);
        checkColumn_1(new LongVal().field1(-9_223_372_036_854_775_808L), SqlColumnType.BIGINT, -9_223_372_036_854_775_808L);

        checkColumn_2(new LongIntegerVal().fields(9_223_372_036_854_775_807L, 1), SqlColumnType.BIGINT, 9_223_372_036_854_775_807L);
        checkColumn_2(new LongIntegerVal().fields(9_223_372_036_854_775_807L, 0), SqlColumnType.BIGINT, 9_223_372_036_854_775_807L);
        checkColumn_2(new LongIntegerVal().fields(9_223_372_036_854_775_807L, -1), SqlColumnType.BIGINT, 9_223_372_036_854_775_800L);
        checkColumn_2(new LongIntegerVal().fields(9_223_372_036_854_775_807L, -19), SqlColumnType.BIGINT, 0L);
        checkColumn_2(new LongIntegerVal().fields(9_223_372_036_854_775_807L, -20), SqlColumnType.BIGINT, 0L);

        checkColumn_2(new LongIntegerVal().fields(-9_223_372_036_854_775_808L, 1), SqlColumnType.BIGINT, -9_223_372_036_854_775_808L);
        checkColumn_2(new LongIntegerVal().fields(-9_223_372_036_854_775_808L, 0), SqlColumnType.BIGINT, -9_223_372_036_854_775_808L);
        checkColumn_2(new LongIntegerVal().fields(-9_223_372_036_854_775_808L, -1), SqlColumnType.BIGINT, -9_223_372_036_854_775_800L);
        checkColumn_2(new LongIntegerVal().fields(-9_223_372_036_854_775_808L, -19), SqlColumnType.BIGINT, 0L);
        checkColumn_2(new LongIntegerVal().fields(-9_223_372_036_854_775_808L, -20), SqlColumnType.BIGINT, 0L);
    }

    @Test
    public void test_BigInteger() {
        checkColumn_1(new BigIntegerVal().field1(new BigInteger("15")), SqlColumnType.DECIMAL, new BigDecimal("15"));
        checkColumn_1(new BigIntegerVal().field1(new BigInteger("-15")), SqlColumnType.DECIMAL, new BigDecimal("-15"));

        checkColumn_2(new BigIntegerIntegerVal().fields(new BigInteger("15"), -1), SqlColumnType.DECIMAL, new BigDecimal("10"));
        checkColumn_2(new BigIntegerIntegerVal().fields(new BigInteger("15"), -2), SqlColumnType.DECIMAL, new BigDecimal("0"));

        checkColumn_2(new BigIntegerIntegerVal().fields(new BigInteger("-15"), -1), SqlColumnType.DECIMAL, new BigDecimal("-10"));
        checkColumn_2(new BigIntegerIntegerVal().fields(new BigInteger("-15"), -2), SqlColumnType.DECIMAL, new BigDecimal("0"));
    }

    @Test
    public void test_BigDecimal() {
        checkColumn_1(new BigDecimalVal().field1(new BigDecimal("15.4")), SqlColumnType.DECIMAL, new BigDecimal("15"));
        checkColumn_1(new BigDecimalVal().field1(new BigDecimal("15.5")), SqlColumnType.DECIMAL, new BigDecimal("15"));
        checkColumn_1(new BigDecimalVal().field1(new BigDecimal("-15.4")), SqlColumnType.DECIMAL, new BigDecimal("-15"));
        checkColumn_1(new BigDecimalVal().field1(new BigDecimal("-15.5")), SqlColumnType.DECIMAL, new BigDecimal("-15"));

        checkColumn_2(new BigDecimalIntegerVal().fields(new BigDecimal("15.5"), -1), SqlColumnType.DECIMAL, new BigDecimal("10"));
        checkColumn_2(new BigDecimalIntegerVal().fields(new BigDecimal("15.5"), -2), SqlColumnType.DECIMAL, new BigDecimal("0"));

        checkColumn_2(new BigDecimalIntegerVal().fields(new BigDecimal("-15.5"), -1), SqlColumnType.DECIMAL, new BigDecimal("-10"));
        checkColumn_2(new BigDecimalIntegerVal().fields(new BigDecimal("-15.5"), -2), SqlColumnType.DECIMAL, new BigDecimal("0"));
    }

    @Test
    public void test_float() {
        checkColumn_1(new FloatVal().field1(15.4f), SqlColumnType.REAL, 15f);
        checkColumn_1(new FloatVal().field1(15.5f), SqlColumnType.REAL, 15f);
        checkColumn_1(new FloatVal().field1(-15.4f), SqlColumnType.REAL, -15f);
        checkColumn_1(new FloatVal().field1(-15.5f), SqlColumnType.REAL, -15f);

        checkColumn_2(new FloatIntegerVal().fields(15.5f, -1), SqlColumnType.REAL, 10f);
        checkColumn_2(new FloatIntegerVal().fields(15.5f, -2), SqlColumnType.REAL, 0f);

        checkColumn_2(new FloatIntegerVal().fields(-15.5f, -1), SqlColumnType.REAL, -10f);
        checkColumn_2(new FloatIntegerVal().fields(-15.5f, -2), SqlColumnType.REAL, 0f);

        checkColumn_2(new FloatIntegerVal().fields(Float.POSITIVE_INFINITY, -1), SqlColumnType.REAL, Float.POSITIVE_INFINITY);
        checkColumn_2(new FloatIntegerVal().fields(Float.NEGATIVE_INFINITY, -1), SqlColumnType.REAL, Float.NEGATIVE_INFINITY);
        checkColumn_2(new FloatIntegerVal().fields(Float.NaN, -1), SqlColumnType.REAL, Float.NaN);
    }

    @Test
    public void test_double() {
        checkColumn_1(new DoubleVal().field1(15.4d), SqlColumnType.DOUBLE, 15d);
        checkColumn_1(new DoubleVal().field1(15.5d), SqlColumnType.DOUBLE, 15d);
        checkColumn_1(new DoubleVal().field1(-15.4d), SqlColumnType.DOUBLE, -15d);
        checkColumn_1(new DoubleVal().field1(-15.5d), SqlColumnType.DOUBLE, -15d);

        checkColumn_2(new DoubleIntegerVal().fields(15.5d, -1), SqlColumnType.DOUBLE, 10d);
        checkColumn_2(new DoubleIntegerVal().fields(15.5d, -2), SqlColumnType.DOUBLE, 0d);

        checkColumn_2(new DoubleIntegerVal().fields(-15.5d, -1), SqlColumnType.DOUBLE, -10d);
        checkColumn_2(new DoubleIntegerVal().fields(-15.5d, -2), SqlColumnType.DOUBLE, 0d);

        checkColumn_2(new DoubleIntegerVal().fields(Double.POSITIVE_INFINITY, -1), SqlColumnType.DOUBLE, Double.POSITIVE_INFINITY);
        checkColumn_2(new DoubleIntegerVal().fields(Double.NEGATIVE_INFINITY, -1), SqlColumnType.DOUBLE, Double.NEGATIVE_INFINITY);
        checkColumn_2(new DoubleIntegerVal().fields(Double.NaN, -1), SqlColumnType.DOUBLE, Double.NaN);
    }

    @Test
    public void testParameters() {
        // One operand
        put(new IntegerVal().field1(0));
        check_1("?", SqlColumnType.DECIMAL, new BigDecimal("10"), (byte) 10);
        check_1("?", SqlColumnType.DECIMAL, new BigDecimal("10"), (short) 10);
        check_1("?", SqlColumnType.DECIMAL, new BigDecimal("10"), 10);
        check_1("?", SqlColumnType.DECIMAL, new BigDecimal("10"), 10L);
        check_1("?", SqlColumnType.DECIMAL, new BigDecimal("10"), new BigInteger("10"));
        check_1("?", SqlColumnType.DECIMAL, new BigDecimal("10"), new BigDecimal("10.5"));
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from REAL to DECIMAL", 10.5f);
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DOUBLE to DECIMAL", 10.5d);
        check_1("?", SqlColumnType.DECIMAL, new BigDecimal("10"), "10.5");
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL", "bad");
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DOUBLE to DECIMAL", 10.5d);
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DATE to DECIMAL", LOCAL_DATE_VAL);
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIME to DECIMAL", LOCAL_TIME_VAL);
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP to DECIMAL", LOCAL_DATE_TIME_VAL);
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP_WITH_TIME_ZONE to DECIMAL", OFFSET_DATE_TIME_VAL);
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from OBJECT to DECIMAL", new ExpressionValue.ObjectVal());
        checkFailure_1("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from BOOLEAN to DECIMAL", true);

        // Two operands, first operand
        check_2("?", "0", SqlColumnType.DECIMAL, new BigDecimal("10"), (byte) 10);
        check_2("?", "0", SqlColumnType.DECIMAL, new BigDecimal("10"), (short) 10);
        check_2("?", "0", SqlColumnType.DECIMAL, new BigDecimal("10"), 10);
        check_2("?", "0", SqlColumnType.DECIMAL, new BigDecimal("10"), 10L);
        check_2("?", "0", SqlColumnType.DECIMAL, new BigDecimal("10"), new BigInteger("10"));
        check_2("?", "0", SqlColumnType.DECIMAL, new BigDecimal("10"), new BigDecimal("10.5"));
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from REAL to DECIMAL", 10.5f);
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DOUBLE to DECIMAL", 10.5d);
        check_2("?", "0", SqlColumnType.DECIMAL, new BigDecimal("10"), "10.5");
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL", "bad");
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DATE to DECIMAL", LOCAL_DATE_VAL);
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIME to DECIMAL", LOCAL_TIME_VAL);
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP to DECIMAL", LOCAL_DATE_TIME_VAL);
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP_WITH_TIME_ZONE to DECIMAL", OFFSET_DATE_TIME_VAL);
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from OBJECT to DECIMAL", new ExpressionValue.ObjectVal());
        checkFailure_2("?", "0", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from BOOLEAN to DECIMAL", true);

        // Two operands, second operand
        check_2("15", "?", SqlColumnType.TINYINT, (byte) 10, (byte) -1);
        check_2("15", "?", SqlColumnType.TINYINT, (byte) 10, (short) -1);
        check_2("15", "?", SqlColumnType.TINYINT, (byte) 10, -1);
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from BIGINT to INTEGER", 1L);
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to INTEGER", BigInteger.ONE.negate());
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to INTEGER", BigDecimal.ONE.negate());
        check_2("15", "?", SqlColumnType.TINYINT, (byte) 10, "-1");
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to INT", "bad");
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DATE to INTEGER", LOCAL_DATE_VAL);
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIME to INTEGER", LOCAL_TIME_VAL);
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP to INTEGER", LOCAL_DATE_TIME_VAL);
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP_WITH_TIME_ZONE to INTEGER", OFFSET_DATE_TIME_VAL);
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from OBJECT to INTEGER", new ExpressionValue.ObjectVal());
        checkFailure_2("15", "?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from BOOLEAN to INTEGER", true);

        // Two operands, both
        check_2("?", "?", SqlColumnType.DECIMAL, new BigDecimal("10"), 15, -1);
    }

    @Test
    public void test_boolean() {
        checkColumnFailure_2(new ExpressionBiValue.BooleanIntegerVal().fields(true, 127), SqlErrorCode.PARSING, "Cannot apply 'TRUNCATE' to arguments of type 'TRUNCATE(<BOOLEAN>, <INTEGER>)'");
        checkColumnFailure_2(new ExpressionBiValue.IntegerBooleanVal().fields(127, true), SqlErrorCode.PARSING, "Cannot apply 'TRUNCATE' to arguments of type 'TRUNCATE(<INTEGER>, <BOOLEAN>)'");
    }

    @Test
    public void test_temporal() {
        checkColumnFailure_2(new ExpressionBiValue.IntegerLocalDateVal().fields(127, LOCAL_DATE_VAL), SqlErrorCode.PARSING, "Cannot apply 'TRUNCATE' to arguments of type 'TRUNCATE(<INTEGER>, <DATE>)'");
        checkColumnFailure_2(new ExpressionBiValue.IntegerLocalTimeVal().fields(127, LOCAL_TIME_VAL), SqlErrorCode.PARSING, "Cannot apply 'TRUNCATE' to arguments of type 'TRUNCATE(<INTEGER>, <TIME>)'");
        checkColumnFailure_2(new ExpressionBiValue.IntegerLocalDateTimeVal().fields(127, LOCAL_DATE_TIME_VAL), SqlErrorCode.PARSING, "Cannot apply 'TRUNCATE' to arguments of type 'TRUNCATE(<INTEGER>, <TIMESTAMP>)'");
        checkColumnFailure_2(new ExpressionBiValue.IntegerOffsetDateTimeVal().fields(127, OFFSET_DATE_TIME_VAL), SqlErrorCode.PARSING, "Cannot apply 'TRUNCATE' to arguments of type 'TRUNCATE(<INTEGER>, <TIMESTAMP_WITH_TIME_ZONE>)'");
    }

    @Test
    public void test_object() {
        checkColumnFailure_2(new IntegerObjectVal().fields(127, "bad"), SqlErrorCode.PARSING, "Cannot apply 'TRUNCATE' to arguments of type 'TRUNCATE(<INTEGER>, <OBJECT>)'");
    }

    @Test
    public void testLiterals() {
        // Single operand
        put(new IntegerVal().field1(0));

        check_1("null", SqlColumnType.DECIMAL, null);

        check_1("15.1", SqlColumnType.DECIMAL, new BigDecimal("15"));
        check_1("15.5", SqlColumnType.DECIMAL, new BigDecimal("15"));
        check_1("-15.1", SqlColumnType.DECIMAL, new BigDecimal("-15"));
        check_1("-15.5", SqlColumnType.DECIMAL, new BigDecimal("-15"));

        check_1("15.1E0", SqlColumnType.DOUBLE, 15d);
        check_1("15.5E0", SqlColumnType.DOUBLE, 15d);
        check_1("-15.1E0", SqlColumnType.DOUBLE, -15d);
        check_1("-15.5E0", SqlColumnType.DOUBLE, -15d);

        // First operand
        put(new IntegerVal().field1(-1));
        check_2("15", "field1", SqlColumnType.TINYINT, (byte) 10);
        check_2("'15'", "field1", SqlColumnType.DECIMAL, new BigDecimal("10"));
        check_2("15.1", "field1", SqlColumnType.DECIMAL, new BigDecimal("10"));
        check_2("'15.1'", "field1", SqlColumnType.DECIMAL, new BigDecimal("10"));
        checkFailure_2("'bad'", "field1", SqlErrorCode.PARSING, "Literal ''bad'' can not be parsed to type 'DECIMAL'");
        checkFailure_2("true", "field1", SqlErrorCode.PARSING, "Cannot apply 'TRUNCATE' to arguments of type 'TRUNCATE(<BOOLEAN>, <INTEGER>)'");

        // Second operand
        put(new IntegerVal().field1(15));
        check_2("field1", "-1", SqlColumnType.INTEGER, 10);
        check_2("field1", "'-1'", SqlColumnType.INTEGER, 10);
        checkFailure_2("field1", "'bad'", SqlErrorCode.PARSING, "Literal ''bad'' can not be parsed to type 'DECIMAL'");
        checkFailure_2("field1", "true", SqlErrorCode.PARSING, "Cannot apply 'TRUNCATE' to arguments of type 'TRUNCATE(<INTEGER>, <BOOLEAN>)'");
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

        checkFailureInternal(sql, expectedErrorCode, expectedErrorMessage);
    }

    private void checkFailure_1(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = sql(operand);

        checkFailureInternal(sql, expectedErrorCode, expectedErrorMessage, params);
    }

    private void checkFailure_2(Object operand1, Object operand2, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = sql(operand1, operand2);

        checkFailureInternal(sql, expectedErrorCode, expectedErrorMessage, params);
    }

    private void check_1(Object operand, SqlColumnType expectedType, Object expectedValue, Object... params) {
        checkValueInternal(sql(operand), expectedType, expectedValue, params);
    }

    private void check_2(Object operand1, Object operand2, SqlColumnType expectedType, Object expectedValue, Object... params) {
        checkValueInternal(sql(operand1, operand2), expectedType, expectedValue, params);
    }

    private static String sql(Object operand1, Object... operand2) {
        assert operand2 == null || operand2.length <= 1;

        if (operand2 != null && operand2.length == 1) {
            return "SELECT TRUNCATE(" + operand1 + ", " + operand2[0] + ") FROM map";
        } else {
            return "SELECT TRUNCATE(" + operand1 + ") FROM map";
        }
    }
}
