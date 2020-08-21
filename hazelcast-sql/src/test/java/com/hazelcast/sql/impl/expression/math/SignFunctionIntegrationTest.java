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
import com.hazelcast.sql.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SignFunctionIntegrationTest extends SqlExpressionIntegrationTestSupport {
    @Test
    public void testColumn() {
        checkColumn((byte) 0, SqlColumnType.TINYINT, (byte) 0);
        checkColumn((byte) 1, SqlColumnType.TINYINT, (byte) 1);
        checkColumn((byte) -1, SqlColumnType.TINYINT, (byte) -1);
        checkColumn(Byte.MAX_VALUE, SqlColumnType.TINYINT, (byte) 1);
        checkColumn(Byte.MIN_VALUE, SqlColumnType.TINYINT, (byte) -1);

        checkColumn((short) 0, SqlColumnType.SMALLINT, (short) 0);
        checkColumn((short) 1, SqlColumnType.SMALLINT, (short) 1);
        checkColumn((short) -1, SqlColumnType.SMALLINT, (short) -1);
        checkColumn(Short.MAX_VALUE, SqlColumnType.SMALLINT, (short) 1);
        checkColumn(Short.MIN_VALUE, SqlColumnType.SMALLINT, (short) -1);

        checkColumn(0, SqlColumnType.INTEGER, 0);
        checkColumn(1, SqlColumnType.INTEGER, 1);
        checkColumn(-1, SqlColumnType.INTEGER, -1);
        checkColumn(Integer.MAX_VALUE, SqlColumnType.INTEGER, 1);
        checkColumn(Integer.MIN_VALUE, SqlColumnType.INTEGER, -1);

        checkColumn(0L, SqlColumnType.BIGINT, 0L);
        checkColumn(1L, SqlColumnType.BIGINT, 1L);
        checkColumn(-1L, SqlColumnType.BIGINT, -1L);
        checkColumn(Long.MAX_VALUE, SqlColumnType.BIGINT, 1L);
        checkColumn(Long.MIN_VALUE, SqlColumnType.BIGINT, -1L);

        checkColumn(BigInteger.ZERO, SqlColumnType.DECIMAL, BigDecimal.ZERO);
        checkColumn(BigInteger.ONE, SqlColumnType.DECIMAL, BigDecimal.ONE);
        checkColumn(BigInteger.ONE.negate(), SqlColumnType.DECIMAL, BigDecimal.ONE.negate());

        checkColumn(BigDecimal.ZERO, SqlColumnType.DECIMAL, BigDecimal.ZERO);
        checkColumn(BigDecimal.ONE, SqlColumnType.DECIMAL, BigDecimal.ONE);
        checkColumn(BigDecimal.ONE.negate(), SqlColumnType.DECIMAL, BigDecimal.ONE.negate());

        checkColumn(0f, SqlColumnType.REAL, 0f);
        checkColumn(1.1f, SqlColumnType.REAL, 1f);
        checkColumn(-1.1f, SqlColumnType.REAL, -1f);
        checkColumn(Float.POSITIVE_INFINITY, SqlColumnType.REAL, 1f);
        checkColumn(Float.NEGATIVE_INFINITY, SqlColumnType.REAL, -1f);
        checkColumn(Float.NaN, SqlColumnType.REAL, Float.NaN);

        checkColumn(0d, SqlColumnType.DOUBLE, 0d);
        checkColumn(1.1d, SqlColumnType.DOUBLE, 1d);
        checkColumn(-1.1d, SqlColumnType.DOUBLE, -1d);
        checkColumn(Double.POSITIVE_INFINITY, SqlColumnType.DOUBLE, 1d);
        checkColumn(Double.NEGATIVE_INFINITY, SqlColumnType.DOUBLE, -1d);
        checkColumn(Double.NaN, SqlColumnType.DOUBLE, Double.NaN);

        checkColumn("0", SqlColumnType.DECIMAL, BigDecimal.ZERO);
        checkColumn("1.1", SqlColumnType.DECIMAL, new BigDecimal("1"));
        checkColumn("-1.1", SqlColumnType.DECIMAL, new BigDecimal("-1"));
        checkColumnFailure("a", SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL");
        checkColumnFailure('a', SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL");

        checkColumnFailure(new ExpressionValue.ObjectVal(), SqlErrorCode.PARSING, "Cannot apply 'SIGN' to arguments of type 'SIGN(<OBJECT>)'");
    }

    private void checkColumn(Object value, SqlColumnType expectedType, Object expectedResult) {
        put(value);

        checkValue("this", expectedType, expectedResult);
    }

    private void checkColumnFailure(Object value, int expectedErrorCode, String expectedErrorMessage) {
        put(value);

        checkFailure("this", expectedErrorCode, expectedErrorMessage);
    }

    @Test
    public void testParameter() {
        put(0);

        BigDecimal zero = BigDecimal.ZERO;
        BigDecimal positive = BigDecimal.ONE;
        BigDecimal negative = BigDecimal.ONE.negate();

        checkParameter((byte) 0, zero);
        checkParameter((byte) 1, positive);
        checkParameter((byte) -1, negative);
        checkParameter(Byte.MAX_VALUE, positive);
        checkParameter(Byte.MIN_VALUE, negative);
        checkParameter(Byte.toString(Byte.MAX_VALUE), positive);
        checkParameter(Byte.toString(Byte.MIN_VALUE), negative);

        checkParameter((short) 0, zero);
        checkParameter((short) 1, positive);
        checkParameter((short) -1, negative);
        checkParameter(Short.MAX_VALUE, positive);
        checkParameter(Short.MIN_VALUE, negative);
        checkParameter(Short.toString(Short.MAX_VALUE), positive);
        checkParameter(Short.toString(Short.MIN_VALUE), negative);

        checkParameter(0, zero);
        checkParameter(1, positive);
        checkParameter(-1, negative);
        checkParameter(Integer.MAX_VALUE, positive);
        checkParameter(Integer.MIN_VALUE, negative);
        checkParameter(Integer.toString(Integer.MAX_VALUE), positive);
        checkParameter(Integer.toString(Integer.MIN_VALUE), negative);

        checkParameter(0L, zero);
        checkParameter(1L, positive);
        checkParameter(-1L, negative);
        checkParameter(Long.MAX_VALUE, positive);
        checkParameter(Long.MIN_VALUE, negative);
        checkParameter(Long.toString(Long.MAX_VALUE), positive);
        checkParameter(Long.toString(Long.MIN_VALUE), negative);

        checkParameter(BigInteger.ZERO, zero);
        checkParameter(BigInteger.ONE, positive);
        checkParameter(BigInteger.ONE.negate(), negative);

        checkParameter(BigDecimal.ZERO, zero);
        checkParameter(BigDecimal.ONE, positive);
        checkParameter(BigDecimal.ONE.negate(), negative);
        checkParameter(new BigDecimal("1.1"), positive);
        checkParameter(new BigDecimal("-1.1"), negative);
        checkParameter("1.1", positive);
        checkParameter("-1.1", negative);

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from REAL to DECIMAL", 0.0f);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DOUBLE to DECIMAL", 0.0d);

        checkParameter('0', zero);
        checkParameter('1', positive);

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to DECIMAL", "bad");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to DECIMAL", 'b');

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from OBJECT to DECIMAL", new ExpressionValue.ObjectVal());
    }

    private void checkParameter(Object parameterValue, Object expectedValue) {
        checkValue("?", SqlColumnType.DECIMAL, expectedValue, parameterValue);
    }

    @Test
    public void testLiteral() {
        put(0);

        checkExactLiteral(0, SqlColumnType.TINYINT, (byte) 0);
        checkExactLiteral(1, SqlColumnType.TINYINT, (byte) 1);
        checkExactLiteral(-1, SqlColumnType.TINYINT, (byte) -1);
        checkExactLiteral(Byte.MAX_VALUE, SqlColumnType.TINYINT, (byte) 1);
        checkExactLiteral(Byte.MIN_VALUE, SqlColumnType.TINYINT, (byte) -1);
        checkExactLiteral(Short.MAX_VALUE, SqlColumnType.SMALLINT, (short) 1);
        checkExactLiteral(Short.MIN_VALUE, SqlColumnType.SMALLINT, (short) -1);
        checkExactLiteral(Integer.MAX_VALUE, SqlColumnType.INTEGER, 1);
        checkExactLiteral(Integer.MIN_VALUE, SqlColumnType.INTEGER, -1);
        checkExactLiteral(Long.MAX_VALUE, SqlColumnType.BIGINT, 1L);
        checkExactLiteral(Long.MIN_VALUE, SqlColumnType.BIGINT, -1L);

        checkValue("null", SqlColumnType.DECIMAL, null);
        checkExactLiteral("1.1", SqlColumnType.DECIMAL, new BigDecimal("1"));
        checkExactLiteral("0.0", SqlColumnType.DECIMAL, new BigDecimal("0"));
        checkExactLiteral("-1.1", SqlColumnType.DECIMAL, new BigDecimal("-1"));

        checkInexactLiteral("1.1E0", SqlColumnType.DOUBLE, 1.0d);
        checkInexactLiteral("0.0E0", SqlColumnType.DOUBLE, 0d);
        checkInexactLiteral("-1.1E0", SqlColumnType.DOUBLE, -1.0d);

        checkFailure("'a'", SqlErrorCode.PARSING, "Literal ''a'' can not be parsed to type 'DECIMAL'");
        checkFailure("true", SqlErrorCode.PARSING, "Cannot apply 'SIGN' to arguments of type 'SIGN(<BOOLEAN>)'");
        checkFailure("false", SqlErrorCode.PARSING, "Cannot apply 'SIGN' to arguments of type 'SIGN(<BOOLEAN>)'");
    }

    private void checkExactLiteral(Object literal, SqlColumnType expectedType, Object expectedValue) {
        String literalString = literal.toString();

        checkValue(literalString, expectedType, expectedValue);
        checkValue("'" + literalString + "'", SqlColumnType.DECIMAL, new BigDecimal(expectedValue.toString()));
    }

    private void checkInexactLiteral(Object literal, SqlColumnType expectedType, double expectedValue) {
        String literalString = literal.toString();

        checkValue(literalString, expectedType, expectedValue);
    }

    private void checkValue(Object operand, SqlColumnType expectedType, Object expectedValue, Object... params) {
        String sql = "SELECT SIGN(" + operand + ") FROM map";

        checkValueInternal(sql, expectedType, expectedValue, params);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT SIGN(" + operand + ") FROM map";

        checkFailureInternal(sql, expectedErrorCode, expectedErrorMessage, params);
    }
}
