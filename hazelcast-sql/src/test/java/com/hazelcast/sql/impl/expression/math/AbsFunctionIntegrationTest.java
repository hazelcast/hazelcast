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
import com.hazelcast.sql.SqlErrorCode;
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
public class AbsFunctionIntegrationTest extends SqlExpressionIntegrationTestSupport {
    @Test
    public void testColumn() {
        checkColumn((byte) 0, SqlColumnType.SMALLINT, (short) 0);
        checkColumn((byte) 1, SqlColumnType.SMALLINT, (short) 1);
        checkColumn((byte) -1, SqlColumnType.SMALLINT, (short) 1);
        checkColumn(Byte.MAX_VALUE, SqlColumnType.SMALLINT, (short) Byte.MAX_VALUE);
        checkColumn(Byte.MIN_VALUE, SqlColumnType.SMALLINT, (short) (Byte.MAX_VALUE + 1));

        checkColumn((short) 0, SqlColumnType.INTEGER, 0);
        checkColumn((short) 1, SqlColumnType.INTEGER, 1);
        checkColumn((short) -1, SqlColumnType.INTEGER, 1);
        checkColumn(Short.MAX_VALUE, SqlColumnType.INTEGER, (int) Short.MAX_VALUE);
        checkColumn(Short.MIN_VALUE, SqlColumnType.INTEGER, Short.MAX_VALUE + 1);

        checkColumn(0, SqlColumnType.BIGINT, (long) 0);
        checkColumn(1, SqlColumnType.BIGINT, (long) 1);
        checkColumn(-1, SqlColumnType.BIGINT, (long) 1);
        checkColumn(Integer.MAX_VALUE, SqlColumnType.BIGINT, (long) Integer.MAX_VALUE);
        checkColumn(Integer.MIN_VALUE, SqlColumnType.BIGINT, (long) Integer.MAX_VALUE + 1);

        checkColumn((long) 0, SqlColumnType.BIGINT, (long) 0);
        checkColumn((long) 1, SqlColumnType.BIGINT, (long) 1);
        checkColumn((long) -1, SqlColumnType.BIGINT, (long) 1);
        checkColumn(Long.MAX_VALUE, SqlColumnType.BIGINT, Long.MAX_VALUE);
        checkColumnFailure(Long.MIN_VALUE, SqlErrorCode.DATA_EXCEPTION, "BIGINT overflow in ABS function (consider adding an explicit CAST to DECIMAL)");

        checkColumn(BigInteger.ZERO, SqlColumnType.DECIMAL, BigDecimal.ZERO);
        checkColumn(BigInteger.ONE, SqlColumnType.DECIMAL, BigDecimal.ONE);
        checkColumn(BigInteger.ONE.negate(), SqlColumnType.DECIMAL, BigDecimal.ONE);

        checkColumn(BigDecimal.ZERO, SqlColumnType.DECIMAL, BigDecimal.ZERO);
        checkColumn(BigDecimal.ONE, SqlColumnType.DECIMAL, BigDecimal.ONE);
        checkColumn(BigDecimal.ONE.negate(), SqlColumnType.DECIMAL, BigDecimal.ONE);

        checkColumn(0.0f, SqlColumnType.REAL, 0.0f);
        checkColumn(-0.0f, SqlColumnType.REAL, 0.0f);
        checkColumn(1.1f, SqlColumnType.REAL, 1.1f);
        checkColumn(-1.1f, SqlColumnType.REAL, 1.1f);
        checkColumn(Float.MAX_VALUE, SqlColumnType.REAL, Float.MAX_VALUE);
        checkColumn(Float.POSITIVE_INFINITY, SqlColumnType.REAL, Float.POSITIVE_INFINITY);
        checkColumn(Float.NEGATIVE_INFINITY, SqlColumnType.REAL, Float.POSITIVE_INFINITY);
        checkColumn(Float.NaN, SqlColumnType.REAL, Float.NaN);

        checkColumn(0.0d, SqlColumnType.DOUBLE, 0.0d);
        checkColumn(-0.0d, SqlColumnType.DOUBLE, 0.0d);
        checkColumn(1.1d, SqlColumnType.DOUBLE, 1.1d);
        checkColumn(-1.1d, SqlColumnType.DOUBLE, 1.1d);
        checkColumn(Double.MAX_VALUE, SqlColumnType.DOUBLE, Double.MAX_VALUE);
        checkColumn(Double.POSITIVE_INFINITY, SqlColumnType.DOUBLE, Double.POSITIVE_INFINITY);
        checkColumn(Double.NEGATIVE_INFINITY, SqlColumnType.DOUBLE, Double.POSITIVE_INFINITY);
        checkColumn(Double.NaN, SqlColumnType.DOUBLE, Double.NaN);

        checkColumn("0", SqlColumnType.DECIMAL, BigDecimal.ZERO);
        checkColumn("1.1", SqlColumnType.DECIMAL, new BigDecimal("1.1"));
        checkColumn("-1.1", SqlColumnType.DECIMAL, new BigDecimal("1.1"));
        checkColumnFailure("a", SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL");
        checkColumnFailure('a', SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL");

        checkColumnFailure(new ExpressionValue.ObjectVal(), SqlErrorCode.PARSING, "Cannot apply 'ABS' to arguments of type 'ABS(<OBJECT>)'");
    }

    private void checkColumn(Object value, SqlColumnType expectedType, Object expectedResult) {
        put(value);

        check("this", expectedType, expectedResult);
    }

    private void checkColumnFailure(Object value, int expectedErrorCode, String expectedErrorMessage) {
        put(value);

        checkFailure("this", expectedErrorCode, expectedErrorMessage);
    }

    @Test
    public void testParameter() {
        put(0);

        checkParameter((byte) 0, BigDecimal.ZERO);
        checkParameter((byte) 1, BigDecimal.ONE);
        checkParameter((byte) -1, BigDecimal.ONE);
        checkParameter(Byte.MAX_VALUE, new BigDecimal(Byte.MAX_VALUE));
        checkParameter(Byte.MIN_VALUE, new BigDecimal(Byte.MIN_VALUE).negate());
        checkParameter(Byte.toString(Byte.MAX_VALUE), new BigDecimal(Byte.MAX_VALUE));
        checkParameter(Byte.toString(Byte.MIN_VALUE), new BigDecimal(Byte.MIN_VALUE).negate());

        checkParameter((short) 0, BigDecimal.ZERO);
        checkParameter((short) 1, BigDecimal.ONE);
        checkParameter((short) -1, BigDecimal.ONE);
        checkParameter(Short.MAX_VALUE, new BigDecimal(Short.MAX_VALUE));
        checkParameter(Short.MIN_VALUE, new BigDecimal(Short.MIN_VALUE).negate());
        checkParameter(Short.toString(Short.MAX_VALUE), new BigDecimal(Short.MAX_VALUE));
        checkParameter(Short.toString(Short.MIN_VALUE), new BigDecimal(Short.MIN_VALUE).negate());

        checkParameter(0, BigDecimal.ZERO);
        checkParameter(1, BigDecimal.ONE);
        checkParameter(-1, BigDecimal.ONE);
        checkParameter(Integer.MAX_VALUE, new BigDecimal(Integer.MAX_VALUE));
        checkParameter(Integer.MIN_VALUE, new BigDecimal(Integer.MIN_VALUE).negate());
        checkParameter(Integer.toString(Integer.MAX_VALUE), new BigDecimal(Integer.MAX_VALUE));
        checkParameter(Integer.toString(Integer.MIN_VALUE), new BigDecimal(Integer.MIN_VALUE).negate());

        checkParameter(0L, BigDecimal.ZERO);
        checkParameter(1L, BigDecimal.ONE);
        checkParameter(-1L, BigDecimal.ONE);
        checkParameter(Long.MAX_VALUE, new BigDecimal(Long.MAX_VALUE));
        checkParameter(Long.MIN_VALUE, new BigDecimal(Long.MIN_VALUE).negate());
        checkParameter(Long.toString(Long.MAX_VALUE), new BigDecimal(Long.MAX_VALUE));
        checkParameter(Long.toString(Long.MIN_VALUE), new BigDecimal(Long.MIN_VALUE).negate());

        checkParameter(BigInteger.ZERO, BigDecimal.ZERO);
        checkParameter(BigInteger.ONE, BigDecimal.ONE);
        checkParameter(BigInteger.ONE.negate(), BigDecimal.ONE);

        checkParameter(BigDecimal.ZERO, BigDecimal.ZERO);
        checkParameter(BigDecimal.ONE, BigDecimal.ONE);
        checkParameter(BigDecimal.ONE.negate(), BigDecimal.ONE);
        checkParameter(new BigDecimal("1.1"), new BigDecimal("1.1"));
        checkParameter(new BigDecimal("-1.1"), new BigDecimal("1.1"));
        checkParameter("1.1", new BigDecimal("1.1"));
        checkParameter("-1.1", new BigDecimal("1.1"));

        checkParameter(0.0f, BigDecimal.ZERO);
        checkParameter(-0.0f, BigDecimal.ZERO);
        checkParameter(1f, BigDecimal.ONE);
        checkParameter(-1f, BigDecimal.ONE);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot convert infinite REAL value to DECIMAL", Float.POSITIVE_INFINITY);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot convert infinite REAL value to DECIMAL", Float.NEGATIVE_INFINITY);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot convert NaN REAL value to DECIMAL", Float.NaN);

        checkParameter(0.0d, BigDecimal.ZERO);
        checkParameter(-0.0d, BigDecimal.ZERO);
        checkParameter(1d, BigDecimal.ONE);
        checkParameter(-1d, BigDecimal.ONE);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot convert infinite DOUBLE value to DECIMAL", Double.POSITIVE_INFINITY);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot convert infinite DOUBLE value to DECIMAL", Double.NEGATIVE_INFINITY);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot convert NaN DOUBLE value to DECIMAL", Double.NaN);

        checkParameter('0', BigDecimal.ZERO);
        checkParameter('1', BigDecimal.ONE);

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to DECIMAL", "bad");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to DECIMAL", 'b');
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from OBJECT to DECIMAL", new ExpressionValue.ObjectVal());
    }

    private void checkParameter(Object parameterValue, Object expectedValue) {
        check("?", SqlColumnType.DECIMAL, expectedValue, parameterValue);
    }

    @Test
    public void testLiteral() {
        put(0);

        checkExactLiteral(1, SqlColumnType.TINYINT, (byte) 1);
        checkExactLiteral(0, SqlColumnType.TINYINT, (byte) 0);
        checkExactLiteral(-1, SqlColumnType.TINYINT, (byte) 1);

        checkExactLiteral(Byte.MAX_VALUE - 1, SqlColumnType.SMALLINT, (short) (Byte.MAX_VALUE - 1));
        checkExactLiteral(Byte.MAX_VALUE, SqlColumnType.SMALLINT, (short) Byte.MAX_VALUE);
        checkExactLiteral(Byte.MIN_VALUE, SqlColumnType.SMALLINT, (short) (Byte.MAX_VALUE + 1));

        checkExactLiteral(Short.MAX_VALUE - 1, SqlColumnType.INTEGER, Short.MAX_VALUE - 1);
        checkExactLiteral(Short.MAX_VALUE, SqlColumnType.INTEGER, (int) Short.MAX_VALUE);
        checkExactLiteral(Short.MIN_VALUE, SqlColumnType.INTEGER, Short.MAX_VALUE + 1);

        checkExactLiteral(Integer.MAX_VALUE - 1, SqlColumnType.BIGINT, (long) (Integer.MAX_VALUE - 1));
        checkExactLiteral(Integer.MAX_VALUE, SqlColumnType.BIGINT, (long) Integer.MAX_VALUE);
        checkExactLiteral(Integer.MIN_VALUE, SqlColumnType.BIGINT, (long) Integer.MAX_VALUE + 1);

        checkExactLiteral(Long.MAX_VALUE - 1, SqlColumnType.BIGINT, Long.MAX_VALUE - 1);
        checkExactLiteral(Long.MAX_VALUE, SqlColumnType.BIGINT, Long.MAX_VALUE);

        check("null", SqlColumnType.DECIMAL, null);
        checkExactLiteral("1.1", SqlColumnType.DECIMAL, new BigDecimal("1.1"));
        checkExactLiteral("0.0", SqlColumnType.DECIMAL, new BigDecimal("0.0"));
        checkExactLiteral("-1.1", SqlColumnType.DECIMAL, new BigDecimal("1.1"));

        checkInexactLiteral("0.0E0", SqlColumnType.DOUBLE, 0.0d);
        checkInexactLiteral("-0.0E0", SqlColumnType.DOUBLE, 0.0d);
        checkInexactLiteral("1.1E0", SqlColumnType.DOUBLE, 1.1d);
        checkInexactLiteral("-1.1E0", SqlColumnType.DOUBLE, 1.1d);

        checkFailure(Long.MIN_VALUE, SqlErrorCode.DATA_EXCEPTION, "BIGINT overflow in ABS function (consider adding an explicit CAST to DECIMAL)");

        checkFailure("'a'", SqlErrorCode.PARSING, "Literal ''a'' can not be parsed to type 'DECIMAL'");
        checkFailure("true", SqlErrorCode.PARSING, "Cannot apply 'ABS' to arguments of type 'ABS(<BOOLEAN>)'");
        checkFailure("false", SqlErrorCode.PARSING, "Cannot apply 'ABS' to arguments of type 'ABS(<BOOLEAN>)'");
    }

    private void checkExactLiteral(Object literal, SqlColumnType expectedType, Object expectedValue) {
        String literalString = literal.toString();

        check(literalString, expectedType, expectedValue);
        check("'" + literalString + "'", SqlColumnType.DECIMAL, new BigDecimal(expectedValue.toString()));
    }

    private void checkInexactLiteral(Object literal, SqlColumnType expectedType, double expectedValue) {
        String literalString = literal.toString();

        check(literalString, expectedType, expectedValue);
    }

    private void check(Object operand, SqlColumnType expectedType, Object expectedValue, Object... params) {
        String sql = "SELECT ABS(" + operand + ") FROM map";

        checkValueInternal(sql, expectedType, expectedValue, params);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT ABS(" + operand + ") FROM map";

        checkFailureInternal(sql, expectedErrorCode, expectedErrorMessage, params);
    }
}
