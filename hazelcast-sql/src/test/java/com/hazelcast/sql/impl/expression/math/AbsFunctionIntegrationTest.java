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
import com.hazelcast.sql.impl.expression.ExpressionTestSupport;
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
public class AbsFunctionIntegrationTest extends ExpressionTestSupport {
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

        checkColumnFailure("0", SqlErrorCode.PARSING, "Cannot apply [VARCHAR] to the 'ABS' function (consider adding an explicit CAST)");
        checkColumnFailure(LOCAL_DATE_VAL, SqlErrorCode.PARSING, "Cannot apply [DATE] to the 'ABS' function (consider adding an explicit CAST)");
        checkColumnFailure(LOCAL_TIME_VAL, SqlErrorCode.PARSING, "Cannot apply [TIME] to the 'ABS' function (consider adding an explicit CAST)");
        checkColumnFailure(LOCAL_DATE_TIME_VAL, SqlErrorCode.PARSING, "Cannot apply [TIMESTAMP] to the 'ABS' function (consider adding an explicit CAST)");
        checkColumnFailure(OFFSET_DATE_TIME_VAL, SqlErrorCode.PARSING, "Cannot apply [TIMESTAMP_WITH_TIME_ZONE] to the 'ABS' function (consider adding an explicit CAST)");
        checkColumnFailure(OBJECT_VAL, SqlErrorCode.PARSING, "Cannot apply [OBJECT] to the 'ABS' function (consider adding an explicit CAST)");
        checkColumnFailure(true, SqlErrorCode.PARSING, "Cannot apply [BOOLEAN] to the 'ABS' function (consider adding an explicit CAST)");
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

        long zero = 0L;
        long one = 1L;

        checkParameter((byte) 0, zero);
        checkParameter((byte) 1, one);
        checkParameter((byte) -1, one);
        checkParameter(Byte.MAX_VALUE, (long) Byte.MAX_VALUE);
        checkParameter(Byte.MIN_VALUE, (long) Byte.MIN_VALUE * -1);

        checkParameter((short) 0, zero);
        checkParameter((short) 1, one);
        checkParameter((short) -1, one);
        checkParameter(Short.MAX_VALUE, (long) Short.MAX_VALUE);
        checkParameter(Short.MIN_VALUE, (long) Short.MIN_VALUE * -1);

        checkParameter(0, zero);
        checkParameter(1, one);
        checkParameter(-1, one);
        checkParameter(Integer.MAX_VALUE, (long) Integer.MAX_VALUE);
        checkParameter(Integer.MIN_VALUE, (long) Integer.MIN_VALUE * -1);

        checkParameter(0L, zero);
        checkParameter(1L, one);
        checkParameter(-1L, one);
        checkParameter(Long.MAX_VALUE, Long.MAX_VALUE);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "BIGINT overflow in ABS function (consider adding an explicit CAST to DECIMAL)", Long.MIN_VALUE);

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from BOOLEAN to BIGINT", true);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from VARCHAR to BIGINT", "0.0");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to BIGINT", BigInteger.ZERO);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to BIGINT", BigDecimal.ZERO);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from REAL to BIGINT", 0.0f);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DOUBLE to BIGINT", 0.0d);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DATE to BIGINT", LOCAL_DATE_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIME to BIGINT", LOCAL_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP to BIGINT", LOCAL_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP_WITH_TIME_ZONE to BIGINT", OFFSET_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from OBJECT to BIGINT", OBJECT_VAL);
    }

    private void checkParameter(Object parameterValue, Object expectedValue) {
        check("?", SqlColumnType.BIGINT, expectedValue, parameterValue);
    }

    @Test
    public void testLiteral() {
        put(0);

        checkLiteral(1, SqlColumnType.TINYINT, (byte) 1);
        checkLiteral(0, SqlColumnType.TINYINT, (byte) 0);
        checkLiteral(-1, SqlColumnType.TINYINT, (byte) 1);

        checkLiteral(Byte.MAX_VALUE - 1, SqlColumnType.SMALLINT, (short) (Byte.MAX_VALUE - 1));
        checkLiteral(Byte.MAX_VALUE, SqlColumnType.SMALLINT, (short) Byte.MAX_VALUE);
        checkLiteral(Byte.MIN_VALUE, SqlColumnType.SMALLINT, (short) (Byte.MAX_VALUE + 1));

        checkLiteral(Short.MAX_VALUE - 1, SqlColumnType.INTEGER, Short.MAX_VALUE - 1);
        checkLiteral(Short.MAX_VALUE, SqlColumnType.INTEGER, (int) Short.MAX_VALUE);
        checkLiteral(Short.MIN_VALUE, SqlColumnType.INTEGER, Short.MAX_VALUE + 1);

        checkLiteral(Integer.MAX_VALUE - 1, SqlColumnType.BIGINT, (long) (Integer.MAX_VALUE - 1));
        checkLiteral(Integer.MAX_VALUE, SqlColumnType.BIGINT, (long) Integer.MAX_VALUE);
        checkLiteral(Integer.MIN_VALUE, SqlColumnType.BIGINT, (long) Integer.MAX_VALUE + 1);

        checkLiteral(Long.MAX_VALUE - 1, SqlColumnType.BIGINT, Long.MAX_VALUE - 1);
        checkLiteral(Long.MAX_VALUE, SqlColumnType.BIGINT, Long.MAX_VALUE);

        check("null", SqlColumnType.BIGINT, null);
        checkLiteral("1.1", SqlColumnType.DECIMAL, new BigDecimal("1.1"));
        checkLiteral("0.0", SqlColumnType.DECIMAL, new BigDecimal("0.0"));
        checkLiteral("-1.1", SqlColumnType.DECIMAL, new BigDecimal("1.1"));

        checkLiteral("0.0E0", SqlColumnType.DOUBLE, 0.0d);
        checkLiteral("-0.0E0", SqlColumnType.DOUBLE, 0.0d);
        checkLiteral("1.1E0", SqlColumnType.DOUBLE, 1.1d);
        checkLiteral("-1.1E0", SqlColumnType.DOUBLE, 1.1d);

        checkFailure(Long.MIN_VALUE, SqlErrorCode.DATA_EXCEPTION, "BIGINT overflow in ABS function (consider adding an explicit CAST to DECIMAL)");

        checkFailure("true", SqlErrorCode.PARSING, "Cannot apply [BOOLEAN] to the 'ABS' function (consider adding an explicit CAST)");
        checkFailure("'a'", SqlErrorCode.PARSING, "Cannot apply [VARCHAR] to the 'ABS' function (consider adding an explicit CAST)");
    }

    private void checkLiteral(Object literal, SqlColumnType expectedType, Object expectedValue) {
        check(literal.toString(), expectedType, expectedValue);
    }

    private void check(Object operand, SqlColumnType expectedType, Object expectedValue, Object... params) {
        String sql = "SELECT ABS(" + operand + ") FROM map";

        checkValue0(sql, expectedType, expectedValue, params);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT ABS(" + operand + ") FROM map";

        checkFailure0(sql, expectedErrorCode, expectedErrorMessage, params);
    }
}
