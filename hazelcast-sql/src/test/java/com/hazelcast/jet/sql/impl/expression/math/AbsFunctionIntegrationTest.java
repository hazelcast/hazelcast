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

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.impl.expression.math.AbsFunction;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;

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
public class AbsFunctionIntegrationTest extends ExpressionTestSupport {
    @Test
    public void testColumn() {
        checkColumn((byte) 0, SMALLINT, (short) 0);
        checkColumn((byte) 1, SMALLINT, (short) 1);
        checkColumn((byte) -1, SMALLINT, (short) 1);
        checkColumn(Byte.MAX_VALUE, SMALLINT, (short) Byte.MAX_VALUE);
        checkColumn(Byte.MIN_VALUE, SMALLINT, (short) (Byte.MAX_VALUE + 1));

        checkColumn((short) 0, INTEGER, 0);
        checkColumn((short) 1, INTEGER, 1);
        checkColumn((short) -1, INTEGER, 1);
        checkColumn(Short.MAX_VALUE, INTEGER, (int) Short.MAX_VALUE);
        checkColumn(Short.MIN_VALUE, INTEGER, Short.MAX_VALUE + 1);

        checkColumn(0, BIGINT, (long) 0);
        checkColumn(1, BIGINT, (long) 1);
        checkColumn(-1, BIGINT, (long) 1);
        checkColumn(Integer.MAX_VALUE, BIGINT, (long) Integer.MAX_VALUE);
        checkColumn(Integer.MIN_VALUE, BIGINT, (long) Integer.MAX_VALUE + 1);

        checkColumn((long) 0, BIGINT, (long) 0);
        checkColumn((long) 1, BIGINT, (long) 1);
        checkColumn((long) -1, BIGINT, (long) 1);
        checkColumn(Long.MAX_VALUE, BIGINT, Long.MAX_VALUE);
        checkColumnFailure(Long.MIN_VALUE, SqlErrorCode.DATA_EXCEPTION, "BIGINT overflow in ABS function (consider adding an explicit CAST to DECIMAL)");

        checkColumn(BigInteger.ZERO, DECIMAL, BigDecimal.ZERO);
        checkColumn(BigInteger.ONE, DECIMAL, BigDecimal.ONE);
        checkColumn(BigInteger.ONE.negate(), DECIMAL, BigDecimal.ONE);

        checkColumn(BigDecimal.ZERO, DECIMAL, BigDecimal.ZERO);
        checkColumn(BigDecimal.ONE, DECIMAL, BigDecimal.ONE);
        checkColumn(BigDecimal.ONE.negate(), DECIMAL, BigDecimal.ONE);

        checkColumn(0.0f, REAL, 0.0f);
        checkColumn(-0.0f, REAL, 0.0f);
        checkColumn(1.1f, REAL, 1.1f);
        checkColumn(-1.1f, REAL, 1.1f);
        checkColumn(Float.MAX_VALUE, REAL, Float.MAX_VALUE);
        checkColumn(Float.POSITIVE_INFINITY, REAL, Float.POSITIVE_INFINITY);
        checkColumn(Float.NEGATIVE_INFINITY, REAL, Float.POSITIVE_INFINITY);
        checkColumn(Float.NaN, REAL, Float.NaN);

        checkColumn(0.0d, DOUBLE, 0.0d);
        checkColumn(-0.0d, DOUBLE, 0.0d);
        checkColumn(1.1d, DOUBLE, 1.1d);
        checkColumn(-1.1d, DOUBLE, 1.1d);
        checkColumn(Double.MAX_VALUE, DOUBLE, Double.MAX_VALUE);
        checkColumn(Double.POSITIVE_INFINITY, DOUBLE, Double.POSITIVE_INFINITY);
        checkColumn(Double.NEGATIVE_INFINITY, DOUBLE, Double.POSITIVE_INFINITY);
        checkColumn(Double.NaN, DOUBLE, Double.NaN);

        checkColumnFailure("0", SqlErrorCode.PARSING, signatureError(VARCHAR));
        checkColumnFailure(true, SqlErrorCode.PARSING, signatureError(BOOLEAN));
        checkColumnFailure(LOCAL_DATE_VAL, SqlErrorCode.PARSING, signatureError(DATE));
        checkColumnFailure(LOCAL_TIME_VAL, SqlErrorCode.PARSING, signatureError(TIME));
        checkColumnFailure(LOCAL_DATE_TIME_VAL, SqlErrorCode.PARSING, signatureError(TIMESTAMP));
        checkColumnFailure(OFFSET_DATE_TIME_VAL, SqlErrorCode.PARSING, signatureError(TIMESTAMP_WITH_TIME_ZONE));
        checkColumnFailure(OBJECT_VAL, SqlErrorCode.PARSING, signatureError(OBJECT));
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

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), "foo");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), true);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BigInteger.ZERO);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BigDecimal.ZERO);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, REAL), 0.0f);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, DOUBLE), 0.0d);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, DATE), LOCAL_DATE_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, TIME), LOCAL_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, OBJECT), OBJECT_VAL);
    }

    private void checkParameter(Object parameterValue, Object expectedValue) {
        check("?", BIGINT, expectedValue, parameterValue);
    }

    @Test
    public void testLiteral() {
        put(0);

        checkLiteral(1, TINYINT, (byte) 1);
        checkLiteral(0, TINYINT, (byte) 0);
        checkLiteral(-1, TINYINT, (byte) 1);

        checkLiteral(Byte.MAX_VALUE - 1, SMALLINT, (short) (Byte.MAX_VALUE - 1));
        checkLiteral(Byte.MAX_VALUE, SMALLINT, (short) Byte.MAX_VALUE);
        checkLiteral(Byte.MIN_VALUE, SMALLINT, (short) (Byte.MAX_VALUE + 1));

        checkLiteral(Short.MAX_VALUE - 1, INTEGER, Short.MAX_VALUE - 1);
        checkLiteral(Short.MAX_VALUE, INTEGER, (int) Short.MAX_VALUE);
        checkLiteral(Short.MIN_VALUE, INTEGER, Short.MAX_VALUE + 1);

        checkLiteral(Integer.MAX_VALUE - 1, BIGINT, (long) (Integer.MAX_VALUE - 1));
        checkLiteral(Integer.MAX_VALUE, BIGINT, (long) Integer.MAX_VALUE);
        checkLiteral(Integer.MIN_VALUE, BIGINT, (long) Integer.MAX_VALUE + 1);

        checkLiteral(Long.MAX_VALUE - 1, BIGINT, Long.MAX_VALUE - 1);
        checkLiteral(Long.MAX_VALUE, BIGINT, Long.MAX_VALUE);

        check("null", BIGINT, null);
        checkLiteral("1.1", DECIMAL, new BigDecimal("1.1"));
        checkLiteral("0.0", DECIMAL, new BigDecimal("0.0"));
        checkLiteral("-1.1", DECIMAL, new BigDecimal("1.1"));

        checkLiteral("0.0E0", DOUBLE, 0.0d);
        checkLiteral("-0.0E0", DOUBLE, 0.0d);
        checkLiteral("1.1E0", DOUBLE, 1.1d);
        checkLiteral("-1.1E0", DOUBLE, 1.1d);

        checkFailure(Long.MIN_VALUE, SqlErrorCode.DATA_EXCEPTION, "BIGINT overflow in ABS function (consider adding an explicit CAST to DECIMAL)");

        checkFailure("'foo'", SqlErrorCode.PARSING, signatureError(VARCHAR));
        checkFailure("true", SqlErrorCode.PARSING, signatureError(BOOLEAN));
    }

    private static String signatureError(SqlColumnType type) {
        return signatureErrorFunction("ABS", type);
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

    @Test
    public void testEquals() {
        AbsFunction<?> function = AbsFunction.create(ConstantExpression.create(1, QueryDataType.INT), QueryDataType.BIGINT);

        checkEquals(function, AbsFunction.create(ConstantExpression.create(1, QueryDataType.INT), QueryDataType.BIGINT), true);
        checkEquals(function, AbsFunction.create(ConstantExpression.create(2, QueryDataType.INT), QueryDataType.BIGINT), false);
        checkEquals(function, AbsFunction.create(ConstantExpression.create(1, QueryDataType.INT), QueryDataType.VARCHAR), false);
    }

    @Test
    public void testSerialization() {
        AbsFunction<?> original = AbsFunction.create(ConstantExpression.create(1, QueryDataType.INT), QueryDataType.BIGINT);
        AbsFunction<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_ABS);

        checkEquals(original, restored, true);
    }
}
