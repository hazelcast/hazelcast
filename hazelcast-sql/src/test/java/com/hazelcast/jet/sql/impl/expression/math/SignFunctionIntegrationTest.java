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
import com.hazelcast.sql.impl.expression.math.SignFunction;
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
import static com.hazelcast.sql.SqlColumnType.OBJECT;
import static com.hazelcast.sql.SqlColumnType.REAL;
import static com.hazelcast.sql.SqlColumnType.TIME;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SignFunctionIntegrationTest extends ExpressionTestSupport {
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

        checkValue("this", expectedType, expectedResult);
    }

    private void checkColumnFailure(Object value, int expectedErrorCode, String expectedErrorMessage) {
        put(value);

        checkFailure("this", expectedErrorCode, expectedErrorMessage);
    }

    @Test
    public void testParameter() {
        put(0);

        long zero = 0L;
        long positive = 1L;
        long negative = -1L;

        checkParameter((byte) 0, zero);
        checkParameter((byte) 1, positive);
        checkParameter((byte) -1, negative);
        checkParameter(Byte.MAX_VALUE, positive);
        checkParameter(Byte.MIN_VALUE, negative);

        checkParameter((short) 0, zero);
        checkParameter((short) 1, positive);
        checkParameter((short) -1, negative);
        checkParameter(Short.MAX_VALUE, positive);
        checkParameter(Short.MIN_VALUE, negative);

        checkParameter(0, zero);
        checkParameter(1, positive);
        checkParameter(-1, negative);
        checkParameter(Integer.MAX_VALUE, positive);
        checkParameter(Integer.MIN_VALUE, negative);

        checkParameter(0L, zero);
        checkParameter(1L, positive);
        checkParameter(-1L, negative);
        checkParameter(Long.MAX_VALUE, positive);
        checkParameter(Long.MIN_VALUE, negative);

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
        checkValue("?", SqlColumnType.BIGINT, expectedValue, parameterValue);
    }

    @Test
    public void testLiteral() {
        put(0);

        checkValue("null", SqlColumnType.BIGINT, null);

        checkValue(0, SqlColumnType.TINYINT, (byte) 0);
        checkValue(1, SqlColumnType.TINYINT, (byte) 1);
        checkValue(-1, SqlColumnType.TINYINT, (byte) -1);
        checkValue(Byte.MAX_VALUE, SqlColumnType.TINYINT, (byte) 1);
        checkValue(Byte.MIN_VALUE, SqlColumnType.TINYINT, (byte) -1);
        checkValue(Short.MAX_VALUE, SqlColumnType.SMALLINT, (short) 1);
        checkValue(Short.MIN_VALUE, SqlColumnType.SMALLINT, (short) -1);
        checkValue(Integer.MAX_VALUE, SqlColumnType.INTEGER, 1);
        checkValue(Integer.MIN_VALUE, SqlColumnType.INTEGER, -1);
        checkValue(Long.MAX_VALUE, SqlColumnType.BIGINT, 1L);
        checkValue(Long.MIN_VALUE, SqlColumnType.BIGINT, -1L);

        checkValue("1.1", SqlColumnType.DECIMAL, new BigDecimal("1"));
        checkValue("0.0", SqlColumnType.DECIMAL, new BigDecimal("0"));
        checkValue("-1.1", SqlColumnType.DECIMAL, new BigDecimal("-1"));

        checkValue("1.1E0", SqlColumnType.DOUBLE, 1.0d);
        checkValue("0.0E0", SqlColumnType.DOUBLE, 0d);
        checkValue("-1.1E0", SqlColumnType.DOUBLE, -1.0d);

        checkFailure("'\fooa'", SqlErrorCode.PARSING, signatureError(VARCHAR));
        checkFailure("true", SqlErrorCode.PARSING, signatureError(BOOLEAN));
    }

    private static String signatureError(SqlColumnType type) {
        return signatureErrorFunction("SIGN", type);
    }

    private void checkValue(Object operand, SqlColumnType expectedType, Object expectedValue, Object... params) {
        String sql = "SELECT SIGN(" + operand + ") FROM map";

        checkValue0(sql, expectedType, expectedValue, params);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT SIGN(" + operand + ") FROM map";

        checkFailure0(sql, expectedErrorCode, expectedErrorMessage, params);
    }

    @Test
    public void testEquals() {
        SignFunction<?> function = SignFunction.create(ConstantExpression.create(1, QueryDataType.INT), QueryDataType.INT);

        checkEquals(function, SignFunction.create(ConstantExpression.create(1, QueryDataType.INT), QueryDataType.INT), true);
        checkEquals(function, SignFunction.create(ConstantExpression.create(2, QueryDataType.INT), QueryDataType.INT), false);
        checkEquals(function, SignFunction.create(ConstantExpression.create(1, QueryDataType.BIGINT), QueryDataType.BIGINT), false);
    }

    @Test
    public void testSerialization() {
        SignFunction<?> original = SignFunction.create(ConstantExpression.create(1, QueryDataType.INT), QueryDataType.INT);
        SignFunction<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_SIGN);

        checkEquals(original, restored, true);
    }
}
