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
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.impl.expression.math.FloorCeilFunction;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;

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
import static com.hazelcast.sql.impl.type.QueryDataType.DECIMAL_BIG_INTEGER;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FloorCeilFunctionIntegrationTest extends ExpressionTestSupport {
    @Parameterized.Parameter
    public boolean floor;

    @Parameterized.Parameters(name = "mode: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {true},
                {false}
        });
    }

    @Test
    public void testColumn() {
        checkColumn((byte) 1, TINYINT, (byte) 1);
        checkColumn((short) 1, SMALLINT, (short) 1);
        checkColumn(1, INTEGER, 1);
        checkColumn(1L, BIGINT, 1L);
        checkColumn(1.1f, REAL, floorCeil(1f, 2f));
        checkColumn(1.1d, DOUBLE, floorCeil(1d, 2d));
        checkColumn(BigInteger.ONE, DECIMAL, BigDecimal.ONE);
        checkColumn(new BigDecimal("1.1"), DECIMAL, floorCeil(BigDecimal.ONE, new BigDecimal("2")));

        checkColumn(Float.POSITIVE_INFINITY, REAL, Float.POSITIVE_INFINITY);
        checkColumn(Float.NEGATIVE_INFINITY, REAL, Float.NEGATIVE_INFINITY);
        checkColumn(Float.NaN, REAL, Float.NaN);

        checkColumn(Double.POSITIVE_INFINITY, DOUBLE, Double.POSITIVE_INFINITY);
        checkColumn(Double.NEGATIVE_INFINITY, DOUBLE, Double.NEGATIVE_INFINITY);
        checkColumn(Double.NaN, DOUBLE, Double.NaN);

        put(new ExpressionValue.IntegerVal());
        checkValue("field1", INTEGER, null);

        checkColumnFailure("bad", SqlErrorCode.PARSING, signatureError(VARCHAR));
        checkColumnFailure('b', SqlErrorCode.PARSING, signatureError(VARCHAR));
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

        checkParameter((byte) 1, BigDecimal.ONE);
        checkParameter((short) 1, BigDecimal.ONE);
        checkParameter(1, BigDecimal.ONE);
        checkParameter(1L, BigDecimal.ONE);
        checkParameter(BigInteger.ONE, BigDecimal.ONE);
        checkParameter(new BigDecimal("1.1"), floorCeil(BigDecimal.ONE, new BigDecimal("2")));

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, REAL), 0.0f);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, DOUBLE), 0.0d);

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), "1.1");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, VARCHAR), '1');

        checkValue("?", DECIMAL, null, new Object[]{null});

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, BOOLEAN), true);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, DATE), LOCAL_DATE_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, TIME), LOCAL_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, parameterError(0, DECIMAL, OBJECT), OBJECT_VAL);
    }

    private Object floorCeil(Object floorVal, Object ceilVal) {
        return floor ? floorVal : ceilVal;
    }

    private void checkParameter(Object param, Object expectedResult) {
        checkValue("?", DECIMAL, expectedResult, param);
    }

    @Test
    public void testLiteral() {
        put(0);

        checkLiteral(1, TINYINT, (byte) 1);

        checkLiteral("null", DECIMAL, null);
        checkLiteral("1.1", DECIMAL, floorCeil(new BigDecimal("1"), new BigDecimal("2")));
        checkLiteral("1.1E0", DOUBLE, floorCeil(1d, 2d));

        checkFailure("'foo'", SqlErrorCode.PARSING, signatureError(VARCHAR));
        checkFailure("true", SqlErrorCode.PARSING, signatureError(BOOLEAN));
    }

    private String signatureError(SqlColumnType type) {
        return signatureErrorFunction(name(), type);
    }

    private void checkLiteral(Object literal, SqlColumnType expectedType, Object expectedResult) {
        String literalString = literal.toString();

        checkValue(literalString, expectedType, expectedResult);
    }

    private void checkValue(Object operand, SqlColumnType expectedType, Object expectedValue, Object... params) {
        String sql = "SELECT " + name() + "(" + operand + ") FROM map";

        checkValue0(sql, expectedType, expectedValue, params);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT " + name() + "(" + operand + ") FROM map";

        checkFailure0(sql, expectedErrorCode, expectedErrorMessage, params);
    }

    private String name() {
        return floor ? "FLOOR" : "CEIL";
    }

    @Test
    public void testEquals() {
        Expression<?> function = FloorCeilFunction.create(ConstantExpression.create(1, QueryDataType.DECIMAL), QueryDataType.DECIMAL, true);

        checkEquals(function, FloorCeilFunction.create(ConstantExpression.create(1, QueryDataType.DECIMAL), QueryDataType.DECIMAL, true), true);
        checkEquals(function, FloorCeilFunction.create(ConstantExpression.create(2, QueryDataType.DECIMAL), QueryDataType.DECIMAL, true), false);
        checkEquals(function, FloorCeilFunction.create(ConstantExpression.create(1, QueryDataType.DECIMAL), QueryDataType.DECIMAL, false), false);
    }

    @Test
    public void testSerialization() {
        Expression<?> original = FloorCeilFunction.create(ConstantExpression.create(1, QueryDataType.DECIMAL), QueryDataType.DECIMAL, true);
        Expression<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_FLOOR_CEIL);

        checkEquals(original, restored, true);
    }

    @Test
    public void testSimplification() {
        checkSimplified(FloorCeilFunction.create(ConstantExpression.create(null, QueryDataType.TINYINT), QueryDataType.TINYINT, true));
        checkSimplified(FloorCeilFunction.create(ConstantExpression.create(null, QueryDataType.SMALLINT), QueryDataType.SMALLINT, true));
        checkSimplified(FloorCeilFunction.create(ConstantExpression.create(null, INT), INT, true));
        checkSimplified(FloorCeilFunction.create(ConstantExpression.create(null, QueryDataType.BIGINT), QueryDataType.BIGINT, true));

        checkNotSimplified(FloorCeilFunction.create(ConstantExpression.create(null, DECIMAL_BIG_INTEGER), DECIMAL_BIG_INTEGER, true));
        checkNotSimplified(FloorCeilFunction.create(ConstantExpression.create(null, QueryDataType.DECIMAL), QueryDataType.DECIMAL, true));

        checkNotSimplified(FloorCeilFunction.create(ConstantExpression.create(null, QueryDataType.REAL), QueryDataType.REAL, true));
        checkNotSimplified(FloorCeilFunction.create(ConstantExpression.create(null, QueryDataType.DOUBLE), QueryDataType.DOUBLE, true));

        checkNotSimplified(FloorCeilFunction.create(ConstantExpression.create(null, QueryDataType.TINYINT), QueryDataType.DECIMAL, true));
    }

    private void checkNotSimplified(Expression<?> expression) {
        assertEquals(FloorCeilFunction.class, expression.getClass());
    }

    private void checkSimplified(Expression<?> expression) {
        assertEquals(ConstantExpression.class, expression.getClass());
    }
}
