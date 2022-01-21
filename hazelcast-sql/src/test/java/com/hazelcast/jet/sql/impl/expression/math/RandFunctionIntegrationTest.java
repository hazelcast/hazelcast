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
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.math.RandFunction;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RandFunctionIntegrationTest extends ExpressionTestSupport {
    @Test
    public void testNoArg() {
        put(0);

        double res1 = checkValue("", SKIP_VALUE_CHECK);
        double res2 = checkValue("", SKIP_VALUE_CHECK);

        assertNotEquals(res1, res2);
    }

    @Test
    public void testMultipleCallsInSingleQuery() {
        putAll(0, 1, 2, 3);

        List<SqlRow> rows = execute("SELECT RAND(), RAND() FROM map");
        assertEquals(4, rows.size());

        Set<Double> values = new HashSet<>();

        for (SqlRow row : rows) {
            double value1 = row.getObject(0);
            double value2 = row.getObject(1);

            assertNotEquals(value1, value2);

            values.add(value1);
        }

        assertTrue(values.size() > 1);
    }

    @Test
    public void testColumn() {
        checkColumn((byte) 1, 1L);
        checkColumn((short) 1, 1L);
        checkColumn(1, 1L);
        checkColumn(1L, 1L);

        put(new ExpressionValue.IntegerVal());
        double nullRes1 = checkValue("field1", SKIP_VALUE_CHECK);
        double nullRes2 = checkValue("field1", SKIP_VALUE_CHECK);
        assertNotEquals(nullRes1, nullRes2);

        checkColumnFailure("1", SqlErrorCode.PARSING, signatureError(VARCHAR));
        checkColumnFailure('1', SqlErrorCode.PARSING, signatureError(VARCHAR));
        checkColumnFailure(true, SqlErrorCode.PARSING, signatureError(BOOLEAN));
        checkColumnFailure(1f, SqlErrorCode.PARSING, signatureError(REAL));
        checkColumnFailure(1d, SqlErrorCode.PARSING, signatureError(DOUBLE));
        checkColumnFailure(BigInteger.ONE, SqlErrorCode.PARSING, signatureError(DECIMAL));
        checkColumnFailure(BigDecimal.ONE, SqlErrorCode.PARSING, signatureError(DECIMAL));
        checkColumnFailure(LOCAL_DATE_VAL, SqlErrorCode.PARSING, signatureError(DATE));
        checkColumnFailure(LOCAL_TIME_VAL, SqlErrorCode.PARSING, signatureError(TIME));
        checkColumnFailure(LOCAL_DATE_TIME_VAL, SqlErrorCode.PARSING, signatureError(TIMESTAMP));
        checkColumnFailure(OFFSET_DATE_TIME_VAL, SqlErrorCode.PARSING, signatureError(TIMESTAMP_WITH_TIME_ZONE));
        checkColumnFailure(OBJECT_VAL, SqlErrorCode.PARSING, signatureError(OBJECT));
    }

    private void checkColumn(Object value, long expectedSeed) {
        put(value);

        checkValue("this", new Random(expectedSeed).nextDouble());
    }

    private void checkColumnFailure(Object value, int expectedErrorCode, String expectedErrorMessage) {
        put(value);

        checkFailure("this", expectedErrorCode, expectedErrorMessage);
    }

    @Test
    public void testParameter() {
        put(0);

        double nullRes1 = checkValue("?", SKIP_VALUE_CHECK, new Object[]{null});
        double nullRes2 = checkValue("?", SKIP_VALUE_CHECK, new Object[]{null});
        assertNotEquals(nullRes1, nullRes2);

        checkParameter((byte) 1, 1L);
        checkParameter((short) 1, 1L);
        checkParameter(1, 1L);
        checkParameter(1L, 1L);

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

    private void checkParameter(Object param, long expectedSeed) {
        checkValue("?", new Random(expectedSeed).nextDouble(), param);
    }

    @Test
    public void testLiteral() {
        put(0);

        checkValue(0, new Random(0).nextDouble());
        checkValue(Long.MAX_VALUE, new Random(Long.MAX_VALUE).nextDouble());

        double nullRes1 = checkValue("null", SKIP_VALUE_CHECK);
        double nullRes2 = checkValue("null", SKIP_VALUE_CHECK);
        assertNotEquals(nullRes1, nullRes2);

        checkFailure("'foo'", SqlErrorCode.PARSING, signatureError(VARCHAR));
        checkFailure("true", SqlErrorCode.PARSING, signatureError(BOOLEAN));
        checkFailure("1.1", SqlErrorCode.PARSING, signatureError(DECIMAL));
        checkFailure("1.1E1", SqlErrorCode.PARSING, signatureError(DOUBLE));
    }

    private static String signatureError(SqlColumnType type) {
        return signatureErrorFunction("RAND", type);
    }

    private Double checkValue(Object operand, Object expectedValue, Object... params) {
        String sql = "SELECT RAND(" + operand + ") FROM map";

        return (Double) checkValue0(sql, SqlColumnType.DOUBLE, expectedValue, params);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT RAND(" + operand + ") FROM map";

        checkFailure0(sql, expectedErrorCode, expectedErrorMessage, params);
    }

    @Test
    public void testEquals() {
        RandFunction function = RandFunction.create(ConstantExpression.create(1, QueryDataType.INT));

        checkEquals(function, RandFunction.create(ConstantExpression.create(1, QueryDataType.INT)), true);
        checkEquals(function, RandFunction.create(ConstantExpression.create(2, QueryDataType.INT)), false);
    }

    @Test
    public void testSerialization() {
        RandFunction original = RandFunction.create(ConstantExpression.create(1, QueryDataType.INT));
        RandFunction restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_RAND);

        checkEquals(original, restored, true);
    }
}
