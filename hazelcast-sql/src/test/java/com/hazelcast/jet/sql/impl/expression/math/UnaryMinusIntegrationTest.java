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
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.math.UnaryMinusFunction;
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
import static com.hazelcast.sql.impl.type.QueryDataType.INT;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnaryMinusIntegrationTest extends ExpressionTestSupport {
    @Test
    public void testColumn() {
        putAndCheckValue(new ExpressionValue.IntegerVal(), sql("field1"), BIGINT, null);

        putAndCheckValue((byte) 0, sql("this"), SMALLINT, (short) 0);
        putAndCheckValue((byte) 1, sql("this"), SMALLINT, (short) -1);
        putAndCheckValue((byte) -1, sql("this"), SMALLINT, (short) 1);
        putAndCheckValue(Byte.MAX_VALUE, sql("this"), SMALLINT, (short) -Byte.MAX_VALUE);
        putAndCheckValue(Byte.MIN_VALUE, sql("this"), SMALLINT, (short) -Byte.MIN_VALUE);

        putAndCheckValue((short) 0, sql("this"), INTEGER, 0);
        putAndCheckValue((short) 1, sql("this"), INTEGER, -1);
        putAndCheckValue((short) -1, sql("this"), INTEGER, 1);
        putAndCheckValue(Short.MAX_VALUE, sql("this"), INTEGER, -Short.MAX_VALUE);
        putAndCheckValue(Short.MIN_VALUE, sql("this"), INTEGER, -Short.MIN_VALUE);

        putAndCheckValue(0, sql("this"), BIGINT, 0L);
        putAndCheckValue(1, sql("this"), BIGINT, -1L);
        putAndCheckValue(-1, sql("this"), BIGINT, 1L);
        putAndCheckValue(Integer.MAX_VALUE, sql("this"), BIGINT, -1L * Integer.MAX_VALUE);
        putAndCheckValue(Integer.MIN_VALUE, sql("this"), BIGINT, -1L * Integer.MIN_VALUE);

        putAndCheckValue(0L, sql("this"), BIGINT, 0L);
        putAndCheckValue(1L, sql("this"), BIGINT, -1L);
        putAndCheckValue(-1L, sql("this"), BIGINT, 1L);
        putAndCheckValue(Long.MAX_VALUE, sql("this"), BIGINT, -1L * Long.MAX_VALUE);
        putAndCheckFailure(Long.MIN_VALUE, sql("this"), SqlErrorCode.DATA_EXCEPTION, "BIGINT overflow in unary '-' operator");
        putAndCheckValue(BigInteger.ONE, sql("this"), DECIMAL, BigDecimal.ONE.negate());
        putAndCheckValue(BigDecimal.ONE, sql("this"), DECIMAL, BigDecimal.ONE.negate());

        putAndCheckValue(0f, sql("this"), REAL, -0f);
        putAndCheckValue(-0f, sql("this"), REAL, 0f);
        putAndCheckValue(1f, sql("this"), REAL, -1f);
        putAndCheckValue(-1f, sql("this"), REAL, 1f);

        putAndCheckValue(0d, sql("this"), DOUBLE, -0d);
        putAndCheckValue(-0d, sql("this"), DOUBLE, 0d);
        putAndCheckValue(1d, sql("this"), DOUBLE, -1d);
        putAndCheckValue(-1d, sql("this"), DOUBLE, 1d);

        putAndCheckFailure("foo", sql("this"), SqlErrorCode.PARSING, signatureError(VARCHAR));
        putAndCheckFailure(true, sql("this"), SqlErrorCode.PARSING, signatureError(BOOLEAN));
        putAndCheckFailure(LOCAL_DATE_VAL, sql("this"), SqlErrorCode.PARSING, signatureError(DATE));
        putAndCheckFailure(LOCAL_TIME_VAL, sql("this"), SqlErrorCode.PARSING, signatureError(TIME));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql("this"), SqlErrorCode.PARSING, signatureError(TIMESTAMP));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql("this"), SqlErrorCode.PARSING, signatureError(TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(OBJECT_VAL, sql("this"), SqlErrorCode.PARSING, signatureError(OBJECT));
    }

    @Test
    public void testParameter() {
        put(1);

        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), 'f');
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, VARCHAR), "foo");

        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, BOOLEAN), true);

        checkValue0(sql("?"), BIGINT, -1L, (byte) 1);
        checkValue0(sql("?"), BIGINT, -1L, (short) 1);
        checkValue0(sql("?"), BIGINT, -1L, 1);
        checkValue0(sql("?"), BIGINT, -1L, 1L);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BigInteger.ONE);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, DECIMAL), BigDecimal.ONE);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, REAL), 1f);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, DOUBLE), 1d);

        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, DATE), LOCAL_DATE_VAL);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, TIME), LOCAL_TIME_VAL);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);

        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, BIGINT, OBJECT), OBJECT_VAL);
    }

    @Test
    public void testLiteral() {
        put(1);

        checkValue0(sql("null"), BIGINT, null);

        checkValue0(sql("1"), TINYINT, (byte) -1);
        checkValue0(sql("1.1"), DECIMAL, new BigDecimal("-1.1"));
        checkValue0(sql("1.1E0"), DOUBLE, -1.1d);

        checkFailure0(sql("'foo'"), SqlErrorCode.PARSING, signatureError(VARCHAR));
        checkFailure0(sql("true"), SqlErrorCode.PARSING, signatureError(BOOLEAN));
    }

    @Test
    public void testEquality() {
        checkEquals(UnaryMinusFunction.create(ConstantExpression.create(1, INT), INT),
                UnaryMinusFunction.create(ConstantExpression.create(1, INT), INT), true);

        checkEquals(UnaryMinusFunction.create(ConstantExpression.create(1, INT), INT),
                UnaryMinusFunction.create(ConstantExpression.create(1, INT), QueryDataType.BIGINT), false);

        checkEquals(UnaryMinusFunction.create(ConstantExpression.create(1, INT), INT),
                UnaryMinusFunction.create(ConstantExpression.create(2, INT), QueryDataType.BIGINT), false);
    }

    @Test
    public void testSerialization() {
        UnaryMinusFunction<?> original = UnaryMinusFunction.create(ConstantExpression.create(1, INT), INT);
        UnaryMinusFunction<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_UNARY_MINUS);

        checkEquals(original, restored, true);
    }

    private static String signatureError(SqlColumnType type) {
        return signatureErrorOperator("-", type);
    }

    private static String sql(String attribute) {
        return "SELECT -" + attribute + " FROM map";
    }
}
