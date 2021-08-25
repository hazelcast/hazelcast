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

package com.hazelcast.jet.sql.impl.expression;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.ParameterExpression;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
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
import static com.hazelcast.sql.SqlColumnType.NULL;
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
public class ColumnIntegrationTest extends ExpressionTestSupport {
    @Before
    public void before() {
        put(1);
    }

    @Test
    public void testLiteral() {
        checkValue0(sql("null"), NULL, null);
        checkValue0(sql("unknown"), BOOLEAN, null);

        checkValue0(sql("''"), VARCHAR, "");
        checkValue0(sql("'f'"), VARCHAR, "f");
        checkValue0(sql("'foo'"), VARCHAR, "foo");

        checkValue0(sql("false"), BOOLEAN, false);
        checkValue0(sql("true"), BOOLEAN, true);

        checkValue0(sql("0"), TINYINT, (byte) 0);
        checkValue0(sql("-0"), TINYINT, (byte) 0);
        checkValue0(sql("000"), TINYINT, (byte) 0);
        checkValue0(sql("1"), TINYINT, (byte) 1);
        checkValue0(sql("-1"), TINYINT, (byte) -1);
        checkValue0(sql("001"), TINYINT, (byte) 1);
        checkValue0(sql("100"), TINYINT, (byte) 100);
        checkValue0(sql(Byte.MAX_VALUE), TINYINT, Byte.MAX_VALUE);
        checkValue0(sql(Byte.MIN_VALUE), TINYINT, Byte.MIN_VALUE);

        checkValue0(sql((short) (Byte.MAX_VALUE + 1)), SMALLINT, (short) (Byte.MAX_VALUE + 1));
        checkValue0(sql((short) (Byte.MIN_VALUE - 1)), SMALLINT, (short) (Byte.MIN_VALUE - 1));
        checkValue0(sql(Short.MAX_VALUE), SMALLINT, Short.MAX_VALUE);
        checkValue0(sql(Short.MIN_VALUE), SMALLINT, Short.MIN_VALUE);

        checkValue0(sql(Short.MAX_VALUE + 1), INTEGER, Short.MAX_VALUE + 1);
        checkValue0(sql(Short.MIN_VALUE - 1), INTEGER, Short.MIN_VALUE - 1);
        checkValue0(sql(Integer.MAX_VALUE), INTEGER, Integer.MAX_VALUE);
        checkValue0(sql(Integer.MIN_VALUE), INTEGER, Integer.MIN_VALUE);

        checkValue0(sql(Integer.MAX_VALUE + 1L), BIGINT, Integer.MAX_VALUE + 1L);
        checkValue0(sql(Integer.MIN_VALUE - 1L), BIGINT, Integer.MIN_VALUE - 1L);
        checkValue0(sql(Long.MAX_VALUE), BIGINT, Long.MAX_VALUE);
        checkValue0(sql(Long.MIN_VALUE), BIGINT, Long.MIN_VALUE);

        checkValue0(sql(Long.MAX_VALUE + "0"), DECIMAL, new BigDecimal(Long.MAX_VALUE).multiply(BigDecimal.TEN));
        checkValue0(sql("0.0"), DECIMAL, new BigDecimal("0.0"));
        checkValue0(sql("1.0"), DECIMAL, new BigDecimal("1.0"));
        checkValue0(sql("1.000"), DECIMAL, new BigDecimal("1.000"));
        checkValue0(sql("001.000"), DECIMAL, new BigDecimal("1.000"));
        checkValue0(sql("1.1"), DECIMAL, new BigDecimal("1.1"));
        checkValue0(sql("1.100"), DECIMAL, new BigDecimal("1.100"));
        checkValue0(sql("001.100"), DECIMAL, new BigDecimal("1.100"));
        checkValue0(sql("-0.0"), DECIMAL, new BigDecimal("0.0"));
        checkValue0(sql("-1.0"), DECIMAL, new BigDecimal("-1.0"));
        checkValue0(sql("-001.100"), DECIMAL, new BigDecimal("-1.100"));
        checkValue0(sql(".0"), DECIMAL, BigDecimal.valueOf(0.0));
        checkValue0(sql(".1"), DECIMAL, BigDecimal.valueOf(0.1));

        checkValue0(sql("0e0"), DOUBLE, 0.0);
        checkValue0(sql("1e0"), DOUBLE, 1.0);
        checkValue0(sql("1e000"), DOUBLE, 1.0);
        checkValue0(sql("001e000"), DOUBLE, 1.0);
        checkValue0(sql("1.1e0"), DOUBLE, 1.1);
        checkValue0(sql("1.100e0"), DOUBLE, 1.1);
        checkValue0(sql("001.100e0"), DOUBLE, 1.1);
        checkValue0(sql("-0.0e0"), DOUBLE, 0.0);
        checkValue0(sql("-1.0e0"), DOUBLE, -1.0);
        checkValue0(sql("-001.100e0"), DOUBLE, -1.1);
        checkValue0(sql(".0e0"), DOUBLE, 0.0);
        checkValue0(sql(".1e0"), DOUBLE, 0.1);
        checkValue0(sql("1.1e1"), DOUBLE, 11.0);
        checkValue0(sql("1.1e-1"), DOUBLE, 0.11);
    }

    @Test
    public void testLiteralEquality() {
        checkEquals(ConstantExpression.create(1, INT), ConstantExpression.create(1, INT), true);
        checkEquals(ConstantExpression.create(1, INT), ConstantExpression.create(1, QueryDataType.BIGINT), false);
        checkEquals(ConstantExpression.create(1, INT), ConstantExpression.create(2, INT), false);
    }

    @Test
    public void testLiteralSerialization() {
        ConstantExpression<?> original = ConstantExpression.create(1, INT);
        ConstantExpression<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_CONSTANT);

        checkEquals(original, restored, true);
    }

    @Test
    public void testParameter() {
        checkFailure0(sql("?"), SqlErrorCode.PARSING, "Illegal use of dynamic parameter", 1);
    }

    @Test
    public void testParameterEquality() {
        checkEquals(ParameterExpression.create(1, INT), ParameterExpression.create(1, INT), true);
        checkEquals(ParameterExpression.create(1, INT), ParameterExpression.create(1, QueryDataType.BIGINT), false);
        checkEquals(ParameterExpression.create(1, INT), ParameterExpression.create(2, INT), false);
    }

    @Test
    public void testParameterSerialization() {
        ParameterExpression<?> original = ParameterExpression.create(1, INT);
        ParameterExpression<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_PARAMETER);

        checkEquals(original, restored, true);
    }

    @Test
    public void testColumn_auto() {
        for (ExpressionType<?> type : ExpressionTypes.all()) {
            Class<? extends ExpressionValue> clazz = ExpressionValue.createClass(type.typeName());

            ExpressionValue value = ExpressionValue.create(clazz, type.valueFrom());
            ExpressionValue nullValue = ExpressionValue.create(clazz, null);

            SqlColumnType expectedType = type.getFieldConverterType().getTypeFamily().getPublicType();
            Object expectedResult = type.getFieldConverterType().getConverter().convertToSelf(
                    type.getFieldConverterType().getConverter(),
                    type.valueFrom()
            );

            putAndCheckValue(value, sql("field1"), expectedType, expectedResult);
            putAndCheckValue(nullValue, sql("field1"), expectedType, null);
        }
    }

    @Test
    public void testColumn_manual() {
        putAndCheckValue('f', sql("this"), VARCHAR, "f");
        putAndCheckValue("foo", sql("this"), VARCHAR, "foo");

        putAndCheckValue(true, sql("this"), BOOLEAN, true);

        putAndCheckValue((byte) 1, sql("this"), TINYINT, (byte) 1);
        putAndCheckValue((short) 1, sql("this"), SMALLINT, (short) 1);
        putAndCheckValue(1, sql("this"), INTEGER, 1);
        putAndCheckValue(1L, sql("this"), BIGINT, 1L);
        putAndCheckValue(BigInteger.ONE, sql("this"), DECIMAL, BigDecimal.ONE);
        putAndCheckValue(BigDecimal.ONE, sql("this"), DECIMAL, BigDecimal.ONE);
        putAndCheckValue(1f, sql("this"), REAL, 1f);
        putAndCheckValue(1d, sql("this"), DOUBLE, 1d);

        putAndCheckValue(LOCAL_DATE_VAL, sql("this"), DATE, LOCAL_DATE_VAL);
        putAndCheckValue(LOCAL_TIME_VAL, sql("this"), TIME, LOCAL_TIME_VAL);
        putAndCheckValue(LOCAL_DATE_TIME_VAL, sql("this"), TIMESTAMP, LOCAL_DATE_TIME_VAL);
        putAndCheckValue(OFFSET_DATE_TIME_VAL, sql("this"), TIMESTAMP_WITH_TIME_ZONE, OFFSET_DATE_TIME_VAL);

        putAndCheckValue(OBJECT_VAL, sql("this"), OBJECT, OBJECT_VAL);
    }

    @Test
    public void testColumnEquality() {
        checkEquals(ColumnExpression.create(1, INT), ColumnExpression.create(1, INT), true);
        checkEquals(ColumnExpression.create(1, INT), ColumnExpression.create(1, QueryDataType.BIGINT), false);
        checkEquals(ColumnExpression.create(1, INT), ColumnExpression.create(2, INT), false);
    }

    @Test
    public void testColumnSerialization() {
        ColumnExpression<?> original = ColumnExpression.create(1, INT);
        ColumnExpression<?> restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_COLUMN);

        checkEquals(original, restored, true);
    }

    private static String sql(Object attribute) {
        return "SELECT " + attribute + " FROM map";
    }
}
