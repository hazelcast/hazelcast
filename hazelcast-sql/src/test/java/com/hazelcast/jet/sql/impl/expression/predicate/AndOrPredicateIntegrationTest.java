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

package com.hazelcast.jet.sql.impl.expression.predicate;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.predicate.AndPredicate;
import com.hazelcast.sql.impl.expression.predicate.OrPredicate;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BooleanBigDecimalVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BooleanBigIntegerVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BooleanByteVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BooleanDoubleVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BooleanLocalDateTimeVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BooleanLocalDateVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BooleanLocalTimeVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BooleanLongVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BooleanObjectVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BooleanOffsetDateTimeVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BooleanShortVal;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BooleanStringVal;
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

import static com.hazelcast.sql.impl.SqlDataSerializerHook.EXPRESSION_AND;
import static com.hazelcast.sql.impl.SqlDataSerializerHook.EXPRESSION_OR;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BooleanFloatVal;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue.BooleanIntegerVal;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AndOrPredicateIntegrationTest extends ExpressionTestSupport {
    @Parameterized.Parameter
    public Mode mode;

    @Parameterized.Parameters(name = "mode: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {Mode.AND},
                {Mode.OR}
        });
    }

    @Test
    public void testArg2() {
        // Column/column
        putCheckCommute(booleanValue2(true, true), "field1", "field2", result(true, true));
        putCheckCommute(booleanValue2(true, false), "field1", "field2", result(true, false));
        putCheckCommute(booleanValue2(true, null), "field1", "field2", result(true, null));
        putCheckCommute(booleanValue2(false, false), "field1", "field2", result(false, false));
        putCheckCommute(booleanValue2(false, null), "field1", "field2", result(false, null));
        putCheckCommute(booleanValue2(null, null), "field1", "field2", result(null, null));
        putCheckFailureSignatureCommute(new BooleanStringVal(), "field1", "field2", SqlColumnType.BOOLEAN, SqlColumnType.VARCHAR);
        putCheckFailureSignatureCommute(new BooleanByteVal(), "field1", "field2", SqlColumnType.BOOLEAN, SqlColumnType.TINYINT);
        putCheckFailureSignatureCommute(new BooleanShortVal(), "field1", "field2", SqlColumnType.BOOLEAN, SqlColumnType.SMALLINT);
        putCheckFailureSignatureCommute(new BooleanIntegerVal(), "field1", "field2", SqlColumnType.BOOLEAN, SqlColumnType.INTEGER);
        putCheckFailureSignatureCommute(new BooleanLongVal(), "field1", "field2", SqlColumnType.BOOLEAN, SqlColumnType.BIGINT);
        putCheckFailureSignatureCommute(new BooleanBigIntegerVal(), "field1", "field2", SqlColumnType.BOOLEAN, SqlColumnType.DECIMAL);
        putCheckFailureSignatureCommute(new BooleanBigDecimalVal(), "field1", "field2", SqlColumnType.BOOLEAN, SqlColumnType.DECIMAL);
        putCheckFailureSignatureCommute(new BooleanFloatVal(), "field1", "field2", SqlColumnType.BOOLEAN, SqlColumnType.REAL);
        putCheckFailureSignatureCommute(new BooleanDoubleVal(), "field1", "field2", SqlColumnType.BOOLEAN, SqlColumnType.DOUBLE);
        putCheckFailureSignatureCommute(new BooleanLocalDateVal(), "field1", "field2", SqlColumnType.BOOLEAN, SqlColumnType.DATE);
        putCheckFailureSignatureCommute(new BooleanLocalTimeVal(), "field1", "field2", SqlColumnType.BOOLEAN, SqlColumnType.TIME);
        putCheckFailureSignatureCommute(new BooleanLocalDateTimeVal(), "field1", "field2", SqlColumnType.BOOLEAN, SqlColumnType.TIMESTAMP);
        putCheckFailureSignatureCommute(new BooleanOffsetDateTimeVal(), "field1", "field2", SqlColumnType.BOOLEAN, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE);
        putCheckFailureSignatureCommute(new BooleanObjectVal(), "field1", "field2", SqlColumnType.BOOLEAN, SqlColumnType.OBJECT);

        // Column/literal
        putCheckCommute(booleanValue1(true), "field1", "true", result(true, true));
        putCheckCommute(booleanValue1(true), "field1", "false", result(true, false));
        putCheckCommute(booleanValue1(true), "field1", "null", result(true, null));
        putCheckCommute(booleanValue1(false), "field1", "true", result(false, true));
        putCheckCommute(booleanValue1(false), "field1", "false", result(false, false));
        putCheckCommute(booleanValue1(false), "field1", "null", result(false, null));
        putCheckCommute(booleanValue1(null), "field1", "true", result(null, true));
        putCheckCommute(booleanValue1(null), "field1", "false", result(null, false));
        putCheckCommute(booleanValue1(null), "field1", "null", result(null, null));
        putCheckFailureSignatureCommute(booleanValue1(true), "field1", "'true'", SqlColumnType.BOOLEAN, SqlColumnType.VARCHAR);
        putCheckFailureSignatureCommute(booleanValue1(true), "field1", "1", SqlColumnType.BOOLEAN, SqlColumnType.TINYINT);
        putCheckFailureSignatureCommute(booleanValue1(true), "field1", "1.1", SqlColumnType.BOOLEAN, SqlColumnType.DECIMAL);
        putCheckFailureSignatureCommute(booleanValue1(true), "field1", "1.1E1", SqlColumnType.BOOLEAN, SqlColumnType.DOUBLE);
        putCheckFailureSignatureCommute(new ExpressionValue.StringVal(), "field1", "true", SqlColumnType.VARCHAR, SqlColumnType.BOOLEAN);
        putCheckFailureSignatureCommute(new ExpressionValue.ByteVal(), "field1", "true", SqlColumnType.TINYINT, SqlColumnType.BOOLEAN);
        putCheckFailureSignatureCommute(new ExpressionValue.ShortVal(), "field1", "true", SqlColumnType.SMALLINT, SqlColumnType.BOOLEAN);
        putCheckFailureSignatureCommute(new ExpressionValue.IntegerVal(), "field1", "true", SqlColumnType.INTEGER, SqlColumnType.BOOLEAN);
        putCheckFailureSignatureCommute(new ExpressionValue.LongVal(), "field1", "true", SqlColumnType.BIGINT, SqlColumnType.BOOLEAN);
        putCheckFailureSignatureCommute(new ExpressionValue.BigIntegerVal(), "field1", "true", SqlColumnType.DECIMAL, SqlColumnType.BOOLEAN);
        putCheckFailureSignatureCommute(new ExpressionValue.BigDecimalVal(), "field1", "true", SqlColumnType.DECIMAL, SqlColumnType.BOOLEAN);
        putCheckFailureSignatureCommute(new ExpressionValue.FloatVal(), "field1", "true", SqlColumnType.REAL, SqlColumnType.BOOLEAN);
        putCheckFailureSignatureCommute(new ExpressionValue.DoubleVal(), "field1", "true", SqlColumnType.DOUBLE, SqlColumnType.BOOLEAN);
        putCheckFailureSignatureCommute(new ExpressionValue.LocalDateVal(), "field1", "true", SqlColumnType.DATE, SqlColumnType.BOOLEAN);
        putCheckFailureSignatureCommute(new ExpressionValue.LocalTimeVal(), "field1", "true", SqlColumnType.TIME, SqlColumnType.BOOLEAN);
        putCheckFailureSignatureCommute(new ExpressionValue.LocalDateTimeVal(), "field1", "true", SqlColumnType.TIMESTAMP, SqlColumnType.BOOLEAN);
        putCheckFailureSignatureCommute(new ExpressionValue.OffsetDateTimeVal(), "field1", "true", SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, SqlColumnType.BOOLEAN);
        putCheckFailureSignatureCommute(OBJECT_VAL, "field1", "true", SqlColumnType.OBJECT, SqlColumnType.BOOLEAN);

        // Column/parameter
        putCheckCommute(booleanValue1(true), "field1", "?", result(true, true), true);
        putCheckCommute(booleanValue1(true), "field1", "?", result(true, false), false);
        putCheckCommute(booleanValue1(true), "field1", "?", result(true, null), (Boolean) null);
        putCheckCommute(booleanValue1(false), "field1", "?", result(false, true), true);
        putCheckCommute(booleanValue1(false), "field1", "?", result(false, false), false);
        putCheckCommute(booleanValue1(false), "field1", "?", result(false, null), (Boolean) null);
        putCheckCommute(booleanValue1(null), "field1", "?", result(null, true), true);
        putCheckCommute(booleanValue1(null), "field1", "?", result(null, false), false);
        putCheckCommute(booleanValue1(null), "field1", "?", result(null, null), (Boolean) null);
        putCheckFailureParameterCommute(booleanValue1(true), "field1", "?", 0, SqlColumnType.VARCHAR, "1");
        putCheckFailureParameterCommute(booleanValue1(true), "field1", "?", 0, SqlColumnType.TINYINT, (byte) 1);
        putCheckFailureParameterCommute(booleanValue1(true), "field1", "?", 0, SqlColumnType.SMALLINT, (short) 1);
        putCheckFailureParameterCommute(booleanValue1(true), "field1", "?", 0, SqlColumnType.INTEGER, 1);
        putCheckFailureParameterCommute(booleanValue1(true), "field1", "?", 0, SqlColumnType.BIGINT, 1L);
        putCheckFailureParameterCommute(booleanValue1(true), "field1", "?", 0, SqlColumnType.DECIMAL, BigInteger.ONE);
        putCheckFailureParameterCommute(booleanValue1(true), "field1", "?", 0, SqlColumnType.DECIMAL, BigDecimal.ONE);
        putCheckFailureParameterCommute(booleanValue1(true), "field1", "?", 0, SqlColumnType.REAL, 1f);
        putCheckFailureParameterCommute(booleanValue1(true), "field1", "?", 0, SqlColumnType.DOUBLE, 1d);
        putCheckFailureParameterCommute(booleanValue1(true), "field1", "?", 0, SqlColumnType.DATE, LOCAL_DATE_VAL);
        putCheckFailureParameterCommute(booleanValue1(true), "field1", "?", 0, SqlColumnType.TIME, LOCAL_TIME_VAL);
        putCheckFailureParameterCommute(booleanValue1(true), "field1", "?", 0, SqlColumnType.TIMESTAMP, LOCAL_DATE_TIME_VAL);
        putCheckFailureParameterCommute(booleanValue1(true), "field1", "?", 0, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, OFFSET_DATE_TIME_VAL);
        putCheckFailureParameterCommute(booleanValue1(true), "field1", "?", 0, SqlColumnType.OBJECT, OBJECT_VAL);
        putCheckFailureSignatureCommute("true", "this", "?", SqlColumnType.VARCHAR, SqlColumnType.BOOLEAN, true);
        putCheckFailureSignatureCommute((byte) 1, "this", "?", SqlColumnType.TINYINT, SqlColumnType.BOOLEAN, true);
        putCheckFailureSignatureCommute((short) 1, "this", "?", SqlColumnType.SMALLINT, SqlColumnType.BOOLEAN, true);
        putCheckFailureSignatureCommute(1, "this", "?", SqlColumnType.INTEGER, SqlColumnType.BOOLEAN, true);
        putCheckFailureSignatureCommute(1L, "this", "?", SqlColumnType.BIGINT, SqlColumnType.BOOLEAN, true);
        putCheckFailureSignatureCommute(BigInteger.ONE, "this", "?", SqlColumnType.DECIMAL, SqlColumnType.BOOLEAN, true);
        putCheckFailureSignatureCommute(BigDecimal.ONE, "this", "?", SqlColumnType.DECIMAL, SqlColumnType.BOOLEAN, true);
        putCheckFailureSignatureCommute(1f, "this", "?", SqlColumnType.REAL, SqlColumnType.BOOLEAN, true);
        putCheckFailureSignatureCommute(1d, "this", "?", SqlColumnType.DOUBLE, SqlColumnType.BOOLEAN, true);
        putCheckFailureSignatureCommute(LOCAL_DATE_VAL, "this", "?", SqlColumnType.DATE, SqlColumnType.BOOLEAN, true);
        putCheckFailureSignatureCommute(LOCAL_TIME_VAL, "this", "?", SqlColumnType.TIME, SqlColumnType.BOOLEAN, true);
        putCheckFailureSignatureCommute(LOCAL_DATE_TIME_VAL, "this", "?", SqlColumnType.TIMESTAMP, SqlColumnType.BOOLEAN, true);
        putCheckFailureSignatureCommute(OFFSET_DATE_TIME_VAL, "this", "?", SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, SqlColumnType.BOOLEAN, true);
        putCheckFailureSignatureCommute(OBJECT_VAL, "field1", "?", SqlColumnType.OBJECT, SqlColumnType.BOOLEAN, true);

        // Parameter/literal
        checkCommute("?", "true", result(true, true), true);
        checkCommute("?", "false", result(true, false), true);
        checkCommute("?", "null", result(true, null), true);
        checkCommute("?", "true", result(false, true), false);
        checkCommute("?", "false", result(false, false), false);
        checkCommute("?", "null", result(false, null), false);
        checkCommute("?", "true", result(null, true), (Boolean) null);
        checkCommute("?", "false", result(null, false), (Boolean) null);
        checkCommute("?", "null", result(null, null), (Boolean) null);
        checkFailureSignatureCommute("?", "'true'", SqlColumnType.BOOLEAN, SqlColumnType.VARCHAR, true);
        checkFailureSignatureCommute("?", "1", SqlColumnType.BOOLEAN, SqlColumnType.TINYINT, true);
        checkFailureSignatureCommute("?", "1.1", SqlColumnType.BOOLEAN, SqlColumnType.DECIMAL, true);
        checkFailureSignatureCommute("?", "1.1E1", SqlColumnType.BOOLEAN, SqlColumnType.DOUBLE, true);
        checkFailureParameterCommute("?", "true", 0, SqlColumnType.VARCHAR, "1");
        checkFailureParameterCommute("?", "true", 0, SqlColumnType.TINYINT, (byte) 1);
        checkFailureParameterCommute("?", "true", 0, SqlColumnType.SMALLINT, (short) 1);
        checkFailureParameterCommute("?", "true", 0, SqlColumnType.INTEGER, 1);
        checkFailureParameterCommute("?", "true", 0, SqlColumnType.BIGINT, 1L);
        checkFailureParameterCommute("?", "true", 0, SqlColumnType.DECIMAL, BigInteger.ONE);
        checkFailureParameterCommute("?", "true", 0, SqlColumnType.DECIMAL, BigDecimal.ONE);
        checkFailureParameterCommute("?", "true", 0, SqlColumnType.REAL, 1f);
        checkFailureParameterCommute("?", "true", 0, SqlColumnType.DOUBLE, 1d);
        checkFailureParameterCommute("?", "true", 0, SqlColumnType.DATE, LOCAL_DATE_VAL);
        checkFailureParameterCommute("?", "true", 0, SqlColumnType.TIME, LOCAL_TIME_VAL);
        checkFailureParameterCommute("?", "true", 0, SqlColumnType.TIMESTAMP, LOCAL_DATE_TIME_VAL);
        checkFailureParameterCommute("?", "true", 0, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, OFFSET_DATE_TIME_VAL);
        checkFailureParameterCommute("?", "true", 0, SqlColumnType.OBJECT, OBJECT_VAL);

        // Parameter/parameter
        checkCommute("?", "?", result(true, true), true, true);
        checkCommute("?", "?", result(true, false), true, false);
        checkCommute("?", "?", result(true, null), true, null);
        checkCommute("?", "?", result(false, true), false, true);
        checkCommute("?", "?", result(false, false), false, false);
        checkCommute("?", "?", result(false, null), false, null);
        checkFailureParameterCommute("?", "?", 1, SqlColumnType.VARCHAR, true, "true");
        checkFailureParameterCommute("?", "?", 1, SqlColumnType.TINYINT, true, (byte) 1);
        checkFailureParameterCommute("?", "?", 1, SqlColumnType.SMALLINT, true, (short) 1);
        checkFailureParameterCommute("?", "?", 1, SqlColumnType.INTEGER, true, 1);
        checkFailureParameterCommute("?", "?", 1, SqlColumnType.BIGINT, true, 1L);
        checkFailureParameterCommute("?", "?", 1, SqlColumnType.DECIMAL, true, BigInteger.ONE);
        checkFailureParameterCommute("?", "?", 1, SqlColumnType.DECIMAL, true, BigDecimal.ONE);
        checkFailureParameterCommute("?", "?", 1, SqlColumnType.REAL, true, 1f);
        checkFailureParameterCommute("?", "?", 1, SqlColumnType.DOUBLE, true, 1d);
        checkFailureParameterCommute("?", "?", 1, SqlColumnType.DATE, true, LOCAL_DATE_VAL);
        checkFailureParameterCommute("?", "?", 1, SqlColumnType.TIME, true, LOCAL_TIME_VAL);
        checkFailureParameterCommute("?", "?", 1, SqlColumnType.TIMESTAMP, true, LOCAL_DATE_TIME_VAL);
        checkFailureParameterCommute("?", "?", 1, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE, true, OFFSET_DATE_TIME_VAL);
        checkFailureParameterCommute("?", "?", 1, SqlColumnType.OBJECT, true, OBJECT_VAL);

        // Literal/literal
        checkCommute("true", "true", result(true, true));
        checkCommute("true", "false", result(true, false));
        checkCommute("true", "null", result(true, null));
        checkCommute("false", "true", result(false, true));
        checkCommute("false", "false", result(false, false));
        checkCommute("false", "null", result(false, null));
        checkFailureSignatureCommute("true", "'true'", SqlColumnType.BOOLEAN, SqlColumnType.VARCHAR);
        checkFailureSignatureCommute("true", "1", SqlColumnType.BOOLEAN, SqlColumnType.TINYINT);
        checkFailureSignatureCommute("true", "1.1", SqlColumnType.BOOLEAN, SqlColumnType.DECIMAL);
        checkFailureSignatureCommute("true", "1.1E1", SqlColumnType.BOOLEAN, SqlColumnType.DOUBLE);
    }

    /**
     * Test more than two arguments. Since we do not have any specific logic for many-arg case, we do just one test
     * to ensure that it works.
     */
    @Test
    public void testArg3() {
        put(0);

        checkValue0(sql("?", "?", "?"), SqlColumnType.BOOLEAN, result(null, true, false), null, true, false);
    }

    private void putCheckCommute(Object value, String operand1, String operand2, Boolean expectedResult, Object... params) {
        put(value);

        checkCommute(operand1, operand2, expectedResult, params);
    }

    private void checkCommute(String operand1, String operand2, Boolean expectedResult, Object... params) {
        check(operand1, operand2, expectedResult, params);
        check(operand2, operand1, expectedResult, params);
    }

    private void check(String operand1, String operand2, Boolean expectedResult, Object... params) {
        String sql = sql(operand1, operand2);

        checkValue0(sql, SqlColumnType.BOOLEAN, expectedResult, params);
    }

    private void putCheckFailureSignatureCommute(
            Object value,
            String operand1,
            String operand2,
            SqlColumnType type1,
            SqlColumnType type2,
            Object... params
    ) {
        put(value);

        checkFailureSignatureCommute(operand1, operand2, type1, type2, params);
    }

    private void checkFailureSignatureCommute(
            String operand1,
            String operand2,
            SqlColumnType type1,
            SqlColumnType type2,
            Object... params
    ) {
        checkFailureSignature(operand1, operand2, type1, type2, params);
        checkFailureSignature(operand2, operand1, type2, type1, params);
    }

    private void checkFailureSignature(
            String operand1,
            String operand2,
            SqlColumnType type1,
            SqlColumnType type2,
            Object... params
    ) {
        String sql = sql(operand1, operand2);

        checkFailure0(sql, SqlErrorCode.PARSING, signatureErrorOperator(mode.name(), type1, type2), params);
    }

    private void putCheckFailureParameterCommute(
            Object value,
            String operand1,
            String operand2,
            int parameterPosition,
            SqlColumnType parameterType,
            Object... params
    ) {
        put(value);

        checkFailureParameterCommute(operand1, operand2, parameterPosition, parameterType, params);
    }

    private void checkFailureParameterCommute(
            String operand1,
            String operand2,
            int parameterPosition,
            SqlColumnType parameterType,
            Object... params
    ) {
        checkFailureParameter(operand1, operand2, parameterPosition, parameterType, params);
        checkFailureParameter(operand2, operand1, parameterPosition, parameterType, params);
    }

    private void checkFailureParameter(
            String operand1,
            String operand2,
            int parameterPosition,
            SqlColumnType parameterType,
            Object... params
    ) {
        String sql = sql(operand1, operand2);

        checkFailure0(
                sql,
                SqlErrorCode.DATA_EXCEPTION,
                parameterError(parameterPosition, SqlColumnType.BOOLEAN, parameterType),
                params
        );
    }

    private String sql(Object... operands) {
        assert operands != null;
        assert operands.length > 1;

        StringBuilder condition = new StringBuilder();
        condition.append(operands[0]);

        for (int i = 1; i < operands.length; i++) {
            condition.append(" ");
            condition.append(mode.name());
            condition.append(" ");
            condition.append(operands[i]);
        }

        return "SELECT " + condition + " FROM map";
    }

    private Boolean result(Boolean... values) {
        assert values != null;

        if (mode == Mode.AND) {
            for (Boolean value : values) {
                if (Boolean.FALSE.equals(value)) {
                    return false;
                }
            }

            for (Boolean value : values) {
                if (value == null) {
                    return null;
                }
            }

            return true;
        } else {
            assert mode == Mode.OR;

            for (Boolean value : values) {
                if (Boolean.TRUE.equals(value)) {
                    return true;
                }
            }

            for (Boolean value : values) {
                if (value == null) {
                    return null;
                }
            }

            return false;
        }
    }

    @Test
    public void testEquality() {
        checkEquals(predicate(true, false), predicate(true, false), true);
        checkEquals(predicate(true, false), predicate(true, true), false);
        checkEquals(predicate(true, true), predicate(true, true, true), false);
    }

    @Test
    public void testSerialization() {
        Expression<?> original = predicate(true, false);
        Expression<?> restored = serializeAndCheck(original, mode == Mode.AND ? EXPRESSION_AND : EXPRESSION_OR);

        checkEquals(original, restored, true);
    }

    private Expression<?> predicate(Boolean... values) {
        Expression<?>[] operands = new Expression<?>[values.length];
        for (int i = 0; i < values.length; ++i) {
            operands[i] = ConstantExpression.create(values[i], QueryDataType.BOOLEAN);
        }
        return mode == Mode.AND ? AndPredicate.create(operands) : OrPredicate.create(operands);
    }

    private enum Mode {
        AND,
        OR
    }
}
