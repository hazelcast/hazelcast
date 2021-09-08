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

import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.impl.expression.math.DoubleFunction;
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

import static com.hazelcast.sql.SqlColumnType.BOOLEAN;
import static com.hazelcast.sql.SqlColumnType.DATE;
import static com.hazelcast.sql.SqlColumnType.DOUBLE;
import static com.hazelcast.sql.SqlColumnType.OBJECT;
import static com.hazelcast.sql.SqlColumnType.TIME;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP;
import static com.hazelcast.sql.SqlColumnType.TIMESTAMP_WITH_TIME_ZONE;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;
import static com.hazelcast.sql.impl.expression.math.DoubleFunction.COS;
import static com.hazelcast.sql.impl.expression.math.DoubleFunction.SIN;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DoubleFunctionIntegrationTest extends ExpressionTestSupport {
    @Parameterized.Parameter
    public FunctionInfo function;

    @Parameterized.Parameters(name = "function: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {new FunctionInfo("COS")},
                {new FunctionInfo("SIN")},
                {new FunctionInfo("TAN")},
                {new FunctionInfo("COT")},
                {new FunctionInfo("ACOS")},
                {new FunctionInfo("ASIN")},
                {new FunctionInfo("ATAN")},
                {new FunctionInfo("EXP")},
                {new FunctionInfo("LN")},
                {new FunctionInfo("LOG10")},
                {new FunctionInfo("DEGREES")},
                {new FunctionInfo("RADIANS")},
                {new FunctionInfo("SQUARE")},
                {new FunctionInfo("SQRT")},
                {new FunctionInfo("CBRT")},
        });
    }

    @Test
    public void testColumn() {
        // NULL
        putAndCheckValue(new ExpressionValue.DoubleVal(), sql("field1"), DOUBLE, null);

        // Numeric
        putAndCheckValue((byte) 1, sql("this"), DOUBLE, function.process(1d));
        putAndCheckValue((short) 1, sql("this"), DOUBLE, function.process(1d));
        putAndCheckValue(1, sql("this"), DOUBLE, function.process(1d));
        putAndCheckValue(1L, sql("this"), DOUBLE, function.process(1d));
        putAndCheckValue(1L, sql("this"), DOUBLE, function.process(1d));
        putAndCheckValue(BigInteger.ONE, sql("this"), DOUBLE, function.process(1d));
        putAndCheckValue(BigDecimal.ONE, sql("this"), DOUBLE, function.process(1d));
        putAndCheckValue(1f, sql("this"), DOUBLE, function.process(1d));
        putAndCheckValue(1d, sql("this"), DOUBLE, function.process(1d));

        // Other
        putAndCheckFailure('1', sql("this"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, VARCHAR));
        putAndCheckFailure("1", sql("this"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, VARCHAR));
        putAndCheckFailure(true, sql("this"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, BOOLEAN));
        putAndCheckFailure(LOCAL_DATE_VAL, sql("this"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, DATE));
        putAndCheckFailure(LOCAL_TIME_VAL, sql("this"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, TIME));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql("this"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, TIMESTAMP));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql("this"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(OBJECT_VAL, sql("this"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, OBJECT));
    }

    @Test
    public void testLiteral() {
        put(0);

        checkValue0(sql("null"), DOUBLE, null);
        checkValue0(sql("1"), DOUBLE, function.process(1d));
        checkValue0(sql("1.0"), DOUBLE, function.process(1d));
        checkValue0(sql("1.5"), DOUBLE, function.process(1.5d));
        checkValue0(sql("0.001"), DOUBLE, function.process(0.001d));
        checkValue0(sql("1E0"), DOUBLE, function.process(1d));

        checkFailure0(sql("true"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, BOOLEAN));
        checkFailure0(sql("'1'"), SqlErrorCode.PARSING, signatureErrorFunction(function.name, VARCHAR));
    }

    @Test
    public void testParameter() {
        put(0);

        checkValue0(sql("?"), DOUBLE, null, new Object[]{null});

        checkValue0(sql("?"), DOUBLE, function.process(1d), (byte) 1);
        checkValue0(sql("?"), DOUBLE, function.process(1d), (short) 1);
        checkValue0(sql("?"), DOUBLE, function.process(1d), 1);
        checkValue0(sql("?"), DOUBLE, function.process(1d), 1L);
        checkValue0(sql("?"), DOUBLE, function.process(1d), BigInteger.ONE);
        checkValue0(sql("?"), DOUBLE, function.process(1d), BigDecimal.ONE);
        checkValue0(sql("?"), DOUBLE, function.process(1d), 1f);
        checkValue0(sql("?"), DOUBLE, function.process(1d), 1d);
        checkValue0(sql("?"), DOUBLE, function.process(0.001d), 0.001d);

        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, DOUBLE, VARCHAR), '1');
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, DOUBLE, VARCHAR), "1");
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, DOUBLE, BOOLEAN), true);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, DOUBLE, DATE), LOCAL_DATE_VAL);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, DOUBLE, TIME), LOCAL_TIME_VAL);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, DOUBLE, TIMESTAMP), LOCAL_DATE_TIME_VAL);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, DOUBLE, TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, DOUBLE, OBJECT), OBJECT_VAL);
    }

    private String sql(String operand) {
        return "SELECT " + function.name + "(" + operand + ") FROM map";
    }

    @Test
    public void testEquals() {
        DoubleFunction function = DoubleFunction.create(ConstantExpression.create(1d, QueryDataType.DOUBLE), COS);

        checkEquals(function, DoubleFunction.create(ConstantExpression.create(1d, QueryDataType.DOUBLE), COS), true);
        checkEquals(function, DoubleFunction.create(ConstantExpression.create(2d, QueryDataType.DOUBLE), COS), false);
        checkEquals(function, DoubleFunction.create(ConstantExpression.create(1d, QueryDataType.DOUBLE), SIN), false);
    }

    @Test
    public void testSerialization() {
        DoubleFunction original = DoubleFunction.create(ConstantExpression.create(1d, QueryDataType.DOUBLE), COS);
        DoubleFunction restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_DOUBLE);

        checkEquals(original, restored, true);
    }

    private static final class FunctionInfo {

        private final String name;

        private FunctionInfo(String name) {
            this.name = name;
        }

        public Double process(Double arg) {
            if (arg == null) {
                return null;
            }

            switch (name) {
                case "COS":
                    return Math.cos(arg);

                case "SIN":
                    return Math.sin(arg);

                case "TAN":
                    return Math.tan(arg);

                case "COT":
                    return 1.0d / Math.tan(arg);

                case "ACOS":
                    return Math.acos(arg);

                case "ASIN":
                    return Math.asin(arg);

                case "ATAN":
                    return Math.atan(arg);

                case "EXP":
                    return Math.exp(arg);

                case "LN":
                    return Math.log(arg);

                case "LOG10":
                    return Math.log10(arg);

                case "DEGREES":
                    return Math.toDegrees(arg);

                case "RADIANS":
                    return Math.toRadians(arg);

                case "SQUARE":
                    return arg * arg;

                case "SQRT":
                    return Math.sqrt(arg);

                case "CBRT":
                    return Math.cbrt(arg);

                default:
                    throw new UnsupportedOperationException("Unsupported function: " + name);
            }
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
