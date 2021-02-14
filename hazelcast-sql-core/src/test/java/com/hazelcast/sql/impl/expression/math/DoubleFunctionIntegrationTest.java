/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
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

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DoubleFunctionIntegrationTest extends ExpressionTestSupport {
    @Parameterized.Parameter
    public Mode mode;

    @Parameterized.Parameters(name = "mode: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
            { new Mode("COS") },
            { new Mode("SIN") },
            { new Mode("TAN") },
            { new Mode("COT") },
            { new Mode("ACOS") },
            { new Mode("ASIN") },
            { new Mode("ATAN") },
            { new Mode("EXP") },
            { new Mode("LN") },
            { new Mode("LOG10") },
            { new Mode("DEGREES") },
            { new Mode("RADIANS") },
        });
    }

    @Test
    public void testColumn() {
        // NULL
        putAndCheckValue(new ExpressionValue.DoubleVal(), sql("field1"), DOUBLE, null);

        // Numeric
        putAndCheckValue((byte) 1, sql("this"), DOUBLE, mode.process(1d));
        putAndCheckValue((short) 1, sql("this"), DOUBLE, mode.process(1d));
        putAndCheckValue(1, sql("this"), DOUBLE, mode.process(1d));
        putAndCheckValue(1L, sql("this"), DOUBLE, mode.process(1d));
        putAndCheckValue(1L, sql("this"), DOUBLE, mode.process(1d));
        putAndCheckValue(BigInteger.ONE, sql("this"), DOUBLE, mode.process(1d));
        putAndCheckValue(BigDecimal.ONE, sql("this"), DOUBLE, mode.process(1d));
        putAndCheckValue(1f, sql("this"), DOUBLE, mode.process(1d));
        putAndCheckValue(1d, sql("this"), DOUBLE, mode.process(1d));

        // Other
        putAndCheckFailure('1', sql("this"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, VARCHAR));
        putAndCheckFailure("1", sql("this"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, VARCHAR));
        putAndCheckFailure(true, sql("this"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, BOOLEAN));
        putAndCheckFailure(LOCAL_DATE_VAL, sql("this"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, DATE));
        putAndCheckFailure(LOCAL_TIME_VAL, sql("this"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, TIME));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql("this"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, TIMESTAMP));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql("this"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(OBJECT_VAL, sql("this"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, OBJECT));
    }

    @Test
    public void testLiteral() {
        put(0);

        checkValue0(sql("null"), DOUBLE, null);
        checkValue0(sql("1"), DOUBLE, mode.process(1d));
        checkValue0(sql("1.0"), DOUBLE, mode.process(1d));
        checkValue0(sql("1E0"), DOUBLE, mode.process(1d));

        checkFailure0(sql("true"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, BOOLEAN));
        checkFailure0(sql("'1'"), SqlErrorCode.PARSING, signatureErrorFunction(mode.mode, VARCHAR));
    }

    @Test
    public void testParameter() {
        put(0);

        checkValue0(sql("?"), DOUBLE, null, new Object[] { null });

        checkValue0(sql("?"), DOUBLE, mode.process(1d), (byte) 1);
        checkValue0(sql("?"), DOUBLE, mode.process(1d), (short) 1);
        checkValue0(sql("?"), DOUBLE, mode.process(1d), 1);
        checkValue0(sql("?"), DOUBLE, mode.process(1d), 1L);
        checkValue0(sql("?"), DOUBLE, mode.process(1d), BigInteger.ONE);
        checkValue0(sql("?"), DOUBLE, mode.process(1d), BigDecimal.ONE);
        checkValue0(sql("?"), DOUBLE, mode.process(1d), 1f);
        checkValue0(sql("?"), DOUBLE, mode.process(1d), 1d);

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
        return "SELECT " + mode.mode + "(" + operand + ") FROM map";
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

    private static final class Mode {

        private final String mode;

        private Mode(String mode) {
            this.mode = mode;
        }

        public Double process(Double arg) {
            if (arg == null) {
                return null;
            }

            switch (mode) {
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

                default:
                    throw new UnsupportedOperationException("Unsupported mode: " + mode);
            }
        }

        @Override
        public String toString() {
            return mode;
        }
    }
}
