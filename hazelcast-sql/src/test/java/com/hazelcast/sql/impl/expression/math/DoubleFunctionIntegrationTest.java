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
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.expression.SqlExpressionIntegrationTestSupport;
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

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DoubleFunctionIntegrationTest extends SqlExpressionIntegrationTestSupport {

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
        checkColumn((byte) 1, 1d);
        checkColumn((short) 1, 1d);
        checkColumn(1, 1d);
        checkColumn(1L, 1d);
        checkColumn(1f, 1d);
        checkColumn(1d, 1d);
        checkColumn(BigInteger.ONE, 1d);
        checkColumn(new BigDecimal("1.1"), 1.1d);

        checkColumn("1", 1d);
        checkColumn('1', 1d);

        put(new ExpressionValue.IntegerVal());
        checkValue("field1", null);

        checkColumnFailure("bad", SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL");
        checkColumnFailure('b', SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL");
        checkColumnFailure(new ExpressionValue.ObjectVal(), SqlErrorCode.PARSING, "Cannot apply '" + mode.mode + "' to arguments of type '" + mode.mode + "(<OBJECT>)'");
    }

    private void checkColumn(Object value, double expectedArgument) {
        put(value);

        checkValue("this", mode.process(expectedArgument));
    }

    private void checkColumnFailure(Object value, int expectedErrorCode, String expectedErrorMessage) {
        put(value);

        checkFailure("this", expectedErrorCode, expectedErrorMessage);
    }

    @Test
    public void testParameter() {
        put(0);

        checkParameter((byte) 1, 1d);
        checkParameter((short) 1, 1d);
        checkParameter(1, 1d);
        checkParameter(1L, 1d);
        checkParameter(1f, 1d);
        checkParameter(1d, 1d);
        checkParameter(BigInteger.ONE, 1d);
        checkParameter(new BigDecimal("1.1"), 1.1d);

        checkParameter("1.1", 1.1d);
        checkParameter('1', 1d);

        checkValue("?", null, new Object[] { null });

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to DOUBLE", "bad");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to DOUBLE", 'b');
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from OBJECT to DOUBLE", new ExpressionValue.ObjectVal());
    }

    private void checkParameter(Object param, double expectedArgument) {
        checkValue("?", mode.process(expectedArgument), param);
    }

    @Test
    public void testLiteral() {
        put(0);

        checkLiteral(0, 0d);
        checkLiteral("1.1", 1.1d);
        checkLiteral("'1.1'", 1.1d);

        checkLiteral("null", null);

        checkFailure("'bad'", SqlErrorCode.PARSING, "Literal ''bad'' can not be parsed to type 'DECIMAL'");
    }

    private void checkLiteral(Object literal, Double expectedArg) {
        String literalString = literal.toString();

        checkValue(literalString, mode.process(expectedArg));
    }

    private void checkValue(Object operand, Object expectedValue, Object... params) {
        String sql = "SELECT " + mode.mode + "(" + operand + ") FROM map";

        checkValueInternal(sql, SqlColumnType.DOUBLE, expectedValue, params);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT " + mode.mode + "(" + operand + ") FROM map";

        checkFailureInternal(sql, expectedErrorCode, expectedErrorMessage, params);
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
