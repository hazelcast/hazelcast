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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.SqlException;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({"unchecked", "rawtypes"})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DoubleFunctionIntegrationTest extends SqlTestSupport {

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
    private HazelcastInstance member;
    private IMap map;

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

    @Before
    public void before() {
        member = factory.newHazelcastInstance();

        map = member.getMap("map");
    }

    @After
    public void after() {
        factory.shutdownAll();
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

        map.clear();
        map.put(0, new ExpressionValue.IntegerVal());
        assertNull(execute("field1"));

        checkColumnFailure("bad", SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL");
        checkColumnFailure('b', SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL");
        checkColumnFailure(new ExpressionValue.ObjectVal(), SqlErrorCode.PARSING, "Cannot apply '" + mode.mode + "' to arguments of type '" + mode.mode + "(<OBJECT>)'");
    }

    private void checkColumn(Object value, double expectedArgument) {
        map.clear();
        map.put(0, value);

        double res = execute("this");
        assertEquals(res, mode.process(expectedArgument), 0.0d);
    }

    private void checkColumnFailure(Object value, int expectedErrorCode, String expectedErrorMessage) {
        map.clear();
        map.put(0, value);

        checkFailure("this", expectedErrorCode, expectedErrorMessage);
    }

    @Test
    public void testParameter() {
        map.put(0, 0);

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

        assertNull(execute("?", new Object[] { null }));

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to DECIMAL", "bad");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to DECIMAL", 'b');
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from OBJECT to DECIMAL", new ExpressionValue.ObjectVal());
    }

    private void checkParameter(Object param, double expectedArgument) {
        double res = execute("?", param);
        assertEquals(res, mode.process(expectedArgument), 0.0d);
    }

    @Test
    public void testLiteral() {
        map.put(0, 0);

        checkLiteral(0, 0d);
        checkLiteral("1.1", 1.1d);
        checkLiteral("'1.1'", 1.1d);

        checkLiteral("null", null);

        checkFailure("'bad'", SqlErrorCode.PARSING, "Literal ''bad'' can not be parsed to type 'DECIMAL'");
    }

    private void checkLiteral(Object literal, Double expectedArg) {
        String literalString = literal.toString();

        Double res1 = execute(literalString);
        assertEquals(res1, mode.process(expectedArg));
    }

    private Double execute(Object operand, Object... params) {
        String sql = "SELECT " + mode.mode + "(" + operand + ") FROM map";

        List<SqlRow> rows = execute(member, sql, params);
        assertEquals(1, rows.size());

        SqlRow row = rows.get(0);
        assertEquals(1, row.getMetadata().getColumnCount());
        assertEquals(SqlColumnType.DOUBLE, row.getMetadata().getColumn(0).getType());

        return row.getObject(0);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT " + mode.mode + "(" + operand + ") FROM map";

        try {
            execute(member, sql, params);

            fail("Must fail");
        } catch (SqlException e) {
            assertTrue(expectedErrorMessage.length() != 0);
            assertNotNull(e.getMessage());
            assertTrue(e.getMessage(),  e.getMessage().contains(expectedErrorMessage));

            assertEquals(expectedErrorCode, e.getCode());
        }
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
