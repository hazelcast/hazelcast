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
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({"unchecked", "rawtypes"})
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RandFunctionIntegrationTest extends SqlTestSupport {

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);
    private HazelcastInstance member;
    private IMap map;

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
    public void testNoArg() {
        map.put(0, 0);

        double res1 = execute("");
        double res2 = execute("");

        assertNotEquals(res1, res2);
    }

    @Test
    public void testColumn() {
        checkColumn((byte) 1, 1L);
        checkColumn((short) 1, 1L);
        checkColumn(1, 1L);
        checkColumn(1L, 1L);
        checkColumn(1f, 1L);
        checkColumn(1d, 1L);
        checkColumn(BigInteger.ONE, 1L);
        checkColumn(BigDecimal.ONE, 1L);

        checkColumn("1", 1L);
        checkColumn('1', 1L);

        map.clear();
        map.put(0, new ExpressionValue.IntegerVal());
        double nullRes1 = execute("field1");
        double nullRes2 = execute("field1");
        assertNotEquals(nullRes1, nullRes2);

        checkColumnFailure("bad", SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL");
        checkColumnFailure('b', SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL");
        checkColumnFailure(new ExpressionValue.ObjectVal(), SqlErrorCode.PARSING, "Cannot apply 'RAND' to arguments of type 'RAND(<OBJECT>)'");
    }

    private void checkColumn(Object value, long expectedSeed) {
        map.clear();
        map.put(0, value);

        double res1 = execute("this");
        assertEquals(res1, new Random(expectedSeed).nextDouble(), 0.0d);
    }

    private void checkColumnFailure(Object value, int expectedErrorCode, String expectedErrorMessage) {
        map.clear();
        map.put(0, value);

        checkFailure("this", expectedErrorCode, expectedErrorMessage);
    }

    @Test
    public void testParameter() {
        map.put(0, 0);

        checkParameter((byte) 1, 1L);
        checkParameter((short) 1, 1L);
        checkParameter(1, 1L);
        checkParameter(1L, 1L);
        checkParameter(1f, 1L);
        checkParameter(1d, 1L);
        checkParameter(BigInteger.ONE, 1L);
        checkParameter(BigDecimal.ONE, 1L);

        checkParameter("1", 1L);
        checkParameter('1', 1L);

        double nullRes1 = execute("?", new Object[] { null });
        double nullRes2 = execute("?", new Object[] { null });
        assertNotEquals(nullRes1, nullRes2);

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to DECIMAL", "bad");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to DECIMAL", 'b');
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from OBJECT to DECIMAL", new ExpressionValue.ObjectVal());
    }

    private void checkParameter(Object param, long expectedSeed) {
        double res = execute("?", param);
        assertEquals(res, new Random(expectedSeed).nextDouble(), 0.0d);
    }

    @Test
    public void testLiteral() {
        map.put(0, 0);

        checkNumericLiteral(0, 0L);
        checkNumericLiteral(Long.MAX_VALUE, Long.MAX_VALUE);
        checkNumericLiteral("1.1", 1L);

        double nullRes1 = execute("null");
        double nullRes2 = execute("null");
        assertNotEquals(nullRes1, nullRes2);

        checkFailure("'bad'", SqlErrorCode.PARSING, "Literal ''bad'' can not be parsed to type 'DECIMAL'");
    }

    private void checkNumericLiteral(Object literal, long expectedSeed) {
        String literalString = literal.toString();

        double res1 = execute(literalString);
        assertEquals(res1, new Random(expectedSeed).nextDouble(), 0.0d);

        double res2 = execute("'" +  literalString + "'");
        assertEquals(res2, new Random(expectedSeed).nextDouble(), 0.0d);
    }

    private Double execute(Object operand, Object... params) {
        String sql = "SELECT RAND(" + operand + ") FROM map";

        List<SqlRow> rows = execute(member, sql, params);
        assertEquals(1, rows.size());

        SqlRow row = rows.get(0);
        assertEquals(1, row.getMetadata().getColumnCount());
        assertEquals(SqlColumnType.DOUBLE, row.getMetadata().getColumn(0).getType());

        return row.getObject(0);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT RAND(" + operand + ") FROM map";

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
}
