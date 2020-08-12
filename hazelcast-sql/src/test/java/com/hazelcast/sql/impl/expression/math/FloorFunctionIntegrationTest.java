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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({"unchecked", "rawtypes"})
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class FloorFunctionIntegrationTest extends SqlTestSupport {

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
    public void testColumn() {
        checkColumn((byte) 1, SqlColumnType.TINYINT, (byte) 1);
        checkColumn((short) 1, SqlColumnType.SMALLINT, (short) 1);
        checkColumn(1, SqlColumnType.INTEGER, 1);
        checkColumn(1L, SqlColumnType.BIGINT, 1L);
        checkColumn(1.1f, SqlColumnType.REAL, 1f);
        checkColumn(1.1d, SqlColumnType.DOUBLE, 1d);
        checkColumn(BigInteger.ONE, SqlColumnType.DECIMAL, BigDecimal.ONE);
        checkColumn(new BigDecimal("1.1"), SqlColumnType.DECIMAL, BigDecimal.ONE);

        checkColumn("1.1", SqlColumnType.DECIMAL, BigDecimal.ONE);
        checkColumn('1', SqlColumnType.DECIMAL, BigDecimal.ONE);

        checkColumn(Float.POSITIVE_INFINITY, SqlColumnType.REAL, Float.POSITIVE_INFINITY);
        checkColumn(Float.NEGATIVE_INFINITY, SqlColumnType.REAL, Float.NEGATIVE_INFINITY);
        checkColumn(Float.NaN, SqlColumnType.REAL, Float.NaN);

        checkColumn(Double.POSITIVE_INFINITY, SqlColumnType.DOUBLE, Double.POSITIVE_INFINITY);
        checkColumn(Double.NEGATIVE_INFINITY, SqlColumnType.DOUBLE, Double.NEGATIVE_INFINITY);
        checkColumn(Double.NaN, SqlColumnType.DOUBLE, Double.NaN);

        map.clear();
        map.put(0, new ExpressionValue.IntegerVal());
        assertNull(execute("field1", SqlColumnType.INTEGER));

        checkColumnFailure("bad", SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL");
        checkColumnFailure('b', SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL");
        checkColumnFailure(new ExpressionValue.ObjectVal(), SqlErrorCode.PARSING, "Cannot apply 'FLOOR' to arguments of type 'FLOOR(<OBJECT>)'");
    }

    private void checkColumn(Object value, SqlColumnType expectedType, Object expectedResult) {
        map.clear();
        map.put(0, value);

        Object res = execute("this", expectedType);
        assertEquals(expectedResult, res);
    }

    private void checkColumnFailure(Object value, int expectedErrorCode, String expectedErrorMessage) {
        map.clear();
        map.put(0, value);

        checkFailure("this", expectedErrorCode, expectedErrorMessage);
    }

    @Test
    public void testParameter() {
        map.put(0, 0);

        checkParameter((byte) 1, BigDecimal.ONE);
        checkParameter((short) 1, BigDecimal.ONE);
        checkParameter(1, BigDecimal.ONE);
        checkParameter(1L, BigDecimal.ONE);
        checkParameter(1.1f, BigDecimal.ONE);
        checkParameter(1.1d, BigDecimal.ONE);
        checkParameter(BigInteger.ONE, BigDecimal.ONE);
        checkParameter(new BigDecimal("1.1"), BigDecimal.ONE);

        checkParameter("1.1", BigDecimal.ONE);
        checkParameter('1', BigDecimal.ONE);

        assertNull(execute("?", SqlColumnType.DECIMAL, new Object[] { null }));

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to DECIMAL", "bad");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to DECIMAL", 'b');
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from OBJECT to DECIMAL", new ExpressionValue.ObjectVal());
    }

    private void checkParameter(Object param, Object expectedResult) {
        Object res = execute("?", SqlColumnType.DECIMAL, param);
        assertEquals(res, expectedResult);
    }

    @Test
    public void testLiteral() {
        map.put(0, 0);

        checkLiteral(1, SqlColumnType.TINYINT, (byte) 1);

        checkLiteral("null", SqlColumnType.DECIMAL, null);
        checkLiteral("1.1", SqlColumnType.DECIMAL, new BigDecimal("1"));
        checkLiteral("1.1E0", SqlColumnType.DOUBLE, 1d);

        checkFailure("'bad'", SqlErrorCode.PARSING, "Literal ''bad'' can not be parsed to type 'DECIMAL'");
    }

    private void checkLiteral(Object literal, SqlColumnType expectedType, Object expectedResult) {
        String literalString = literal.toString();

        Object res1 = execute(literalString, expectedType);
        assertEquals(res1, expectedResult);
    }

    private Object execute(Object operand, SqlColumnType expectedType, Object... params) {
        String sql = "SELECT FLOOR(" + operand + ") FROM map";

        List<SqlRow> rows = execute(member, sql, params);
        assertEquals(1, rows.size());

        SqlRow row = rows.get(0);
        assertEquals(1, row.getMetadata().getColumnCount());
        assertEquals(expectedType, row.getMetadata().getColumn(0).getType());

        return row.getObject(0);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT FLOOR(" + operand + ") FROM map";

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
