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

package com.hazelcast.sql.impl.expression.predicate;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.SqlExpressionIntegrationTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for NOT predicate.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NotPredicateIntegrationTest extends SqlExpressionIntegrationTestSupport {
    @Test
    public void test_column() {
        // Check boolean values
        checkColumn(true, false);
        checkColumn(false, true);

        // Check null
        put(new ExpressionValue.BooleanVal());
        check("field1", null);

        // Check string
        checkColumnFailure("true", SqlErrorCode.PARSING, "Cannot apply [VARCHAR] to the 'NOT' operator (consider adding an explicit CAST)");
        checkColumnFailure("false", SqlErrorCode.PARSING, "Cannot apply [VARCHAR] to the 'NOT' operator (consider adding an explicit CAST)");
        checkColumnFailure("bad", SqlErrorCode.PARSING, "Cannot apply [VARCHAR] to the 'NOT' operator (consider adding an explicit CAST)");
        checkColumnFailure('b', SqlErrorCode.PARSING, "Cannot apply [VARCHAR] to the 'NOT' operator (consider adding an explicit CAST)");

        // Check unsupported values
        checkColumnFailure((byte) 1, SqlErrorCode.PARSING, "Cannot apply [TINYINT] to the 'NOT' operator (consider adding an explicit CAST)");
        checkColumnFailure((short) 1, SqlErrorCode.PARSING, "Cannot apply [SMALLINT] to the 'NOT' operator (consider adding an explicit CAST)");
        checkColumnFailure(1, SqlErrorCode.PARSING, "Cannot apply [INTEGER] to the 'NOT' operator (consider adding an explicit CAST)");
        checkColumnFailure(1L, SqlErrorCode.PARSING, "Cannot apply [BIGINT] to the 'NOT' operator (consider adding an explicit CAST)");
        checkColumnFailure(BigInteger.ONE, SqlErrorCode.PARSING, "Cannot apply [DECIMAL] to the 'NOT' operator (consider adding an explicit CAST)");
        checkColumnFailure(BigDecimal.ONE, SqlErrorCode.PARSING, "Cannot apply [DECIMAL] to the 'NOT' operator (consider adding an explicit CAST)");
        checkColumnFailure(1f, SqlErrorCode.PARSING, "Cannot apply [REAL] to the 'NOT' operator (consider adding an explicit CAST)");
        checkColumnFailure(1d, SqlErrorCode.PARSING, "Cannot apply [DOUBLE] to the 'NOT' operator (consider adding an explicit CAST)");
        checkColumnFailure(LOCAL_DATE_VAL, SqlErrorCode.PARSING, "Cannot apply [DATE] to the 'NOT' operator (consider adding an explicit CAST)");
        checkColumnFailure(LOCAL_TIME_VAL, SqlErrorCode.PARSING, "Cannot apply [TIME] to the 'NOT' operator (consider adding an explicit CAST)");
        checkColumnFailure(LOCAL_DATE_TIME_VAL, SqlErrorCode.PARSING, "Cannot apply [TIMESTAMP] to the 'NOT' operator (consider adding an explicit CAST)");
        checkColumnFailure(OFFSET_DATE_TIME_VAL, SqlErrorCode.PARSING, "Cannot apply [TIMESTAMP_WITH_TIME_ZONE] to the 'NOT' operator (consider adding an explicit CAST)");

        put(new ExpressionValue.ObjectVal());
        checkFailure("field1", SqlErrorCode.PARSING, "Cannot apply [OBJECT] to the 'NOT' operator (consider adding an explicit CAST)");
    }

    @Test
    public void test_parameter() {
        put(1);

        check("?", null, new Object[] { null });

        check("?", false, true);
        check("?", true, false);

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", "false");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", "true");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", "bad");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", 'b');
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", (byte) 0);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", (short) 0);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", 0);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", 0L);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", BigInteger.ZERO);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", BigDecimal.ZERO);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", 0f);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", 0d);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", LOCAL_DATE_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", LOCAL_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", LOCAL_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", OFFSET_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Parameter at position 0 must be of BOOLEAN type", new ExpressionValue.ObjectVal());
    }

    @Test
    public void test_literal() {
        put(1);

        check("true", false);
        check("false", true);
        check("null", null);

        checkFailure("'true'", SqlErrorCode.PARSING, "Cannot apply [VARCHAR] to the 'NOT' operator (consider adding an explicit CAST)");
        checkFailure("'false'", SqlErrorCode.PARSING, "Cannot apply [VARCHAR] to the 'NOT' operator (consider adding an explicit CAST)");
        checkFailure("'bad'", SqlErrorCode.PARSING, "Cannot apply [VARCHAR] to the 'NOT' operator (consider adding an explicit CAST)");
        checkFailure("1", SqlErrorCode.PARSING, "Cannot apply [TINYINT] to the 'NOT' operator (consider adding an explicit CAST)");
        checkFailure("1E0", SqlErrorCode.PARSING, "Cannot apply [DOUBLE] to the 'NOT' operator (consider adding an explicit CAST)");
    }

    private void checkColumn(Object value, Boolean expectedResult) {
        put(value);

        check("this", expectedResult);
    }

    private void checkColumnFailure(Object value, int expectedErrorCode, String expectedErrorMessage) {
        put(value);

        checkFailure("this", expectedErrorCode, expectedErrorMessage);
    }

    private void checkFailure(String operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT NOT " + operand + " FROM map";

        try {
            execute(member, sql, params);

            fail("Must fail!");
        } catch (HazelcastSqlException e) {
            assertTrue(expectedErrorMessage != null && !expectedErrorMessage.isEmpty());
            assertTrue(e.getMessage(), e.getMessage().contains(expectedErrorMessage));
            assertEquals(expectedErrorCode, e.getCode());
        }
    }

    private void check(String operand, Boolean expectedResult, Object... params) {
        String sql = "SELECT NOT " + operand + " FROM map";

        List<SqlRow> rows = execute(member, sql, params);
        assertEquals(1, rows.size());

        SqlRow row = rows.get(0);
        assertEquals(1, row.getMetadata().getColumnCount());
        assertEquals(SqlColumnType.BOOLEAN, row.getMetadata().getColumn(0).getType());
        assertEquals(expectedResult, row.getObject(0));
    }
}
