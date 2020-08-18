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
import com.hazelcast.sql.SqlRow;
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
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RandFunctionIntegrationTest extends SqlExpressionIntegrationTestSupport {
    @Test
    public void testNoArg() {
        put(0);

        double res1 = checkValue("", SKIP_VALUE_CHECK);
        double res2 = checkValue("", SKIP_VALUE_CHECK);

        assertNotEquals(res1, res2);
    }

    @Test
    public void testMultipleCallsInSingleQuery() {
        putAll(0, 1, 2, 3);

        List<SqlRow> rows = execute(member, "SELECT RAND(), RAND() FROM map");
        assertEquals(4, rows.size());

        Set<Double> values = new HashSet<>();

        for (SqlRow row : rows) {
            double value1 = row.getObject(0);
            double value2 = row.getObject(1);

            assertNotEquals(value1, value2);

            values.add(value1);
        }

        assertTrue(values.size() > 1);
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

        put(new ExpressionValue.IntegerVal());
        double nullRes1 = checkValue("field1", SKIP_VALUE_CHECK);
        double nullRes2 = checkValue("field1", SKIP_VALUE_CHECK);
        assertNotEquals(nullRes1, nullRes2);

        checkColumnFailure("bad", SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL");
        checkColumnFailure('b', SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to DECIMAL");
        checkColumnFailure(new ExpressionValue.ObjectVal(), SqlErrorCode.PARSING, "Cannot apply 'RAND' to arguments of type 'RAND(<OBJECT>)'");
    }

    private void checkColumn(Object value, long expectedSeed) {
        put(value);

        checkValue("this", new Random(expectedSeed).nextDouble());
    }

    private void checkColumnFailure(Object value, int expectedErrorCode, String expectedErrorMessage) {
        put(value);

        checkFailure("this", expectedErrorCode, expectedErrorMessage);
    }

    @Test
    public void testParameter() {
        put(0);

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

        double nullRes1 = checkValue("?", SKIP_VALUE_CHECK, new Object[] { null });
        double nullRes2 = checkValue("?", SKIP_VALUE_CHECK, new Object[] { null });
        assertNotEquals(nullRes1, nullRes2);

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to BIGINT", "bad");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to BIGINT", 'b');
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from OBJECT to BIGINT", new ExpressionValue.ObjectVal());
    }

    private void checkParameter(Object param, long expectedSeed) {
        checkValue("?", new Random(expectedSeed).nextDouble(), param);
    }

    @Test
    public void testLiteral() {
        put(0);

        checkNumericLiteral(0, 0L);
        checkNumericLiteral(Long.MAX_VALUE, Long.MAX_VALUE);
        checkNumericLiteral("1.1", 1L);

        double nullRes1 = checkValue("null", SKIP_VALUE_CHECK);
        double nullRes2 = checkValue("null", SKIP_VALUE_CHECK);
        assertNotEquals(nullRes1, nullRes2);

        checkFailure("'bad'", SqlErrorCode.PARSING, "Literal ''bad'' can not be parsed to type 'DECIMAL'");
    }

    private void checkNumericLiteral(Object literal, long expectedSeed) {
        String literalString = literal.toString();

        checkValue(literalString, new Random(expectedSeed).nextDouble());
        checkValue("'" +  literalString + "'", new Random(expectedSeed).nextDouble());
    }

    private Double checkValue(Object operand, Object expectedValue, Object... params) {
        String sql = "SELECT RAND(" + operand + ") FROM map";

        return (Double) checkValueInternal(sql, SqlColumnType.DOUBLE, expectedValue, params);
    }

    private void checkFailure(Object operand, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT RAND(" + operand + ") FROM map";

        checkFailureInternal(sql, expectedErrorCode, expectedErrorMessage, params);
    }
}
