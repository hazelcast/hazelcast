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
import com.hazelcast.sql.impl.SqlErrorCode;
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

        put(new ExpressionValue.IntegerVal());
        double nullRes1 = checkValue("field1", SKIP_VALUE_CHECK);
        double nullRes2 = checkValue("field1", SKIP_VALUE_CHECK);
        assertNotEquals(nullRes1, nullRes2);

        checkColumnFailure(true, SqlErrorCode.PARSING, "Cannot apply [BOOLEAN] to the 'RAND' function (consider adding an explicit CAST)");
        checkColumnFailure(1f, SqlErrorCode.PARSING, "Cannot apply [REAL] to the 'RAND' function (consider adding an explicit CAST)");
        checkColumnFailure(1d, SqlErrorCode.PARSING, "Cannot apply [DOUBLE] to the 'RAND' function (consider adding an explicit CAST)");
        checkColumnFailure(BigInteger.ONE, SqlErrorCode.PARSING, "Cannot apply [DECIMAL] to the 'RAND' function (consider adding an explicit CAST)");
        checkColumnFailure(BigDecimal.ONE, SqlErrorCode.PARSING, "Cannot apply [DECIMAL] to the 'RAND' function (consider adding an explicit CAST)");
        checkColumnFailure("1", SqlErrorCode.PARSING, "Cannot apply [VARCHAR] to the 'RAND' function (consider adding an explicit CAST)");
        checkColumnFailure('1', SqlErrorCode.PARSING, "Cannot apply [VARCHAR] to the 'RAND' function (consider adding an explicit CAST)");
        checkColumnFailure(LOCAL_DATE_VAL, SqlErrorCode.PARSING, "Cannot apply [DATE] to the 'RAND' function (consider adding an explicit CAST)");
        checkColumnFailure(LOCAL_TIME_VAL, SqlErrorCode.PARSING, "Cannot apply [TIME] to the 'RAND' function (consider adding an explicit CAST)");
        checkColumnFailure(LOCAL_DATE_TIME_VAL, SqlErrorCode.PARSING, "Cannot apply [TIMESTAMP] to the 'RAND' function (consider adding an explicit CAST)");
        checkColumnFailure(OFFSET_DATE_TIME_VAL, SqlErrorCode.PARSING, "Cannot apply [TIMESTAMP_WITH_TIME_ZONE] to the 'RAND' function (consider adding an explicit CAST)");
        checkColumnFailure(new ExpressionValue.ObjectVal(), SqlErrorCode.PARSING, "Cannot apply [OBJECT] to the 'RAND' function (consider adding an explicit CAST)");
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

        double nullRes1 = checkValue("?", SKIP_VALUE_CHECK, new Object[] { null });
        double nullRes2 = checkValue("?", SKIP_VALUE_CHECK, new Object[] { null });
        assertNotEquals(nullRes1, nullRes2);

        checkParameter((byte) 1, 1L);
        checkParameter((short) 1, 1L);
        checkParameter(1, 1L);
        checkParameter(1L, 1L);

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from BOOLEAN to BIGINT", true);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to BIGINT", BigInteger.ONE);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to BIGINT", BigDecimal.ONE);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from REAL to BIGINT", 0.0f);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DOUBLE to BIGINT", 0.0d);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from VARCHAR to BIGINT", "1");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from VARCHAR to BIGINT", '1');
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DATE to BIGINT", LOCAL_DATE_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIME to BIGINT", LOCAL_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP to BIGINT", LOCAL_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP_WITH_TIME_ZONE to BIGINT", OFFSET_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from OBJECT to BIGINT", new ExpressionValue.ObjectVal());
    }

    private void checkParameter(Object param, long expectedSeed) {
        checkValue("?", new Random(expectedSeed).nextDouble(), param);
    }

    @Test
    public void testLiteral() {
        put(0);

        checkValue(0, new Random(0).nextDouble());
        checkValue(Long.MAX_VALUE, new Random(Long.MAX_VALUE).nextDouble());

        double nullRes1 = checkValue("null", SKIP_VALUE_CHECK);
        double nullRes2 = checkValue("null", SKIP_VALUE_CHECK);
        assertNotEquals(nullRes1, nullRes2);

        checkFailure("true", SqlErrorCode.PARSING, "Cannot apply [BOOLEAN] to the 'RAND' function (consider adding an explicit CAST)");
        checkFailure("1.1", SqlErrorCode.PARSING, "Cannot apply [DECIMAL] to the 'RAND' function (consider adding an explicit CAST)");
        checkFailure("1.1E1", SqlErrorCode.PARSING, "Cannot apply [DOUBLE] to the 'RAND' function (consider adding an explicit CAST)");
        checkFailure("'bad'", SqlErrorCode.PARSING, "Cannot apply [VARCHAR] to the 'RAND' function (consider adding an explicit CAST)");
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
