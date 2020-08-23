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

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.expression.SqlExpressionIntegrationTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionType;
import com.hazelcast.sql.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.sql.support.expressions.ExpressionTypes.BIG_DECIMAL;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.BIG_INTEGER;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.BOOLEAN;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.BYTE;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.CHARACTER;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.DOUBLE;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.FLOAT;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.INTEGER;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.LOCAL_DATE;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.LOCAL_DATE_TIME;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.LOCAL_TIME;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.LONG;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.OBJECT;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.OFFSET_DATE_TIME;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.STRING;
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
        checkColumn("true", false);
        checkColumn("false", true);

        checkColumnFailure("bad", SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to BOOLEAN");
        checkColumnFailure('b', SqlErrorCode.DATA_EXCEPTION, "Cannot convert VARCHAR to BOOLEAN");

        // Check unsupported values
        checkColumnFailure((byte) 1, SqlErrorCode.PARSING, "Cannot apply 'NOT' to arguments of type 'NOT<TINYINT>'");
        checkColumnFailure((short) 1, SqlErrorCode.PARSING, "Cannot apply 'NOT' to arguments of type 'NOT<SMALLINT>'");
        checkColumnFailure(1, SqlErrorCode.PARSING, "Cannot apply 'NOT' to arguments of type 'NOT<INTEGER>'");
        checkColumnFailure(1L, SqlErrorCode.PARSING, "Cannot apply 'NOT' to arguments of type 'NOT<BIGINT>'");
        checkColumnFailure(BigInteger.ONE, SqlErrorCode.PARSING, "Cannot apply 'NOT' to arguments of type 'NOT<DECIMAL(38, 38)>'");
        checkColumnFailure(BigDecimal.ONE, SqlErrorCode.PARSING, "Cannot apply 'NOT' to arguments of type 'NOT<DECIMAL(38, 38)>'");
        checkColumnFailure(1f, SqlErrorCode.PARSING, "Cannot apply 'NOT' to arguments of type 'NOT<REAL>'");
        checkColumnFailure(1d, SqlErrorCode.PARSING, "Cannot apply 'NOT' to arguments of type 'NOT<DOUBLE>'");

        checkColumnFailure(LOCAL_DATE_VAL, SqlErrorCode.PARSING, "Cannot apply 'NOT' to arguments of type 'NOT<DATE>'");
        checkColumnFailure(LOCAL_TIME_VAL, SqlErrorCode.PARSING, "Cannot apply 'NOT' to arguments of type 'NOT<TIME>'");
        checkColumnFailure(LOCAL_DATE_TIME_VAL, SqlErrorCode.PARSING, "Cannot apply 'NOT' to arguments of type 'NOT<TIMESTAMP>'");
        checkColumnFailure(OFFSET_DATE_TIME_VAL, SqlErrorCode.PARSING, "Cannot apply 'NOT' to arguments of type 'NOT<TIMESTAMP_WITH_TIME_ZONE>'");

        put(new ExpressionValue.ObjectVal());
        checkFailure("field1", SqlErrorCode.PARSING, "Cannot apply 'NOT' to arguments of type 'NOT<OBJECT>'");
    }

    @Test
    public void test_parameter() {
        put(1);

        check("?", null, new Object[] { null });

        check("?", false, true);
        check("?", true, false);

        check("?", true, "false");
        check("?", false, "true");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to BOOLEAN", "bad");
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Failed to convert parameter at position 0 from VARCHAR to BOOLEAN", 'b');

        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TINYINT to BOOLEAN", (byte) 0);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from SMALLINT to BOOLEAN", (short) 0);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to BOOLEAN", 0);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from BIGINT to BOOLEAN", 0L);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to BOOLEAN", BigInteger.ZERO);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DECIMAL to BOOLEAN", BigDecimal.ZERO);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from REAL to BOOLEAN", 0f);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DOUBLE to BOOLEAN", 0d);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from DATE to BOOLEAN", LOCAL_DATE_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIME to BOOLEAN", LOCAL_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP to BOOLEAN", LOCAL_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from TIMESTAMP_WITH_TIME_ZONE to BOOLEAN", OFFSET_DATE_TIME_VAL);
        checkFailure("?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from OBJECT to BOOLEAN", new ExpressionValue.ObjectVal());
    }

    @Test
    public void test_literal() {
        put(1);

        check("true", false);
        check("false", true);
        check("null", null);

        check("'true'", false);
        check("'false'", true);
        checkFailure("'bad'", SqlErrorCode.PARSING, "Literal ''bad'' can not be parsed to type 'BOOLEAN'");

        checkFailure("1", SqlErrorCode.PARSING, "Cannot apply 'NOT' to arguments of type 'NOT<TINYINT>'");
        checkFailure("1E0", SqlErrorCode.PARSING, "Cannot apply 'NOT' to arguments of type 'NOT<DOUBLE>'");
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

    @Test
    public void testLiteral() {
        Class<? extends ExpressionValue> clazz = ExpressionValue.createClass(INTEGER);

        int key = 0;
        ExpressionValue value = ExpressionValue.create(clazz, 0, 1);

        put(key, value);

        // TRUE literal
        checkLiteral("TRUE", "IS TRUE", true);
        checkLiteral("true", "IS TRUE", true);
        checkLiteral("'TRUE'", "IS TRUE", true);
        checkLiteral("'true'", "IS TRUE", true);

        checkLiteral("TRUE", "IS FALSE", false);

        checkLiteral("true", "IS FALSE", false);
        checkLiteral("'TRUE'", "IS FALSE", false);
        checkLiteral("'true'", "IS FALSE", false);

        checkLiteral("TRUE", "IS NOT TRUE", false);
        checkLiteral("true", "IS NOT TRUE", false);
        checkLiteral("'TRUE'", "IS NOT TRUE", false);
        checkLiteral("'true'", "IS NOT TRUE", false);

        checkLiteral("TRUE", "IS NOT FALSE", true);
        checkLiteral("true", "IS NOT FALSE", true);
        checkLiteral("'TRUE'", "IS NOT FALSE", true);
        checkLiteral("'true'", "IS NOT FALSE", true);

        // False literal
        checkLiteral("FALSE", "IS TRUE", false);
        checkLiteral("false", "IS TRUE", false);
        checkLiteral("'FALSE'", "IS TRUE", false);
        checkLiteral("'false'", "IS TRUE", false);

        checkLiteral("FALSE", "IS FALSE", true);
        checkLiteral("false", "IS FALSE", true);
        checkLiteral("'FALSE'", "IS FALSE", true);
        checkLiteral("'false'", "IS FALSE", true);

        checkLiteral("FALSE", "IS NOT TRUE", true);
        checkLiteral("false", "IS NOT TRUE", true);
        checkLiteral("'FALSE'", "IS NOT TRUE", true);
        checkLiteral("'false'", "IS NOT TRUE", true);

        checkLiteral("FALSE", "IS NOT FALSE", false);
        checkLiteral("false", "IS NOT FALSE", false);
        checkLiteral("'FALSE'", "IS NOT FALSE", false);
        checkLiteral("'false'", "IS NOT FALSE", false);

        // NULL literal
        checkLiteral("NULL", "IS TRUE", false);
        checkLiteral("null", "IS TRUE", false);

        checkLiteral("NULL", "IS FALSE", false);
        checkLiteral("null", "IS FALSE", false);

        checkLiteral("NULL", "IS NOT TRUE", true);
        checkLiteral("null", "IS NOT TRUE", true);

        checkLiteral("NULL", "IS NOT FALSE", true);
        checkLiteral("null", "IS NOT FALSE", true);

        // Bad literal
        checkBadLiteral("IS TRUE");
        checkBadLiteral("IS FALSE");
        checkBadLiteral("IS NOT TRUE");
        checkBadLiteral("IS NOT FALSE");
    }

    private void checkLiteral(String literal, String function, boolean expectedResult) {
        String expression = literal + " " + function;
        String sql = "SELECT " + expression + " FROM map WHERE " + expression;

        List<SqlRow> rows = execute(member, sql);

        if (expectedResult) {
            assertEquals(1, rows.size());

            SqlRow row = rows.get(0);

            assertEquals(SqlColumnType.BOOLEAN, row.getMetadata().getColumn(0).getType());
            assertTrue(row.getObject(0));
        } else {
            assertEquals(0, rows.size());
        }
    }

    private void checkBadLiteral(String function) {
        checkFailureInternal(
            "SELECT * FROM map WHERE 'bad' " + function,
            SqlErrorCode.PARSING,
            "Literal ''bad'' can not be parsed to type 'BOOLEAN'"
        );

        checkFailureInternal(
            "SELECT 'bad' " + function + " FROM map",
            SqlErrorCode.PARSING,
            "Literal ''bad'' can not be parsed to type 'BOOLEAN'"
        );
    }

    @Test
    public void testColumn_boolean() {
        checkColumn(BOOLEAN, true, false);
    }

    @Test
    public void testColumn_string() {
        checkColumn(STRING, "true", "false");
    }

    private void checkColumn(ExpressionType<?> type, Object trueValue, Object falseValue) {
        Class<? extends ExpressionValue> clazz = ExpressionValue.createClass(type);

        int keyTrue = 0;
        int keyFalse = 1;
        int keyNull = 2;

        Map<Integer, Object> entries = new HashMap<>();
        entries.put(keyTrue, ExpressionValue.create(clazz, keyTrue, trueValue));
        entries.put(keyFalse, ExpressionValue.create(clazz, keyFalse, falseValue));
        entries.put(keyNull, ExpressionValue.create(clazz, keyNull, null));
        putAll(entries);

        checkColumn("IS TRUE", set(keyTrue));
        checkColumn("IS FALSE", set(keyFalse));
        checkColumn("IS NOT TRUE", set(keyFalse, keyNull));
        checkColumn("IS NOT FALSE", set(keyTrue, keyNull));
    }

    private void checkColumn(String function, Set<Integer> expectedKeys) {
        String expression = "field1 " + function;
        String sql = "SELECT key, " + expression + " FROM map WHERE " + expression;

        List<SqlRow> rows = execute(member, sql);

        assertEquals(expectedKeys.size(), rows.size());

        for (SqlRow row : rows) {
            assertEquals(SqlColumnType.BOOLEAN, row.getMetadata().getColumn(1).getType());

            int key = row.getObject(0);
            boolean value = row.getObject(1);

            assertTrue("Key is not returned: " + key, expectedKeys.contains(key));
            assertTrue(value);
        }
    }

    @Test
    public void testColumnBad_string() {
        checkColumnBad(STRING, "bad", "VARCHAR");
    }

    @Test
    public void testColumnBad_character() {
        checkColumnBad(CHARACTER, 'a', "VARCHAR");
    }

    private void checkColumnBad(ExpressionType<?> type, Object value, Object expectedFromType) {
        Class<? extends ExpressionValue> clazz = ExpressionValue.createClass(type);

        put(1, ExpressionValue.create(clazz, 1, value));

        checkColumnBad("IS TRUE", expectedFromType);
        checkColumnBad("IS FALSE", expectedFromType);
        checkColumnBad("IS NOT TRUE", expectedFromType);
        checkColumnBad("IS NOT FALSE", expectedFromType);
    }

    private void checkColumnBad(String function, Object expectedFromType) {
        checkFailureInternal(
            "SELECT key FROM map WHERE field1 " + function,
            SqlErrorCode.DATA_EXCEPTION,
            "Cannot convert " + expectedFromType + " to BOOLEAN"
        );
    }

    @Test
    public void testColumn_unsupported() {
        checkUnsupportedColumn(BYTE, "TINYINT");
        checkUnsupportedColumn(INTEGER, "INTEGER");
        checkUnsupportedColumn(LONG, "BIGINT");
        checkUnsupportedColumn(BIG_INTEGER, "DECIMAL(38, 38)");
        checkUnsupportedColumn(BIG_DECIMAL, "DECIMAL(38, 38)");
        checkUnsupportedColumn(FLOAT, "REAL");
        checkUnsupportedColumn(DOUBLE, "DOUBLE");
        checkUnsupportedColumn(LOCAL_DATE, "DATE");
        checkUnsupportedColumn(LOCAL_TIME, "TIME");
        checkUnsupportedColumn(LOCAL_DATE_TIME, "TIMESTAMP");
        checkUnsupportedColumn(OFFSET_DATE_TIME, "TIMESTAMP_WITH_TIME_ZONE");
        checkUnsupportedColumn(OBJECT, "OBJECT");
    }

    private void checkUnsupportedColumn(ExpressionType<?> type, String expectedTypeNameInErrorMessage) {
        checkUnsupportedColumn(type, "IS TRUE", expectedTypeNameInErrorMessage);
        checkUnsupportedColumn(type, "IS FALSE", expectedTypeNameInErrorMessage);
        checkUnsupportedColumn(type, "IS NOT FALSE", expectedTypeNameInErrorMessage);
        checkUnsupportedColumn(type, "IS NOT TRUE", expectedTypeNameInErrorMessage);
    }

    private void checkUnsupportedColumn(ExpressionType<?> type, String function, String expectedTypeNameInErrorMessage) {
        String expectedErrorMessage = "Cannot apply '" + function + "' to arguments of type '<"
            + expectedTypeNameInErrorMessage + "> " + function + "'. Supported form(s): '<BOOLEAN> " + function + "'";

        Class<? extends ExpressionValue> clazz = ExpressionValue.createClass(type);

        int key = 0;
        ExpressionValue value = ExpressionValue.create(clazz, key, type.valueFrom());

        put(key, value);

        // Function in the condition
        checkFailureInternal(
            "SELECT key FROM map WHERE field1 " + function,
            SqlErrorCode.PARSING,
            expectedErrorMessage
        );

        // Function in the column
        checkFailureInternal(
            "SELECT field1 " + function + " FROM map",
            SqlErrorCode.PARSING,
            expectedErrorMessage
        );
    }

    @Test
    public void testParameter() {
        Class<? extends ExpressionValue> clazz = ExpressionValue.createClass(INTEGER);

        int key = 0;
        ExpressionValue value = ExpressionValue.create(clazz, 0, 1);

        put(key, value);

        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS TRUE", true));
        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS TRUE", false));
        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS TRUE", "true"));
        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS TRUE", "false"));
        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS TRUE", new Object[] { null }));

        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS FALSE", true));
        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS FALSE", false));
        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS FALSE", "true"));
        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS FALSE", "false"));
        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS FALSE", new Object[] { null }));

        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS NOT TRUE", true));
        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS NOT TRUE", false));
        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS NOT TRUE", "true"));
        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS NOT TRUE", "false"));
        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS NOT TRUE", new Object[] { null }));

        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS NOT FALSE", true));
        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS NOT FALSE", false));
        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS NOT FALSE", "true"));
        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS NOT FALSE", "false"));
        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS NOT FALSE", new Object[] { null }));

        checkUnsupportedParameter((byte) 1, "TINYINT");
        checkUnsupportedParameter((short) 1, "SMALLINT");
        checkUnsupportedParameter(1, "INTEGER");
        checkUnsupportedParameter((long) 1, "BIGINT");
        checkUnsupportedParameter(BigInteger.ONE, "DECIMAL");
        checkUnsupportedParameter(BigDecimal.ONE, "DECIMAL");
        checkUnsupportedParameter(1f, "REAL");
        checkUnsupportedParameter(1d, "DOUBLE");
    }

    private void checkUnsupportedParameter(Object param, String expectedTypeInErrorMessage) {
        checkUnsupportedParameter("IS TRUE", param, expectedTypeInErrorMessage);
        checkUnsupportedParameter("IS FALSE", param, expectedTypeInErrorMessage);
        checkUnsupportedParameter("IS NOT TRUE", param, expectedTypeInErrorMessage);
        checkUnsupportedParameter("IS NOT FALSE", param, expectedTypeInErrorMessage);
    }

    private void checkUnsupportedParameter(String function, Object param, String expectedTypeInErrorMessage) {
        checkFailureInternal(
            "SELECT * FROM map WHERE ? " + function,
            SqlErrorCode.DATA_EXCEPTION,
            "Cannot implicitly convert parameter at position 0 from " + expectedTypeInErrorMessage + " to BOOLEAN",
            param
        );
    }

    private Set<Integer> keys(String sql, Object... params) {
        List<SqlRow> rows = execute(member, sql, params);

        if (rows.size() == 0) {
            return Collections.emptySet();
        }

        assertEquals(1, rows.get(0).getMetadata().getColumnCount());

        Set<Integer> keys = new HashSet<>();

        for (SqlRow row : rows) {
            int key = row.getObject(0);

            boolean added = keys.add(key);

            assertTrue("Key is not unique: " + key, added);
        }

        return keys;
    }

    private static Set<Integer> set(Integer... values) {
        Set<Integer> res = new HashSet<>();

        if (values != null) {
            res.addAll(Arrays.asList(values));
        }

        return res;
    }
}
