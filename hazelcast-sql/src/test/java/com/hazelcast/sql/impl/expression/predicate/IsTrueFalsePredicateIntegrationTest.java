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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.SqlException;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionType;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.sql.support.expressions.ExpressionTypes.BIG_DECIMAL;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.BIG_INTEGER;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.BOOLEAN;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.BYTE;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.DOUBLE;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.FLOAT;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.INTEGER;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.LONG;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.OBJECT;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for IS (NOT) TRUE/FALSE predicates.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IsTrueFalsePredicateIntegrationTest extends SqlTestSupport {

    private static final String MAP_NAME = "map";

    private final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(1);

    private HazelcastInstance member;
    private IMap<Integer, ExpressionValue> map;

    @Before
    public void before() {
        member = factory.newHazelcastInstance();
        map = member.getMap(MAP_NAME);
    }

    @After
    public void after() {
        factory.shutdownAll();
    }

    @Test
    public void testLiteral() {
        Class<? extends ExpressionValue> clazz = ExpressionValue.createClass(INTEGER);

        int key = 0;
        ExpressionValue value = ExpressionValue.create(clazz, 0, 1);

        map.put(key, value);

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
        String sql = "SELECT " + expression + " FROM " + MAP_NAME + " WHERE " + expression;

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
        SqlException error = executeWithException(member, "SELECT * FROM " + MAP_NAME + " WHERE 'bad' " + function);
        assertEquals(SqlErrorCode.PARSING, error.getCode());
        assertTrue(error.getMessage(), error.getMessage().contains("Literal ''bad'' can not be parsed to type 'BOOLEAN'"));

        error = executeWithException(member, "SELECT 'bad' " + function + " FROM " + MAP_NAME);
        assertEquals(SqlErrorCode.PARSING, error.getCode());
        assertTrue(error.getMessage(), error.getMessage().contains("Literal ''bad'' can not be parsed to type 'BOOLEAN'"));
    }

    @Test
    public void testColumn_boolean() {
        checkColumn(BOOLEAN, true, false);
    }

    @Test
    public void testColumn_string() {
        checkColumn(STRING, "true", "false");
    }

    @Test
    public void testColumn_object() {
        checkColumn(OBJECT, true, false);
    }

    private void checkColumn(ExpressionType<?> type, Object trueValue, Object falseValue) {
        Class<? extends ExpressionValue> clazz = ExpressionValue.createClass(type);

        int keyTrue = 0;
        int keyFalse = 1;
        int keyNull = 2;

        map.put(keyTrue, ExpressionValue.create(clazz, keyTrue, trueValue));
        map.put(keyFalse, ExpressionValue.create(clazz, keyFalse, falseValue));
        map.put(keyNull, ExpressionValue.create(clazz, keyNull, null));

        checkColumn("IS TRUE", set(keyTrue));
        checkColumn("IS FALSE", set(keyFalse));
        checkColumn("IS NOT TRUE", set(keyFalse, keyNull));
        checkColumn("IS NOT FALSE", set(keyTrue, keyNull));
    }

    private void checkColumn(String function, Set<Integer> expectedKeys) {
        String expression = "field1 " + function;
        String sql = "SELECT key, " + expression + " FROM " + MAP_NAME + " WHERE " + expression;

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
        // TODO: Uncomment when merging to master, it should be fixed there.
//        checkColumnBad(CHARACTER, 'a', "VARCHAR");
    }

    @Test
    public void testColumnBad_object() {
        // TODO: Uncomment when merging to master, it should be fixed there.
//        checkColumnBad(OBJECT, OBJECT.valueFrom(), "OBJECT");
    }

    private void checkColumnBad(ExpressionType<?> type, Object value, Object expectedFromType) {
        Class<? extends ExpressionValue> clazz = ExpressionValue.createClass(type);

        map.put(1, ExpressionValue.create(clazz, 1, value));

        checkColumnBad("IS TRUE", expectedFromType, value);
        checkColumnBad("IS FALSE", expectedFromType, value);
        checkColumnBad("IS NOT TRUE", expectedFromType, value);
        checkColumnBad("IS NOT FALSE", expectedFromType, value);
    }

    private void checkColumnBad(String function, Object expectedFromType, Object expectedBadValue) {
        String expectedErrorMessage = expectedFromType + " value cannot be converted to BOOLEAN: " + expectedBadValue;

        SqlException exception = executeWithException(member, "SELECT key FROM " + MAP_NAME + " WHERE field1 " + function);

        assertEquals(SqlErrorCode.DATA_EXCEPTION, exception.getCode());
        assertEquals(expectedErrorMessage, exception.getMessage());
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

        map.clear();

        Class<? extends ExpressionValue> clazz = ExpressionValue.createClass(type);

        int key = 0;
        ExpressionValue value = ExpressionValue.create(clazz, key, type.valueFrom());

        map.put(key, value);

        // Function in the condition
        SqlException error = executeWithException(member, "SELECT key FROM " + MAP_NAME + " WHERE field1 " + function);

        assertEquals(SqlErrorCode.PARSING, error.getCode());
        assertTrue(error.getMessage(), error.getMessage().contains(expectedErrorMessage));

        // Function in the column
        error = executeWithException(member, "SELECT field1 " + function + " FROM " + MAP_NAME);

        assertEquals(SqlErrorCode.PARSING, error.getCode());
        assertTrue(error.getMessage(), error.getMessage().contains(expectedErrorMessage));
    }

    @Test
    public void testParameter() {
        Class<? extends ExpressionValue> clazz = ExpressionValue.createClass(INTEGER);

        int key = 0;
        ExpressionValue value = ExpressionValue.create(clazz, 0, 1);

        map.put(key, value);

        assertEquals(set(key), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS TRUE", true));
        assertEquals(set(), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS TRUE", false));
        assertEquals(set(key), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS TRUE", "true"));
        assertEquals(set(), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS TRUE", "false"));
        assertEquals(set(), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS TRUE", new Object[] { null }));

        assertEquals(set(), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS FALSE", true));
        assertEquals(set(key), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS FALSE", false));
        assertEquals(set(), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS FALSE", "true"));
        assertEquals(set(key), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS FALSE", "false"));
        assertEquals(set(), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS FALSE", new Object[] { null }));

        assertEquals(set(), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS NOT TRUE", true));
        assertEquals(set(key), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS NOT TRUE", false));
        assertEquals(set(), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS NOT TRUE", "true"));
        assertEquals(set(key), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS NOT TRUE", "false"));
        assertEquals(set(key), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS NOT TRUE", new Object[] { null }));

        assertEquals(set(key), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS NOT FALSE", true));
        assertEquals(set(), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS NOT FALSE", false));
        assertEquals(set(key), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS NOT FALSE", "true"));
        assertEquals(set(), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS NOT FALSE", "false"));
        assertEquals(set(key), keys("SELECT key FROM " + MAP_NAME + " WHERE ? IS NOT FALSE", new Object[] { null }));

        checkUnsupportedParameter((byte) 1, "TINYINT");
        checkUnsupportedParameter((short) 1, "SMALLINT");
        checkUnsupportedParameter(1, "INT");
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
        String sql = "SELECT * FROM " + MAP_NAME + " WHERE ? " + function;

        SqlException error = executeWithException(member, sql, param);

        assertEquals(SqlErrorCode.DATA_EXCEPTION, error.getCode());
        assertEquals("Cannot convert " + expectedTypeInErrorMessage + " to BOOLEAN", error.getMessage());
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
