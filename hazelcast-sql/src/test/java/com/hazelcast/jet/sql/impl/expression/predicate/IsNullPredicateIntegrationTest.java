/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.expression.predicate;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.predicate.IsNotNullPredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNullPredicate;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.BIG_DECIMAL;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.BIG_INTEGER;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.BOOLEAN;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.BYTE;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.CHARACTER;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.DOUBLE;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.FLOAT;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.INTEGER;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.LOCAL_DATE;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.LOCAL_DATE_TIME;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.LOCAL_TIME;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.LONG;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.OBJECT;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.OFFSET_DATE_TIME;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.SHORT;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for IS (NOT) NULL predicates.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IsNullPredicateIntegrationTest extends ExpressionTestSupport {
    @Test
    public void testColumn() {
        checkColumn(BOOLEAN);

        checkColumn(BYTE);
        checkColumn(SHORT);
        checkColumn(INTEGER);
        checkColumn(LONG);
        checkColumn(BIG_INTEGER);
        checkColumn(BIG_DECIMAL);
        checkColumn(FLOAT);
        checkColumn(DOUBLE);

        checkColumn(STRING);
        checkColumn(CHARACTER);

        checkColumn(LOCAL_DATE);
        checkColumn(LOCAL_TIME);
        checkColumn(LOCAL_DATE_TIME);
        checkColumn(OFFSET_DATE_TIME);

        checkColumn(OBJECT);
    }

    private void checkColumn(ExpressionType<?> type) {
        Class<? extends ExpressionValue> clazz = ExpressionValue.createClass(type);

        int keyNull = 0;
        int keyNotNull = 1;

        Map<Integer, Object> entries = new HashMap<>();
        entries.put(keyNull, ExpressionValue.create(clazz, keyNull, null));
        entries.put(keyNotNull, ExpressionValue.create(clazz, keyNotNull, type.valueFrom()));
        putAll(entries);

        checkColumn("IS NULL", set(keyNull));
        checkColumn("IS NOT NULL", set(keyNotNull));
    }

    private void checkColumn(String function, Set<Integer> expectedKeys) {
        String expression = "field1 " + function;
        String sql = "SELECT key, " + expression + " FROM map WHERE " + expression;

        List<SqlRow> rows = execute(sql);

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
    public void testLiteral() {
        Class<? extends ExpressionValue> clazz = ExpressionValue.createClass(INTEGER);

        int key = 0;
        ExpressionValue value = ExpressionValue.create(clazz, 0, 1);

        put(key, value);

        checkLiteral("null", true);

        checkLiteral("true", false);
        checkLiteral("false", false);

        checkLiteral("1", false);
        checkLiteral("1.1", false);
        checkLiteral("1.1E1", false);

        checkLiteral("'a'", false);
    }

    private void checkLiteral(String literal, boolean expectedResult) {
        checkLiteral(literal, "IS NULL", expectedResult);
        checkLiteral(literal, "IS NOT NULL", !expectedResult);
    }

    private void checkLiteral(String literal, String function, boolean expectedResult) {
        String expression = literal + " " + function;
        String sql = "SELECT " + expression + " FROM map WHERE " + expression;

        List<SqlRow> rows = execute(sql);

        if (expectedResult) {
            assertEquals(1, rows.size());

            SqlRow row = rows.get(0);

            assertEquals(SqlColumnType.BOOLEAN, row.getMetadata().getColumn(0).getType());
            assertTrue(row.getObject(0));
        } else {
            assertEquals(0, rows.size());
        }
    }

    @Test
    public void testParameter() {
        Class<? extends ExpressionValue> clazz = ExpressionValue.createClass(INTEGER);

        int key = 0;
        ExpressionValue value = ExpressionValue.create(clazz, 0, 1);

        put(key, value);

        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS NULL", new Object[]{null}));
        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS NOT NULL", new Object[]{null}));

        checkParameter(key, BOOLEAN);

        checkParameter(key, BYTE);
        checkParameter(key, SHORT);
        checkParameter(key, INTEGER);
        checkParameter(key, LONG);
        checkParameter(key, BIG_INTEGER);
        checkParameter(key, BIG_DECIMAL);
        checkParameter(key, FLOAT);
        checkParameter(key, DOUBLE);

        checkParameter(key, STRING);
        checkParameter(key, CHARACTER);

        checkParameter(key, LOCAL_DATE);
        checkParameter(key, LOCAL_TIME);
        checkParameter(key, LOCAL_DATE_TIME);
        checkParameter(key, OFFSET_DATE_TIME);

        checkParameter(key, OBJECT);
    }

    private void checkParameter(int key, ExpressionType<?> type) {
        Object parameter = type.valueFrom();

        assertNotNull(parameter);

        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS NULL", parameter));
        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS NOT NULL", parameter));
    }

    private Set<Integer> keys(String sql, Object... params) {
        List<SqlRow> rows = execute(sql, params);

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

    @Test
    public void testEquality_isNull() {
        ColumnExpression<?> column1 = ColumnExpression.create(1, QueryDataType.INT);
        ColumnExpression<?> column2 = ColumnExpression.create(2, QueryDataType.INT);

        checkEquals(IsNullPredicate.create(column1), IsNullPredicate.create(column1), true);
        checkEquals(IsNullPredicate.create(column1), IsNullPredicate.create(column2), false);
    }

    @Test
    public void testSerialization_isNull() {
        IsNullPredicate original = IsNullPredicate.create(ColumnExpression.create(1, QueryDataType.INT));
        IsNullPredicate restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_IS_NULL);

        checkEquals(original, restored, true);
    }

    @Test
    public void testEquality_isNotNull() {
        ColumnExpression<?> column1 = ColumnExpression.create(1, QueryDataType.INT);
        ColumnExpression<?> column2 = ColumnExpression.create(2, QueryDataType.INT);

        checkEquals(IsNotNullPredicate.create(column1), IsNotNullPredicate.create(column1), true);
        checkEquals(IsNotNullPredicate.create(column1), IsNotNullPredicate.create(column2), false);
    }

    @Test
    public void testSerialization_isNotNull() {
        IsNotNullPredicate original = IsNotNullPredicate.create(ColumnExpression.create(1, QueryDataType.INT));
        IsNotNullPredicate restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_IS_NOT_NULL);

        checkEquals(original, restored, true);
    }
}
