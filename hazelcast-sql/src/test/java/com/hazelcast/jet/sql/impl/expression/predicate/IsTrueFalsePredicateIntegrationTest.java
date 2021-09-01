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
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.predicate.IsFalsePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotFalsePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsNotTruePredicate;
import com.hazelcast.sql.impl.expression.predicate.IsTruePredicate;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionValue;
import com.hazelcast.test.HazelcastSerialClassRunner;
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

import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.BIG_DECIMAL;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.BIG_INTEGER;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.BOOLEAN;
import static com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes.BYTE;
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
import static org.junit.Assert.assertTrue;

/**
 * Tests for IS (NOT) TRUE/FALSE predicates.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class IsTrueFalsePredicateIntegrationTest extends ExpressionTestSupport {
    @Test
    public void testColumn() {
        Class<? extends ExpressionValue> clazz = ExpressionValue.createClass(BOOLEAN);

        int keyTrue = 0;
        int keyFalse = 1;
        int keyNull = 2;

        Map<Integer, Object> entries = new HashMap<>();
        entries.put(keyTrue, ExpressionValue.create(clazz, keyTrue, true));
        entries.put(keyFalse, ExpressionValue.create(clazz, keyFalse, false));
        entries.put(keyNull, ExpressionValue.create(clazz, keyNull, null));
        putAll(entries);

        checkColumn("IS TRUE", set(keyTrue));
        checkColumn("IS FALSE", set(keyFalse));
        checkColumn("IS NOT TRUE", set(keyFalse, keyNull));
        checkColumn("IS NOT FALSE", set(keyTrue, keyNull));

        checkUnsupportedColumn(STRING, SqlColumnType.VARCHAR);
        checkUnsupportedColumn(BYTE, SqlColumnType.TINYINT);
        checkUnsupportedColumn(SHORT, SqlColumnType.SMALLINT);
        checkUnsupportedColumn(INTEGER, SqlColumnType.INTEGER);
        checkUnsupportedColumn(LONG, SqlColumnType.BIGINT);
        checkUnsupportedColumn(BIG_INTEGER, SqlColumnType.DECIMAL);
        checkUnsupportedColumn(BIG_DECIMAL, SqlColumnType.DECIMAL);
        checkUnsupportedColumn(FLOAT, SqlColumnType.REAL);
        checkUnsupportedColumn(DOUBLE, SqlColumnType.DOUBLE);
        checkUnsupportedColumn(LOCAL_DATE, SqlColumnType.DATE);
        checkUnsupportedColumn(LOCAL_TIME, SqlColumnType.TIME);
        checkUnsupportedColumn(LOCAL_DATE_TIME, SqlColumnType.TIMESTAMP);
        checkUnsupportedColumn(OFFSET_DATE_TIME, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE);
        checkUnsupportedColumn(OBJECT, SqlColumnType.OBJECT);
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

    private void checkUnsupportedColumn(ExpressionType<?> type, SqlColumnType expectedType) {
        checkUnsupportedColumn(type, "IS TRUE", expectedType);
        checkUnsupportedColumn(type, "IS FALSE", expectedType);
        checkUnsupportedColumn(type, "IS NOT FALSE", expectedType);
        checkUnsupportedColumn(type, "IS NOT TRUE", expectedType);
    }

    private void checkUnsupportedColumn(ExpressionType<?> type, String function, SqlColumnType expectedType) {
        String expectedErrorMessage = signatureErrorOperator(function, expectedType);

        int key = 0;
        put(key, ExpressionValue.create(ExpressionValue.createClass(type), key, type.valueFrom()));

        // Function in the condition
        checkFailure0(
                "SELECT key FROM map WHERE field1 " + function,
                SqlErrorCode.PARSING,
                expectedErrorMessage
        );

        // Function in the column
        checkFailure0(
                "SELECT field1 " + function + " FROM map",
                SqlErrorCode.PARSING,
                expectedErrorMessage
        );
    }

    @Test
    public void testLiteral() {
        put(ExpressionValue.create(ExpressionValue.createClass(INTEGER), 0, 1));

        // TRUE literal
        checkLiteral("TRUE", "IS TRUE", true);
        checkLiteral("true", "IS TRUE", true);

        checkLiteral("TRUE", "IS FALSE", false);
        checkLiteral("true", "IS FALSE", false);

        checkLiteral("TRUE", "IS NOT TRUE", false);
        checkLiteral("true", "IS NOT TRUE", false);

        checkLiteral("TRUE", "IS NOT FALSE", true);
        checkLiteral("true", "IS NOT FALSE", true);

        // False literal
        checkLiteral("FALSE", "IS TRUE", false);
        checkLiteral("false", "IS TRUE", false);

        checkLiteral("FALSE", "IS FALSE", true);
        checkLiteral("false", "IS FALSE", true);

        checkLiteral("FALSE", "IS NOT TRUE", true);
        checkLiteral("false", "IS NOT TRUE", true);

        checkLiteral("FALSE", "IS NOT FALSE", false);
        checkLiteral("false", "IS NOT FALSE", false);

        // NULL literal
        checkLiteral("NULL", "IS TRUE", false);
        checkLiteral("null", "IS TRUE", false);

        checkLiteral("NULL", "IS FALSE", false);
        checkLiteral("null", "IS FALSE", false);

        checkLiteral("NULL", "IS NOT TRUE", true);
        checkLiteral("null", "IS NOT TRUE", true);

        checkLiteral("NULL", "IS NOT FALSE", true);
        checkLiteral("null", "IS NOT FALSE", true);

        // Bad literals
        checkLiteralFailure("1", SqlColumnType.TINYINT);
        checkLiteralFailure("1.1", SqlColumnType.DECIMAL);
        checkLiteralFailure("1e1", SqlColumnType.DOUBLE);
        checkLiteralFailure("'true'", SqlColumnType.VARCHAR);
        checkLiteralFailure("'false'", SqlColumnType.VARCHAR);
        checkLiteralFailure("'null'", SqlColumnType.VARCHAR);
        checkLiteralFailure("'string'", SqlColumnType.VARCHAR);
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

    private void checkLiteralFailure(String literal, SqlColumnType expectedType) {
        checkLiteralFailure(literal, "IS TRUE", expectedType);
        checkLiteralFailure(literal, "IS FALSE", expectedType);
        checkLiteralFailure(literal, "IS NOT TRUE", expectedType);
        checkLiteralFailure(literal, "IS NOT FALSE", expectedType);
    }

    private void checkLiteralFailure(String literal, String function, SqlColumnType expectedType) {
        String expression = literal + " " + function;
        String sql = "SELECT " + expression + " FROM map WHERE " + expression;

        checkFailure0(sql, SqlErrorCode.PARSING, signatureErrorOperator(function, expectedType));
    }

    @Test
    public void testParameter() {
        int key = 0;
        put(key, ExpressionValue.create(ExpressionValue.createClass(INTEGER), 0, 1));

        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS TRUE", true));
        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS TRUE", false));
        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS TRUE", new Object[]{null}));

        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS FALSE", true));
        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS FALSE", false));
        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS FALSE", new Object[]{null}));

        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS NOT TRUE", true));
        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS NOT TRUE", false));
        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS NOT TRUE", new Object[]{null}));

        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS NOT FALSE", true));
        assertEquals(set(), keys("SELECT key FROM map WHERE ? IS NOT FALSE", false));
        assertEquals(set(key), keys("SELECT key FROM map WHERE ? IS NOT FALSE", new Object[]{null}));

        checkUnsupportedParameter("true", SqlColumnType.VARCHAR);
        checkUnsupportedParameter((byte) 1, SqlColumnType.TINYINT);
        checkUnsupportedParameter((short) 1, SqlColumnType.SMALLINT);
        checkUnsupportedParameter(1, SqlColumnType.INTEGER);
        checkUnsupportedParameter((long) 1, SqlColumnType.BIGINT);
        checkUnsupportedParameter(BigInteger.ONE, SqlColumnType.DECIMAL);
        checkUnsupportedParameter(BigDecimal.ONE, SqlColumnType.DECIMAL);
        checkUnsupportedParameter(1f, SqlColumnType.REAL);
        checkUnsupportedParameter(1d, SqlColumnType.DOUBLE);
        checkUnsupportedParameter(LOCAL_DATE_VAL, SqlColumnType.DATE);
        checkUnsupportedParameter(LOCAL_TIME_VAL, SqlColumnType.TIME);
        checkUnsupportedParameter(LOCAL_DATE_TIME_VAL, SqlColumnType.TIMESTAMP);
        checkUnsupportedParameter(OFFSET_DATE_TIME_VAL, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE);
        checkUnsupportedParameter(OBJECT_VAL, SqlColumnType.OBJECT);
    }

    private void checkUnsupportedParameter(Object param, SqlColumnType paramType) {
        checkUnsupportedParameter("IS TRUE", param, paramType);
        checkUnsupportedParameter("IS FALSE", param, paramType);
        checkUnsupportedParameter("IS NOT TRUE", param, paramType);
        checkUnsupportedParameter("IS NOT FALSE", param, paramType);
    }

    private void checkUnsupportedParameter(String function, Object param, SqlColumnType paramType) {
        String sql = "SELECT * FROM map WHERE ? " + function;

        String expectedErrorMessage = parameterError(0, SqlColumnType.BOOLEAN, paramType);

        checkFailure0(
                sql,
                SqlErrorCode.DATA_EXCEPTION,
                expectedErrorMessage,
                param
        );
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
    public void testEquality_isTrue() {
        ColumnExpression<?> column1 = ColumnExpression.create(0, QueryDataType.BOOLEAN);
        ColumnExpression<?> column2 = ColumnExpression.create(1, QueryDataType.BOOLEAN);

        checkEquals(IsTruePredicate.create(column1), IsTruePredicate.create(column1), true);
        checkEquals(IsTruePredicate.create(column1), IsTruePredicate.create(column2), false);
    }

    @Test
    public void testSerialization_isTrue() {
        IsTruePredicate original = IsTruePredicate.create(ColumnExpression.create(0, QueryDataType.BOOLEAN));
        IsTruePredicate restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_IS_TRUE);

        checkEquals(original, restored, true);
    }

    @Test
    public void testEquality_isFalse() {
        ColumnExpression<?> column1 = ColumnExpression.create(0, QueryDataType.BOOLEAN);
        ColumnExpression<?> column2 = ColumnExpression.create(1, QueryDataType.BOOLEAN);

        checkEquals(IsFalsePredicate.create(column1), IsFalsePredicate.create(column1), true);
        checkEquals(IsFalsePredicate.create(column1), IsFalsePredicate.create(column2), false);
    }

    @Test
    public void testSerialization_isFalse() {
        IsFalsePredicate original = IsFalsePredicate.create(ColumnExpression.create(0, QueryDataType.BOOLEAN));
        IsFalsePredicate restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_IS_FALSE);

        checkEquals(original, restored, true);
    }

    @Test
    public void testEquality_isNotTrue() {
        ColumnExpression<?> column1 = ColumnExpression.create(0, QueryDataType.BOOLEAN);
        ColumnExpression<?> column2 = ColumnExpression.create(1, QueryDataType.BOOLEAN);

        checkEquals(IsNotTruePredicate.create(column1), IsNotTruePredicate.create(column1), true);
        checkEquals(IsNotTruePredicate.create(column1), IsNotTruePredicate.create(column2), false);
    }

    @Test
    public void testSerialization_isNotTrue() {
        IsNotTruePredicate original = IsNotTruePredicate.create(ColumnExpression.create(0, QueryDataType.BOOLEAN));
        IsNotTruePredicate restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_IS_NOT_TRUE);

        checkEquals(original, restored, true);
    }

    @Test
    public void testEquality_isNotFalse() {
        ColumnExpression<?> column1 = ColumnExpression.create(0, QueryDataType.BOOLEAN);
        ColumnExpression<?> column2 = ColumnExpression.create(1, QueryDataType.BOOLEAN);

        checkEquals(IsNotFalsePredicate.create(column1), IsNotFalsePredicate.create(column1), true);
        checkEquals(IsNotFalsePredicate.create(column1), IsNotFalsePredicate.create(column2), false);
    }

    @Test
    public void testSerialization_isNotFalse() {
        IsNotFalsePredicate original = IsNotFalsePredicate.create(ColumnExpression.create(0, QueryDataType.BOOLEAN));
        IsNotFalsePredicate restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_IS_NOT_FALSE);

        checkEquals(original, restored, true);
    }
}
