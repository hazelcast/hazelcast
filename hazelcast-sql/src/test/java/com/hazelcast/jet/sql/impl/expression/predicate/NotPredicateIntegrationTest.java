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
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.predicate.NotPredicate;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for NOT predicate.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class NotPredicateIntegrationTest extends ExpressionTestSupport {
    @Test
    public void test_column() {
        putAndCheck(true, false);
        putAndCheck(false, true);
        putAndCheck(null, null);

        putAndCheckFailure('t', sql("this"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.VARCHAR));
        putAndCheckFailure("true", sql("this"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.VARCHAR));
        putAndCheckFailure((byte) 1, sql("this"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.TINYINT));
        putAndCheckFailure((short) 1, sql("this"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.SMALLINT));
        putAndCheckFailure(1, sql("this"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.INTEGER));
        putAndCheckFailure(1L, sql("this"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.BIGINT));
        putAndCheckFailure(BigInteger.ONE, sql("this"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.DECIMAL));
        putAndCheckFailure(BigDecimal.ONE, sql("this"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.DECIMAL));
        putAndCheckFailure(1f, sql("this"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.REAL));
        putAndCheckFailure(LOCAL_DATE_VAL, sql("this"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.DATE));
        putAndCheckFailure(LOCAL_TIME_VAL, sql("this"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.TIME));
        putAndCheckFailure(LOCAL_DATE_TIME_VAL, sql("this"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.TIMESTAMP));
        putAndCheckFailure(OFFSET_DATE_TIME_VAL, sql("this"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.TIMESTAMP_WITH_TIME_ZONE));
        putAndCheckFailure(OBJECT_VAL, sql("this"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.OBJECT));
    }

    private void putAndCheck(Boolean value, Boolean expectedResult) {
        put(booleanValue1(value));

        check("field1", expectedResult);
    }

    @Test
    public void test_parameter() {
        put(1);

        check("?", false, true);
        check("?", true, false);
        check("?", null, (Boolean) null);

        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, SqlColumnType.BOOLEAN, SqlColumnType.VARCHAR), 't');
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, SqlColumnType.BOOLEAN, SqlColumnType.VARCHAR), "true");
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, SqlColumnType.BOOLEAN, SqlColumnType.TINYINT), (byte) 1);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, SqlColumnType.BOOLEAN, SqlColumnType.SMALLINT), (short) 1);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, SqlColumnType.BOOLEAN, SqlColumnType.INTEGER), 1);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, SqlColumnType.BOOLEAN, SqlColumnType.BIGINT), 1L);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, SqlColumnType.BOOLEAN, SqlColumnType.DECIMAL), BigInteger.ONE);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, SqlColumnType.BOOLEAN, SqlColumnType.DECIMAL), BigDecimal.ONE);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, SqlColumnType.BOOLEAN, SqlColumnType.REAL), 1f);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, SqlColumnType.BOOLEAN, SqlColumnType.DOUBLE), 1d);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, SqlColumnType.BOOLEAN, SqlColumnType.DATE), LOCAL_DATE_VAL);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, SqlColumnType.BOOLEAN, SqlColumnType.TIME), LOCAL_TIME_VAL);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, SqlColumnType.BOOLEAN, SqlColumnType.TIMESTAMP), LOCAL_DATE_TIME_VAL);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, SqlColumnType.BOOLEAN, SqlColumnType.TIMESTAMP_WITH_TIME_ZONE), OFFSET_DATE_TIME_VAL);
        checkFailure0(sql("?"), SqlErrorCode.DATA_EXCEPTION, parameterError(0, SqlColumnType.BOOLEAN, SqlColumnType.OBJECT), OBJECT_VAL);
    }

    @Test
    public void test_literal() {
        put(1);

        check("true", false);
        check("false", true);
        check("null", null);

        checkFailure0(sql("'true'"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.VARCHAR));
        checkFailure0(sql("1"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.TINYINT));
        checkFailure0(sql("1.1"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.DECIMAL));
        checkFailure0(sql("1.1E1"), SqlErrorCode.PARSING, signatureErrorOperator("NOT", SqlColumnType.DOUBLE));
    }

    private void check(String operand, Boolean expectedResult, Object... params) {
        String sql = sql(operand);

        List<SqlRow> rows = execute(sql, params);
        assertEquals(1, rows.size());

        SqlRow row = rows.get(0);
        assertEquals(1, row.getMetadata().getColumnCount());
        assertEquals(SqlColumnType.BOOLEAN, row.getMetadata().getColumn(0).getType());
        assertEquals(expectedResult, row.getObject(0));
    }

    private static String sql(String operand) {
        return "SELECT NOT " + operand + " FROM map";
    }

    @Test
    public void testEquality() {
        checkEquals(not(true), not(true), true);
        checkEquals(not(true), not(false), false);
    }

    @Test
    public void testSerialization() {
        NotPredicate original = not(true);
        NotPredicate restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_NOT);

        checkEquals(original, restored, true);
    }

    private static NotPredicate not(Boolean value) {
        return NotPredicate.create(ConstantExpression.create(value, QueryDataType.BOOLEAN));
    }
}
