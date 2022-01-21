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

package com.hazelcast.jet.sql.impl.expression.string;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.string.PositionFunction;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PositionFunctionIntegrationTest extends ExpressionTestSupport {

    @Test
    public void test() {
        put("XYZ");

        check(sql("'ABCD'", "'BC'"), 2);
        check(sql("'ABCD'", "''"), 1);
        check(sql("'ABCD'", "'NO_MATCH'"), 0);


        // Test with start argument, slide the search index to right.
        // Valid start indices are between 1 and 4.
        check(sql("'ABCD'", "'BC'", 0), 0); // Invalid index
        check(sql("'ABCD'", "'BC'", 1), 2);
        check(sql("'ABCD'", "'BC'", 2), 2);
        check(sql("'ABCD'", "'BC'", 3), 0);
        check(sql("'ABCD'", "'BC'", 4), 0);
        check(sql("'ABCD'", "'BC'", 5), 0); // Invalid index

        // Repeated string
        check(sql("'ABAB'", "'AB'", 1), 1);
        check(sql("'ABAB'", "'AB'", 2), 3);
    }

    @Test
    public void test_null() {
        put("XYZ");

        check(sql("'BC'", "NULL"), null);
        check(sql("NULL", "'BC'"), null);
        check(sql("NULL", "NULL"), null);

        check(sql("NULL", "NULL", 1), null);
        check(sql("'ABC'", "'ABC'", "NULL"), null);
    }

    @Test
    public void test_parameter() {
        put("XYZ");

        check(sql("?", "'BC'"), 2, "ABCD");
        check(sql("'ABCD'", "?"), 2, "BC");
        check(sql("?", "?"), 2, "BC", "ABCD");

        check(sql("?", "?", "?"), 2, "BC", "ABCD", 1);
        check(sql("?", "?", "?"), 0, "BC", "ABCD", 3);
    }

    @Test
    public void test_wrong_type() {
        put("XYZ");

        checkError(sql("123", "23"), "Cannot apply 'POSITION' function to [TINYINT, TINYINT] (consider adding an explicit CAST)");
        checkError(sql("'123'", "23"), "Cannot apply 'POSITION' function to [TINYINT, VARCHAR] (consider adding an explicit CAST)");
        checkError(sql("123", "'23'"), "Cannot apply 'POSITION' function to [VARCHAR, TINYINT] (consider adding an explicit CAST)");

        checkError(sql("TRUE", "FALSE"), "Cannot apply 'POSITION' function to [BOOLEAN, BOOLEAN] (consider adding an explicit CAST)");
        checkError(sql("'ABC'", "TRUE"), "Cannot apply 'POSITION' function to [BOOLEAN, VARCHAR] (consider adding an explicit CAST)");
        checkError(sql("TRUE", "'ABC'"), "Cannot apply 'POSITION' function to [VARCHAR, BOOLEAN] (consider adding an explicit CAST)");

        checkError(sql("'ABCD'", "'BC'", "'CC'"), "Cannot apply 'POSITION' function to [VARCHAR, VARCHAR, VARCHAR] (consider adding an explicit CAST)");
        checkError(sql("'ABCD'", "'BC'", "TRUE"), "Cannot apply 'POSITION' function to [VARCHAR, VARCHAR, BOOLEAN] (consider adding an explicit CAST)");

        checkError(sql("?", "?"), "Parameter at position 0 must be of VARCHAR type, but INTEGER was found (consider adding an explicit CAST)", INTEGER_VAL, INTEGER_VAL);
        checkError(sql("?", "?"), "Parameter at position 1 must be of VARCHAR type, but INTEGER was found (consider adding an explicit CAST)", STRING_VAL, INTEGER_VAL);
        checkError(sql("?", "?"), "Parameter at position 0 must be of VARCHAR type, but INTEGER was found (consider adding an explicit CAST)", INTEGER_VAL, STRING_VAL);

        checkError(sql("?", "?"), "Parameter at position 0 must be of VARCHAR type, but BOOLEAN was found (consider adding an explicit CAST)", BOOLEAN_VAL, BOOLEAN_VAL);
        checkError(sql("?", "?"), "Parameter at position 1 must be of VARCHAR type, but BOOLEAN was found (consider adding an explicit CAST)", STRING_VAL, BOOLEAN_VAL);
        checkError(sql("?", "?"), "Parameter at position 0 must be of VARCHAR type, but BOOLEAN was found (consider adding an explicit CAST)", BOOLEAN_VAL, STRING_VAL);

        checkError(sql("?", "?"), "Parameter at position 0 must be of VARCHAR type, but TIMESTAMP WITH TIME ZONE was found (consider adding an explicit CAST)", OFFSET_DATE_TIME_VAL, OFFSET_DATE_TIME_VAL);
        checkError(sql("?", "?"), "Parameter at position 1 must be of VARCHAR type, but TIMESTAMP WITH TIME ZONE was found (consider adding an explicit CAST)", STRING_VAL, OFFSET_DATE_TIME_VAL);
        checkError(sql("?", "?"), "Parameter at position 0 must be of VARCHAR type, but TIMESTAMP WITH TIME ZONE was found (consider adding an explicit CAST)", OFFSET_DATE_TIME_VAL, STRING_VAL);

        checkError(sql("?", "?", "?"), "Parameter at position 2 must be of INTEGER type, but VARCHAR was found (consider adding an explicit CAST)", STRING_VAL, STRING_VAL, STRING_VAL);
        checkError(sql("?", "?", "?"), "Parameter at position 2 must be of INTEGER type, but BOOLEAN was found (consider adding an explicit CAST)", STRING_VAL, STRING_VAL, BOOLEAN_VAL);
        checkError(sql("?", "?", "?"), "Parameter at position 2 must be of INTEGER type, but TIMESTAMP WITH TIME ZONE was found (consider adding an explicit CAST)", STRING_VAL, STRING_VAL, OFFSET_DATE_TIME_VAL);
    }

    @Test
    public void test_equality() {
        PositionFunction f = createFunctionWithoutStart("AB", "ABCD");

        checkEquals(f, createFunctionWithoutStart("AB", "ABCD"), true);

        checkEquals(f, createFunctionWithoutStart("AB", "ABC"), false);
        checkEquals(f, createFunctionWithoutStart("ABC", "ABCD"), false);

        f = createFunction("AB", "ABCD", 2);

        checkEquals(f, createFunction("AB", "ABCD", 2), true);

        checkEquals(f, createFunction("AB", "ABC", 2), false);
        checkEquals(f, createFunction("ABC", "ABCD", 3), false);
        checkEquals(f, createFunctionWithoutStart("AB", "ABCD"), false);
    }

    @Test
    public void test_serialization() {
        PositionFunction f = createFunctionWithoutStart("AB", "ABCD");
        PositionFunction deserialized = serializeAndCheck(f, SqlDataSerializerHook.EXPRESSION_POSITION);

        checkEquals(f, deserialized, true);

        f = createFunction("AB", "ABCD", 2);
        deserialized = serializeAndCheck(f, SqlDataSerializerHook.EXPRESSION_POSITION);

        checkEquals(f, deserialized, true);
    }

    private void check(String sql, Integer expectedResult, Object ...parameters) {
        List<SqlRow> rows =  execute(sql, parameters);
        assertEquals("size", 1, rows.size());
        SqlRow row = rows.get(0);

        assertEquals(SqlColumnType.INTEGER, row.getMetadata().getColumn(0).getType());
        assertEquals(expectedResult, row.getObject(0));
    }

    private void checkError(String sql, String expectedError, Object... parameters) {
        assertThatThrownBy(
                () -> {
                    execute(sql, parameters);
                    fail("did not throw exception");
                })
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining(expectedError);
    }

    private String sql(Object text, Object search) {
        return String.format("SELECT POSITION(%s IN %s) FROM map", search, text);
    }

    private String sql(Object text, Object search, Object start) {
        return String.format("SELECT POSITION(%s IN %s FROM %s) FROM map", search, text, start);
    }

    private PositionFunction createFunction(String search, String text, int start) {
        return PositionFunction.create(
                ConstantExpression.create(search, QueryDataType.VARCHAR),
                ConstantExpression.create(text, QueryDataType.VARCHAR),
                ConstantExpression.create(start, QueryDataType.INT)
        );
    }

    private PositionFunction createFunctionWithoutStart(String search, String text) {
        return PositionFunction.create(
                ConstantExpression.create(search, QueryDataType.VARCHAR),
                ConstantExpression.create(text, QueryDataType.VARCHAR),
                null
        );
    }
}
