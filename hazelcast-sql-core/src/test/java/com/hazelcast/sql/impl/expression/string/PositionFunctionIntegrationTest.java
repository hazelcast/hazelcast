/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PositionFunctionIntegrationTest extends ExpressionTestSupport {
    private static final Pattern ERROR1 = Pattern.compile(
            "Parameter at position \\d+ must be of ((VARCHAR)|(INTEGER)) type,"
                    + " but \\w+ was found \\(consider adding an explicit CAST\\)"
    );

    private static final Pattern ERROR2 = Pattern.compile(
            "Cannot apply 'POSITION' function to \\[\\w+(, \\w+)+]"
                    + " \\(consider adding an explicit CAST\\)"
    );

    @Test
    public void test() {
        put("XYZ");

        check(sql("'ABCD'", "'BC'"), 2);
        check(sql("'ABCD'", "''"), 1);
        check(sql("'ABCD'", "'NO_MATCH'"), 0);


        // Test with start argument, slide the search index to right.
        // Valid start indices are between 0 and 4.
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

        checkError(sql("123", "23"));
        checkError(sql("'123'", "23"));
        checkError(sql("123", "'23'"));

        checkError(sql("TRUE", "FALSE"));
        checkError(sql("'ABC'", "TRUE"));
        checkError(sql("TRUE", "'ABC'"));

        checkError(sql("'ABCD'", "'BC'", "'CC'"));
        checkError(sql("'ABCD'", "'BC'", "TRUE"));

        checkError(sql("?", "?"), INTEGER_VAL, INTEGER_VAL);
        checkError(sql("?", "?"), STRING_VAL, INTEGER_VAL);
        checkError(sql("?", "?"), INTEGER_VAL, STRING_VAL);

        checkError(sql("?", "?"), BOOLEAN_VAL, BOOLEAN_VAL);
        checkError(sql("?", "?"), STRING_VAL, BOOLEAN_VAL);
        checkError(sql("?", "?"), BOOLEAN_VAL, STRING_VAL);

        checkError(sql("?", "?"), OFFSET_DATE_TIME_VAL, OFFSET_DATE_TIME_VAL);
        checkError(sql("?", "?"), STRING_VAL, OFFSET_DATE_TIME_VAL);
        checkError(sql("?", "?"), OFFSET_DATE_TIME_VAL, STRING_VAL);

        checkError(sql("?", "?", "?"), STRING_VAL, STRING_VAL, STRING_VAL);
        checkError(sql("?", "?", "?"), STRING_VAL, STRING_VAL, BOOLEAN_VAL);
        checkError(sql("?", "?", "?"), STRING_VAL, STRING_VAL, OFFSET_DATE_TIME_VAL);
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
        List<SqlRow> rows =  execute(member, sql, parameters);
        assertEquals("size must be equal to 1", rows.size(), 1);
        SqlRow row = rows.get(0);

        assertEquals(SqlColumnType.INTEGER, row.getMetadata().getColumn(0).getType());
        assertEquals(expectedResult, row.getObject(0));
    }

    private void checkError(String sql, Object ...parameters) {
        try {
            execute(member, sql, parameters);
            fail("did not throw exception");
        } catch (Exception e) {
            assertInstanceOf(HazelcastSqlException.class, e);

            String message = e.getMessage();
            assertTrue(
                    String.format("got unexpected error message: %s", message),
                    ERROR1.matcher(message).find() || ERROR2.matcher(message).find());

        }
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
