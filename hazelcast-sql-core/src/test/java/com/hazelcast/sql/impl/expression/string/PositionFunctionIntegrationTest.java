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

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PositionFunctionIntegrationTest extends ExpressionTestSupport {

    @Test
    public void test() {
        put("XYZ");

        check(sql("'ABCD'", "'BC'"), 2);
        check(sql("'ABCD'", "''"), 1);
        check(sql("'ABCD'", "'NO_MATCH'"), 0);
    }

    @Test
    public void test_null() {
        put("XYZ");

        check(sql("'BC'", "NULL"), null);
        check(sql("NULL", "'BC'"), null);
        check(sql("NULL", "NULL"), null);
    }

    @Test
    public void test_parameter() {
        put("XYZ");

        check(sql("?", "'BC'"), 2, "ABCD");
        check(sql("'ABCD'", "?"), 2, "BC");
        check(sql("?", "?"), 2, "BC", "ABCD");
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

        checkError(sql("?", "?"), INTEGER_VAL, INTEGER_VAL);
        checkError(sql("?", "?"), STRING_VAL, INTEGER_VAL);
        checkError(sql("?", "?"), INTEGER_VAL, STRING_VAL);

        checkError(sql("?", "?"), BOOLEAN_VAL, BOOLEAN_VAL);
        checkError(sql("?", "?"), STRING_VAL, BOOLEAN_VAL);
        checkError(sql("?", "?"), BOOLEAN_VAL, STRING_VAL);

        checkError(sql("?", "?"), OFFSET_DATE_TIME_VAL, OFFSET_DATE_TIME_VAL);
        checkError(sql("?", "?"), STRING_VAL, OFFSET_DATE_TIME_VAL);
        checkError(sql("?", "?"), OFFSET_DATE_TIME_VAL, STRING_VAL);
    }

    @Test
    public void test_equality() {
        PositionFunction f = createFunction("AB", "ABCD");

        checkEquals(f, createFunction("AB", "ABCD"), true);

        checkEquals(f, createFunction("AB", "ABC"), false);
        checkEquals(f, createFunction("ABC", "ABCD"), false);
    }

    @Test
    public void test_serialization() {
        PositionFunction f = createFunction("AB", "ABCD");
        PositionFunction deserialized = serializeAndCheck(f, SqlDataSerializerHook.EXPRESSION_POSITION);

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
        assertThrows(HazelcastSqlException.class, () -> {
            execute(member, sql, parameters);
        });
    }

    private String sql(Object text, Object search) {
        return String.format("SELECT POSITION(%s IN %s) FROM map", search, text);
    }

    private PositionFunction createFunction(String search, String text) {
        return PositionFunction.create(
                ConstantExpression.create(search, QueryDataType.VARCHAR),
                ConstantExpression.create(text, QueryDataType.VARCHAR)
        );
    }
}
