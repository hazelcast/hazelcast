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

import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.string.ConcatWSFunction;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConcatWSFunctionIntegrationTest extends ExpressionTestSupport {

    @Test
    public void testColumn() {
        ExpressionType<?>[] allTypes = ExpressionTypes.all();

        for (int i = 0; i < allTypes.length; i++) {
            for (int j = i; j < allTypes.length; j++) {
                ExpressionType<?> type1 = allTypes[i];
                ExpressionType<?> type2 = allTypes[j];

                Class<? extends ExpressionBiValue> clazz =
                        ExpressionBiValue.createBiClass(type1.typeName(), type2.typeName());

                ExpressionBiValue[] values = new ExpressionBiValue[]{
                        ExpressionBiValue.createBiValue(clazz, 0, type1.valueFrom(), type2.valueFrom()),
                        ExpressionBiValue.createBiValue(clazz, 0, null, type2.valueFrom()),
                        ExpressionBiValue.createBiValue(clazz, 0, type1.valueFrom(), null),
                        ExpressionBiValue.createBiValue(clazz, 0, null, null)
                };

                String[] expectedResults = new String[]{
                        type1.valueFrom() + "-" + type2.valueFrom(),
                        type2.valueFrom().toString(),
                        type1.valueFrom().toString(),
                        ""
                };

                checkColumns(values, expectedResults);
            }
        }
    }

    @Test
    public void testLiteral() {
        put("1");

        check(getConcatWsExpression("-", "this", "2"), "1-2");
        check(getConcatWsExpression("-", "this", "'2'"), "1-2");
        check(getConcatWsExpression("-", "this", "2e0"), "1-2E0");

        check(getConcatWsExpression("-", "this", "true"), "1-true");

        check(getConcatWsExpression("-", "this", "null"), "1");

        check(getConcatWsExpression("-", "1", "2"), "1-2");
        check(getConcatWsExpression("-", "'1'", "2"), "1-2");
    }

    @Test
    public void testNull() {
        put("1");

        //Null on separator => returns null
        check(getConcatWsExpression("null", false, "3", "2"), null);

        //null on parameters => ignores null parameters
        check(getConcatWsExpression("-", "1", "null", "3"), "1-3");
        check(getConcatWsExpression("-", "null", "null"), "");

        check(getConcatWsExpression("-", "null", "2", "'3'"), "2-3");
    }

    @Test
    public void testEmpty() {
        put("1");
        // Empty separator => just concat, ignoring nulls
        check(getConcatWsExpression("", "3", "2"), "32");
        check(getConcatWsExpression("", "3", "null", "2"), "32");
        // Empty element => not ignored
        check(getConcatWsExpression("-", "3", "''", "2"), "3--2");
    }

    @Test
    public void testNonStringSeparator() {
        put("1");

        //int on separator => returns error
        checkFail(getConcatWsExpression("5", false, "3", "2"));
    }

    @Test
    public void testParameter() {
        put("1");

        check(getConcatWsExpression("-", "this", "?"), "1-2", 2);
        check(getConcatWsExpression("-", "this", "?"), "1-2", "2");
        check(getConcatWsExpression("-", "this", "?"), "1-2", '2');
        check(getConcatWsExpression("-", "this", "?"), "1", new Object[]{null});

        check(getConcatWsExpression("-", "this", "?"), "1-" + LOCAL_DATE_VAL, LOCAL_DATE_VAL);
        check(getConcatWsExpression("-", "this", "?"), "1-" + LOCAL_TIME_VAL, LOCAL_TIME_VAL);
        check(getConcatWsExpression("-", "this", "?"), "1-" + LOCAL_DATE_TIME_VAL, LOCAL_DATE_TIME_VAL);
        check(getConcatWsExpression("-", "this", "?"), "1-" + OFFSET_DATE_TIME_VAL, OFFSET_DATE_TIME_VAL);

        check(getConcatWsExpression("-", "?", "?"), "1-2", 1, 2);
        check(getConcatWsExpression("-", "?", "?"), "1", 1, null);
        check(getConcatWsExpression("-", "?", "?"), "2", null, '2');
        check(getConcatWsExpression("-", "?", "?"), "", null, null);

        check(getConcatWsExpression("?", false, "?", "?"), "", "-", null, null);
        checkFail(getConcatWsExpression("?", false, "?", "?"), 1, null, null);
        check(getConcatWsExpression("?", false, "?", "?"), null, null, null, null);
    }

    @Test
    public void testOperandCount() {
        put(1, new ExpressionBiValue.StringStringVal().fields("2", "3"));

        //less than 2 params => returns error
        checkFail(getConcatWsExpression("-"));

        //more than 3 params
        check(getConcatWsExpression("-", "__key", "field1", "field2"), "1-2-3");
        check(getConcatWsExpression("-", "1", "2", "3", "'4'"), "1-2-3-4");
    }

    @Test
    public void testEquals() {
        ConcatWSFunction function = ConcatWSFunction.create(ConstantExpression.create("-", VARCHAR), ConstantExpression.create("1", VARCHAR), ConstantExpression.create("2", VARCHAR));

        checkEquals(function, ConcatWSFunction.create(ConstantExpression.create("-", VARCHAR), ConstantExpression.create("1", VARCHAR), ConstantExpression.create("2", VARCHAR)), true);
        checkEquals(function, ConcatWSFunction.create(ConstantExpression.create("-", VARCHAR), ConstantExpression.create("1", INT), ConstantExpression.create("2", VARCHAR)), false);
        checkEquals(function, ConcatWSFunction.create(ConstantExpression.create("-", VARCHAR), ConstantExpression.create("1", VARCHAR), ConstantExpression.create("20", VARCHAR)), false);
        checkEquals(function, ConcatWSFunction.create(ConstantExpression.create("-", VARCHAR), ConstantExpression.create("2", VARCHAR)), false);
        checkEquals(function, ConcatWSFunction.create(ConstantExpression.create("1", VARCHAR), ConstantExpression.create("2", VARCHAR)), false);
    }

    @Test
    public void testSerialization() {
        ConcatWSFunction original = ConcatWSFunction.create(ConstantExpression.create("-", VARCHAR), ConstantExpression.create("1", VARCHAR), ConstantExpression.create("2", VARCHAR));
        ConcatWSFunction corrupted1 = ConcatWSFunction.create(ConstantExpression.create("===", VARCHAR), ConstantExpression.create("1", VARCHAR), ConstantExpression.create("2", VARCHAR));
        ConcatWSFunction corrupted2 = ConcatWSFunction.create(ConstantExpression.create("-", VARCHAR), ConstantExpression.create("10", VARCHAR), ConstantExpression.create("2", VARCHAR));
        ConcatWSFunction corrupted3 = ConcatWSFunction.create(ConstantExpression.create("-", VARCHAR), ConstantExpression.create("1", INT), ConstantExpression.create("2", VARCHAR));
        ConcatWSFunction corrupted4 = ConcatWSFunction.create(ConstantExpression.create("-", VARCHAR), ConstantExpression.create("1", VARCHAR), ConstantExpression.create(null, VARCHAR));
        ConcatWSFunction restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_CONCAT_WS);

        checkEquals(original, restored, true);
        checkEquals(corrupted1, restored, false);
        checkEquals(corrupted2, restored, false);
        checkEquals(corrupted3, restored, false);
        checkEquals(corrupted4, restored, false);
    }

    private void checkColumns(Object[] values, Object[] expectedResults) {
        putAll(values);

        checkValues0("SELECT Concat_WS('-', field1, field2) FROM map", SqlColumnType.VARCHAR, expectedResults);
    }

    private void check(String operands, String expectedResult, Object... params) {
        String sql = "SELECT " + operands + " FROM map";

        checkValue0(sql, SqlColumnType.VARCHAR, expectedResult, params);
    }

    private void checkFail(String expression, Object... params) {
        try {
            String sql = "SELECT " + expression + " FROM map";
            execute(sql, params);
            fail("Following query should have caused an error!  ===> " + sql);
        } catch (Exception e) {
            assertTrue(e instanceof HazelcastSqlException);
        }
    }

    private String getConcatWsExpression(String separator, String... operands) {
        return getConcatWsExpression(separator, true, operands);
    }

    private String getConcatWsExpression(String separator, boolean quoteSeparator, String... operands) {
        if (quoteSeparator) {
            separator = "'" + separator + "'";
        }
        StringBuilder expression = new StringBuilder("Concat_WS(" + separator);
        for (String operand : operands) {
            expression.append(", ").append(operand);
        }
        expression.append(")");
        return expression.toString();
    }
}
