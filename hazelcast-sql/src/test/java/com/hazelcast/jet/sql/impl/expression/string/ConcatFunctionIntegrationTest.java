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

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.jet.sql.impl.expression.ExpressionTestSupport;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionBiValue;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionType;
import com.hazelcast.jet.sql.impl.support.expressions.ExpressionTypes;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.string.ConcatFunction;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.sql.impl.type.QueryDataType.VARCHAR;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConcatFunctionIntegrationTest extends ExpressionTestSupport {
    @Test
    public void testColumn() {
        for (ExpressionType<?> type1 : ExpressionTypes.all()) {
            // Test supported types
            for (ExpressionType<?> type2 : ExpressionTypes.all()) {
                Class<? extends ExpressionBiValue> clazz = ExpressionBiValue.createBiClass(type1.typeName(), type2.typeName());

                checkColumns(
                        ExpressionBiValue.createBiValue(clazz, 0, type1.valueFrom(), type2.valueFrom()),
                        "" + type1.valueFrom() + type2.valueFrom()
                );

                checkColumns(
                        ExpressionBiValue.createBiValue(clazz, 0, null, type2.valueFrom()),
                        null
                );

                checkColumns(
                        ExpressionBiValue.createBiValue(clazz, 0, type1.valueFrom(), null),
                        null
                );

                checkColumns(
                        ExpressionBiValue.createBiValue(clazz, 0, null, null),
                        null
                );
            }
        }
    }

    @Test
    public void testLiteral() {
        put("1");

        check("this || 2", "12");
        check("this || '2'", "12");
        check("this || 2e0", "12E0");

        check("this || true", "1true");

        check("this || null", null);
        check("null || null", null);

        check("1 || 2", "12");
        check("'1' || '2'", "12");
    }

    @Test
    public void testParameter() {
        put("1");

        check("this || ?", "12", "2");
        check("this || ?", "12", '2');
        check("this || ?", null, new Object[]{null});
        check("this || ?", "12", 2);

        check("this || ?", "1" + LOCAL_DATE_VAL, LOCAL_DATE_VAL);
        check("this || ?", "1" + LOCAL_TIME_VAL, LOCAL_TIME_VAL);
        check("this || ?", "1" + LOCAL_DATE_TIME_VAL, LOCAL_DATE_TIME_VAL);
        check("this || ?", "1" + OFFSET_DATE_TIME_VAL, OFFSET_DATE_TIME_VAL);

        check("? || this", "21", "2");
        check("? || this", "21", '2');
        check("? || this", null, new Object[]{null});

        check("? || ?", "12", "1", "2");
        check("? || ?", null, "1", null);
        check("? || ?", null, null, "2");
        check("? || ?", null, null, null);

        check("? || null", null, "1");
        check("null || ?", null, "1");
    }

    /**
     * Make sure that the function produces proper result when there are more than two arguments.
     * We need this to track possible regression in case function's signature is changed in the future
     * to varargs.
     */
    @Test
    public void testThreeOperands() {
        put(1, new ExpressionBiValue.StringStringVal().fields("2", "3"));

        check("__key || field1 || field2", "123");
    }

    @Test
    public void testEquals() {
        ConcatFunction function = ConcatFunction.create(ConstantExpression.create("1", VARCHAR), ConstantExpression.create("2", VARCHAR));

        checkEquals(function, ConcatFunction.create(ConstantExpression.create("1", VARCHAR), ConstantExpression.create("2", VARCHAR)), true);
        checkEquals(function, ConcatFunction.create(ConstantExpression.create("10", VARCHAR), ConstantExpression.create("2", VARCHAR)), false);
        checkEquals(function, ConcatFunction.create(ConstantExpression.create("1", VARCHAR), ConstantExpression.create("20", VARCHAR)), false);
    }

    @Test
    public void testSerialization() {
        ConcatFunction original = ConcatFunction.create(ConstantExpression.create("1", VARCHAR), ConstantExpression.create("2", VARCHAR));
        ConcatFunction restored = serializeAndCheck(original, SqlDataSerializerHook.EXPRESSION_CONCAT);

        checkEquals(original, restored, true);
    }

    private void checkColumns(ExpressionBiValue value, String expectedResult) {
        put(value);

        check("field1 || field2", expectedResult);
    }

    private void check(String operands, String expectedResult, Object... params) {
        String sql = "SELECT " + operands + " FROM map";

        checkValue0(sql, SqlColumnType.VARCHAR, expectedResult, params);
    }
}
