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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.SqlExpressionIntegrationTestSupport;
import com.hazelcast.sql.support.expressions.ExpressionBiValue;
import com.hazelcast.sql.support.expressions.ExpressionType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

import static com.hazelcast.sql.support.expressions.ExpressionTypes.BIG_DECIMAL;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.BIG_INTEGER;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.BOOLEAN;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.BYTE;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.DOUBLE;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.FLOAT;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.INTEGER;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.LONG;
import static com.hazelcast.sql.support.expressions.ExpressionTypes.SHORT;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConcatFunctionIntegrationTest extends SqlExpressionIntegrationTestSupport {
    @Test
    public void test_literal() {
        put("1");

        check("this || 2", "12");
        check("this || '2'", "12");
        check("this || 2e0", "12.0");

        check("this || true", "1true");

        check("this || null", null);
        check("null || null", null);

        check("1 || 2", "12");
        check("'1' || '2'", "12");
    }

    @Test
    public void test_parameter() {
        put("1");

        check("this || ?", "12", "2");
        check("this || ?", "12", '2');
        check("this || ?", null, new Object[] { null });

        check("? || this", "21", "2");
        check("? || this", "21", '2');
        check("? || this", null, new Object[] { null });

        check("? || ?", "12", "1", "2");
        check("? || ?", null, "1", null);
        check("? || ?", null, null, "2");
        check("? || ?", null, null, null);

        check("? || null", null, "1");
        check("null || ?", null, "1");

        checkFailure("this || ?", SqlErrorCode.DATA_EXCEPTION, "Cannot implicitly convert parameter at position 0 from INTEGER to VARCHAR ", 2);
    }

    @Test
    public void test_column() {
        for (ExpressionType<?> type1 : supportedTypes()) {
            // Test supported types
            for (ExpressionType<?> type2 : supportedTypes()) {
                Class<? extends ExpressionBiValue> clazz = ExpressionBiValue.createBiClass(type1.typeName(), type2.typeName());

                checkColumn(
                    ExpressionBiValue.createBiValue(clazz, 0, type1.valueFrom(), type2.valueFrom()),
                    "" + type1.valueFrom() + type2.valueFrom()
                );

                checkColumn(
                    ExpressionBiValue.createBiValue(clazz, 0, null, type2.valueFrom()),
                    null
                );

                checkColumn(
                    ExpressionBiValue.createBiValue(clazz, 0, type1.valueFrom(), null),
                    null
                );

                checkColumn(
                    ExpressionBiValue.createBiValue(clazz, 0, null, null),
                    null
                );
            }
        }
    }

    private void checkColumn(ExpressionBiValue value, String expectedResult) {
        put(value);

        check("field1 || field2", expectedResult);
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

    private void check(String operands, String expectedResult, Object... params) {
        String sql = "SELECT " + operands + " FROM map";

        checkValueInternal(sql, SqlColumnType.VARCHAR, expectedResult, params);
    }

    private void checkFailure(String operands, int expectedErrorCode, String expectedErrorMessage, Object... params) {
        String sql = "SELECT " + operands + " FROM map";

        checkFailureInternal(sql, expectedErrorCode, expectedErrorMessage, params);
    }

    private static List<ExpressionType<?>> supportedTypes() {
        return Arrays.asList(
            BOOLEAN,
            BYTE,
            SHORT,
            INTEGER,
            LONG,
            BIG_DECIMAL,
            BIG_INTEGER,
            FLOAT,
            DOUBLE
        );
    }
}
