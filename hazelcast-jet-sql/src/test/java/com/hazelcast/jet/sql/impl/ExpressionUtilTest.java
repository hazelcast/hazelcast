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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.FunctionalPredicateExpression;
import com.hazelcast.sql.impl.expression.math.MultiplyFunction;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.jet.sql.impl.ExpressionUtil.NOT_IMPLEMENTED_ARGUMENTS_CONTEXT;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class ExpressionUtilTest {

    @SuppressWarnings("unchecked")
    @Test
    public void test_join_1() {
        test_join(
                (Expression<Boolean>) ConstantExpression.create(true, BOOLEAN),
                new Object[]{1},
                new Object[]{2},
                new Object[]{1, 2});
    }

    @SuppressWarnings("unchecked")
    @Test
    public void test_join_2() {
        test_join(
                (Expression<Boolean>) ConstantExpression.create(true, BOOLEAN),
                new Object[]{1},
                new Object[]{2},
                new Object[]{1, 2});
    }

    @Test
    public void test_join_3() {
        test_join(
                ComparisonPredicate.create(
                        ColumnExpression.create(0, INT),
                        ColumnExpression.create(1, INT),
                        ComparisonMode.GREATER_THAN),
                new Object[]{1},
                new Object[]{2},
                null);
    }

    @Test
    public void test_join_4() {
        test_join(
                ComparisonPredicate.create(
                        ColumnExpression.create(0, INT),
                        ColumnExpression.create(1, INT),
                        ComparisonMode.LESS_THAN),
                new Object[]{2},
                new Object[]{1},
                null);
    }

    private void test_join(Expression<Boolean> predicate, Object[] leftRow, Object[] rightRow, Object[] expected) {
        Object[] joined = ExpressionUtil.join(leftRow, rightRow, predicate, NOT_IMPLEMENTED_ARGUMENTS_CONTEXT);

        assertThat(joined).isEqualTo(expected);
    }

    @Test
    public void test_evaluate() {
        List<Object[]> rows = asList(new Object[]{0, "a"}, new Object[]{1, "b"});

        List<Object[]> evaluated = ExpressionUtil.evaluate(null, null, rows, NOT_IMPLEMENTED_ARGUMENTS_CONTEXT);

        assertThat(evaluated).containsExactlyElementsOf(rows);
    }

    @Test
    public void test_evaluateWithPredicate() {
        List<Object[]> rows = asList(new Object[]{0, "a"}, new Object[]{1, "b"}, new Object[]{2, "c"});

        Expression<Boolean> predicate = new FunctionalPredicateExpression(row -> {
            int value = row.get(0);
            return value != 1;
        });

        List<Object[]> evaluated = ExpressionUtil.evaluate(predicate, null, rows, NOT_IMPLEMENTED_ARGUMENTS_CONTEXT);

        assertThat(evaluated).containsExactly(new Object[]{0, "a"}, new Object[]{2, "c"});
    }

    @Test
    public void test_evaluateWithProjection() {
        List<Object[]> rows = asList(new Object[]{0, "a"}, new Object[]{1, "b"}, new Object[]{2, "c"});

        MultiplyFunction<?> projection =
                MultiplyFunction.create(ColumnExpression.create(0, INT), ConstantExpression.create(2, INT), INT);

        List<Object[]> evaluated = ExpressionUtil.evaluate(null, singletonList(projection), rows,
                NOT_IMPLEMENTED_ARGUMENTS_CONTEXT);

        assertThat(evaluated).containsExactly(new Object[]{0}, new Object[]{2}, new Object[]{4});
    }

    @Test
    public void test_evaluateWithPredicateAndProjection() {
        List<Object[]> rows = asList(new Object[]{0, "a"}, new Object[]{1, "b"}, new Object[]{2, "c"});

        Expression<Boolean> predicate = new FunctionalPredicateExpression(row -> {
            int value = row.get(0);
            return value != 1;
        });
        MultiplyFunction<?> projection =
                MultiplyFunction.create(ColumnExpression.create(0, INT), ConstantExpression.create(2, INT), INT);

        List<Object[]> evaluated = ExpressionUtil.evaluate(predicate, singletonList(projection), rows,
                NOT_IMPLEMENTED_ARGUMENTS_CONTEXT);

        assertThat(evaluated).containsExactly(new Object[]{0}, new Object[]{4});
    }
}
