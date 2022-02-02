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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.FunctionalPredicateExpression;
import com.hazelcast.sql.impl.expression.math.MultiplyFunction;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static com.hazelcast.jet.core.JetTestSupport.TEST_SS;
import static com.hazelcast.jet.impl.util.Util.toList;
import static com.hazelcast.jet.sql.SqlTestSupport.createExpressionEvalContext;
import static com.hazelcast.sql.impl.type.QueryDataType.BOOLEAN;
import static com.hazelcast.sql.impl.type.QueryDataType.INT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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
        JetSqlRow joined = ExpressionUtil.join(new JetSqlRow(TEST_SS, leftRow), new JetSqlRow(TEST_SS, rightRow), predicate, createExpressionEvalContext());

        assertThat(joined == null ? null : joined.getValues()).isEqualTo(expected);
    }

    @Test
    public void test_evaluate() {
        List<Object[]> rows = asList(new Object[]{0, "a"}, new Object[]{1, "b"});

        List<JetSqlRow> evaluated = ExpressionUtil.evaluate(null, null, rows.stream().map(v -> new JetSqlRow(TEST_SS, v)), createExpressionEvalContext());

        assertThat(toList(evaluated, JetSqlRow::getValues)).containsExactlyElementsOf(rows);
    }

    @Test
    public void test_evaluateWithPredicate() {
        List<Object[]> rows = asList(new Object[]{0, "a"}, new Object[]{1, "b"}, new Object[]{2, "c"});

        Expression<Boolean> predicate = new FunctionalPredicateExpression(row -> {
            int value = row.get(0);
            return value != 1;
        });

        List<JetSqlRow> evaluated = ExpressionUtil.evaluate(predicate, null, rows.stream().map(v -> new JetSqlRow(TEST_SS, v)), createExpressionEvalContext());

        assertThat(toList(evaluated, JetSqlRow::getValues)).containsExactly(new Object[]{0, "a"}, new Object[]{2, "c"});
    }

    @Test
    public void test_evaluateWithProjection() {
        List<Object[]> rows = asList(new Object[]{0, "a"}, new Object[]{1, "b"}, new Object[]{2, "c"});

        MultiplyFunction<?> projection =
                MultiplyFunction.create(ColumnExpression.create(0, INT), ConstantExpression.create(2, INT), INT);

        List<JetSqlRow> evaluated = ExpressionUtil.evaluate(null, singletonList(projection), rows.stream().map(v -> new JetSqlRow(TEST_SS, v)),
                mock(ExpressionEvalContext.class));

        assertThat(toList(evaluated, JetSqlRow::getValues)).containsExactly(new Object[]{0}, new Object[]{2}, new Object[]{4});
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

        List<JetSqlRow> evaluated = ExpressionUtil.evaluate(predicate, singletonList(projection), rows.stream().map(v -> new JetSqlRow(TEST_SS, v)),
                mock(ExpressionEvalContext.class));

        assertThat(toList(evaluated, JetSqlRow::getValues)).containsExactly(new Object[]{0}, new Object[]{4});
    }
}
