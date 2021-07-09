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

import com.hazelcast.function.ComparatorEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.jet.sql.impl.opt.FieldCollation;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.Row;
import org.apache.calcite.rel.RelFieldCollation.Direction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ExpressionUtil {

    private ExpressionUtil() {
    }

    public static PredicateEx<Object[]> filterFn(
            @Nonnull Expression<Boolean> predicate,
            @Nonnull ExpressionEvalContext context
    ) {
        return values -> {
            Row row = new HeapRow(values);
            return Boolean.TRUE.equals(evaluate(predicate, row, context));
        };
    }

    public static ComparatorEx<Object[]> comparisonFn(
            @Nonnull List<FieldCollation> fieldCollationList
    ) {
        return (Object[] row1, Object[] row2) -> {
            // Comparison of row values:
            // - Compare the rows according to field collations starting from left to right.
            // - If one of the field comparison returns the non-zero value, then return it.
            // - Otherwise, the rows are equal according to field collations, then return 0.
            for (FieldCollation fieldCollation : fieldCollationList) {
                // For each collation:
                // - Get collation index and use it to fetch values from the rows.
                // - Get direction (ASCENDING, DESCENDING)
                // - Comparison of field values:
                //   - If both of them are NULL, then result is 0.
                //   - Otherwise, if one of them is NULL, then return:
                //     - result is -1 if LHS is NULL.
                //     - result is 1 if RHS is NULL.
                // - Return the result if ASCENDING
                //   Return the reverted result if DESCENDING
                int index = fieldCollation.getIndex();

                Comparable o1 = (Comparable) row1[index];
                Object o2 = row2[index];

                Direction direction = fieldCollation.getDirection();

                int result;
                if (o1 == o2) {
                    result = 0;
                } else if (o1 == null) {
                    result = -1;
                } else if (o2 == null) {
                    result = 1;
                } else {
                    result = o1.compareTo(o2);
                }

                if (direction.isDescending()) {
                    if (result < 0) {
                        result = 1;
                    } else if (result > 0) {
                        result = -1;
                    }
                }

                if (result != 0) {
                    return result;
                }

            }
            return 0;
        };
    }

    public static FunctionEx<Object[], Object[]> projectionFn(
            @Nonnull List<Expression<?>> projections,
            @Nonnull ExpressionEvalContext context
    ) {
        return values -> {
            Row row = new HeapRow(values);
            Object[] result = new Object[projections.size()];
            for (int i = 0; i < projections.size(); i++) {
                result[i] = evaluate(projections.get(i), row, context);
            }
            return result;
        };
    }

    /**
     * Concatenates {@code leftRow} and {@code rightRow} into one, evaluates
     * the {@code predicate} on it, and if the predicate passed, returns the
     * joined row; returns {@code null} if the predicate didn't pass.
     */
    @Nullable
    public static Object[] join(
            @Nonnull Object[] leftRow,
            @Nonnull Object[] rightRow,
            @Nonnull Expression<Boolean> predicate,
            @Nonnull ExpressionEvalContext context
    ) {
        Object[] joined = Arrays.copyOf(leftRow, leftRow.length + rightRow.length);
        System.arraycopy(rightRow, 0, joined, leftRow.length, rightRow.length);

        Row row = new HeapRow(joined);
        return Boolean.TRUE.equals(evaluate(predicate, row, context)) ? joined : null;
    }

    /**
     * Evaluate projection&predicate for multiple rows.
     */
    @Nonnull
    public static List<Object[]> evaluate(
            @Nullable Expression<Boolean> predicate,
            @Nullable List<Expression<?>> projection,
            @Nonnull List<Object[]> rows,
            @Nonnull ExpressionEvalContext context
    ) {
        List<Object[]> evaluatedRows = new ArrayList<>();
        for (Object[] values : rows) {
            Object[] transformed = evaluate(predicate, projection, values, context);
            if (transformed != null) {
                evaluatedRows.add(transformed);
            }
        }
        return evaluatedRows;
    }

    /**
     * Evaluate projection&predicate for a single row. Returns {@code null} if
     * the row is rejected by the predicate.
     */
    @Nullable
    public static Object[] evaluate(
            @Nullable Expression<Boolean> predicate,
            @Nullable List<Expression<?>> projection,
            @Nonnull Object[] values,
            @Nonnull ExpressionEvalContext context
    ) {
        Row row = new HeapRow(values);

        if (predicate != null && !Boolean.TRUE.equals(evaluate(predicate, row, context))) {
            return null;
        }

        if (projection == null) {
            return values;
        }

        Object[] result = new Object[projection.size()];
        for (int i = 0; i < projection.size(); i++) {
            result[i] = evaluate(projection.get(i), row, context);
        }
        return result;
    }

    public static <T> T evaluate(
            @Nonnull Expression<T> expression,
            @Nonnull Row row,
            @Nonnull ExpressionEvalContext context
    ) {
        return expression.eval(row, context);
    }
}
