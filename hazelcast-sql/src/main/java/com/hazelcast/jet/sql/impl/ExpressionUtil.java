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

import com.hazelcast.function.ComparatorEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.opt.FieldCollation;
import com.hazelcast.jet.sql.impl.processors.JetSqlRow;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.rel.RelFieldCollation.NullDirection;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ExpressionUtil {

    /**
     * An {@link ExpressionEvalContext} that would fail if any dynamic
     * parameter (a.k.a. argument) or column expression requests its values.
     * <p>
     * Useful when evaluating expressions in planning phase where these are not
     * available.
     */
    public static final ExpressionEvalContext NOT_IMPLEMENTED_ARGUMENTS_CONTEXT = new ExpressionEvalContext() {

        @Override
        public Object getArgument(int index) {
            throw new IndexOutOfBoundsException("" + index);
        }

        @Override
        public List<Object> getArguments() {
            return Collections.emptyList();
        }

        @Override
        public InternalSerializationService getSerializationService() {
            return null;
        }
    };

    private ExpressionUtil() {
    }

    public static PredicateEx<JetSqlRow> filterFn(
            @Nonnull Expression<Boolean> predicate,
            @Nonnull ExpressionEvalContext context
    ) {
        return row0 -> {
            Row row = row0.getRow(context.getSerializationService());
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
                // - Get direction (ASCENDING, DESCENDING) and null direction
                //   (NULLS FIRST, NULLS LAST). If no null direction is given, then
                //   it will be inferred from the direction. Since NULL is sorted as
                //   the +Inf, ASCENDING implies NULL LAST whereas DESCENDING implies
                //   NULLS FIRST.
                // - Comparison of field values:
                //   - If both of them are NULL, then return 0.
                //   - Otherwise, if one of them is NULL, then return:
                //     - null direction value if LHS is NULL.
                //     - or negative null direction if RHS is NULL.
                //   - If none of them is NULL, then:
                //     - If direction is ASCENDING, then return the comparison result.
                //     - If direction is DESCENDING, return the negation of comparison result.
                int index = fieldCollation.getIndex();

                Comparable o1 = (Comparable) row1[index];
                Object o2 = row2[index];

                Direction direction = fieldCollation.getDirection();

                NullDirection nullDirection = fieldCollation.getNullDirection();
                if (nullDirection == null) {
                    nullDirection = direction.defaultNullDirection();
                }

                int result;
                if (o1 == null && o2 == null) {
                    result = 0;
                } else if (o1 == null) {
                    result = nullDirection.nullComparison;
                } else if (o2 == null) {
                    result = -nullDirection.nullComparison;
                } else {
                    result = o1.compareTo(o2);
                    result = direction.isDescending() ? -result : result;
                }

                if (result != 0) {
                    return result;
                }

            }
            return 0;
        };
    }

    public static FunctionEx<JetSqlRow, JetSqlRow> projectionFn(
            @Nonnull List<Expression<?>> projections,
            @Nonnull ExpressionEvalContext context
    ) {
        return row0 -> {
            Row row = row0.getRow(context.getSerializationService());
            JetSqlRow result = new JetSqlRow(projections.size());
            for (int i = 0; i < projections.size(); i++) {
                result.set(i, evaluate(projections.get(i), row, context));
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
    public static JetSqlRow join(
            @Nonnull JetSqlRow leftRow,
            @Nonnull JetSqlRow rightRow,
            @Nonnull Expression<Boolean> predicate,
            @Nonnull ExpressionEvalContext context
    ) {
        Object[] joined = Arrays.copyOf(leftRow.getValues(), leftRow.getFieldCount() + rightRow.getFieldCount());
        System.arraycopy(rightRow.getValues(), 0, joined, leftRow.getFieldCount(), rightRow.getFieldCount());

        JetSqlRow result = new JetSqlRow(joined);
        Row row = result.getRow(context.getSerializationService());
        return Boolean.TRUE.equals(evaluate(predicate, row, context)) ? result : null;
    }

    /**
     * Evaluate projection&predicate for multiple rows.
     */
    @Nonnull
    public static List<JetSqlRow> evaluate(
            @Nullable Expression<Boolean> predicate,
            @Nullable List<Expression<?>> projection,
            @Nonnull Stream<JetSqlRow> rows,
            @Nonnull ExpressionEvalContext context
    ) {
        return rows
                .map(row -> evaluate(predicate, projection, row, context))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Evaluate projection&predicate for a single row. Returns {@code null} if
     * the row is rejected by the predicate.
     */
    @Nullable
    public static JetSqlRow evaluate(
            @Nullable Expression<Boolean> predicate,
            @Nullable List<Expression<?>> projection,
            @Nonnull JetSqlRow values,
            @Nonnull ExpressionEvalContext context
    ) {
        Row row = values.getRow(context.getSerializationService());

        if (predicate != null && !Boolean.TRUE.equals(evaluate(predicate, row, context))) {
            return null;
        }

        if (projection == null) {
            return values;
        }

        JetSqlRow result = new JetSqlRow(projection.size());
        for (int i = 0; i < projection.size(); i++) {
            result.set(i, evaluate(projection.get(i), row, context));
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
