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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.processors.JetSqlRow;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;

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
