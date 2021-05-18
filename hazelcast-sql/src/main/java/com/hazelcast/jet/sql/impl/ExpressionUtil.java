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
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.Row;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
            throw new UnsupportedOperationException();
        }
    };

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
