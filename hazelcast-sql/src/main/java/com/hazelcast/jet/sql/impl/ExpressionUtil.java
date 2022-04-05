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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.row.Row;
import org.apache.calcite.rel.RelFieldCollation.Direction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ExpressionUtil {

    private ExpressionUtil() {
    }

    public static PredicateEx<JetSqlRow> filterFn(
            @Nonnull Expression<Boolean> predicate,
            @Nonnull ExpressionEvalContext context
    ) {
        return row0 -> {
            Row row = row0.getRow();
            return Boolean.TRUE.equals(evaluate(predicate, row, context));
        };
    }

    public static ComparatorEx<JetSqlRow> comparisonFn(
            @Nonnull List<FieldCollation> fieldCollationList
    ) {
        return new SqlRowComparator(fieldCollationList);
    }

    public static class SqlRowComparator implements IdentifiedDataSerializable, ComparatorEx<JetSqlRow> {
        private List<FieldCollation> fieldCollationList;

        public SqlRowComparator() {
        }

        public SqlRowComparator(List<FieldCollation> fieldCollationList) {
            this.fieldCollationList = fieldCollationList;
        }

        @Override
        public int compareEx(JetSqlRow row1, JetSqlRow row2) {
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

                Comparable o1 = (Comparable) row1.get(index);
                Object o2 = row2.get(index);

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
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(fieldCollationList.size());
            for (FieldCollation fieldCollation : fieldCollationList) {
                out.writeObject(fieldCollation);
            }
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            int size = in.readInt();
            fieldCollationList = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                fieldCollationList.add(in.readObject());
            }
        }

        @Override
        public int getFactoryId() {
            return JetSqlSerializerHook.F_ID;
        }

        @Override
        public int getClassId() {
            return JetSqlSerializerHook.SQL_ROW_COMPARATOR;
        }
    }

    public static FunctionEx<JetSqlRow, JetSqlRow> projectionFn(
            @Nonnull List<Expression<?>> projections,
            @Nonnull ExpressionEvalContext context
    ) {
        return row0 -> {
            Row row = row0.getRow();
            Object[] result = new Object[projections.size()];
            for (int i = 0; i < projections.size(); i++) {
                result[i] = evaluate(projections.get(i), row, context);
            }
            return new JetSqlRow(context.getSerializationService(), result);
        };
    }

    public static FunctionEx<JetSqlRow, JetSqlRow> calcFn(
            @Nonnull List<Expression<?>> projections,
            @Nonnull Expression<Boolean> predicate,
            @Nonnull ExpressionEvalContext context
    ) {
        return row0 -> {
            Row row = row0.getRow();
            if (Boolean.TRUE.equals(evaluate(predicate, row, context))) {
                Object[] result = new Object[projections.size()];
                for (int i = 0; i < projections.size(); i++) {
                    result[i] = evaluate(projections.get(i), row, context);
                }
                return new JetSqlRow(context.getSerializationService(), result);
            } else {
                return null;
            }
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

        JetSqlRow result = new JetSqlRow(context.getSerializationService(), joined);
        Row row = result.getRow();
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
        Row row = values.getRow();

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
        return new JetSqlRow(context.getSerializationService(), result);
    }

    /**
     * Evaluate projection&predicate for a single row. Returns {@code null} if
     * the row is rejected by the predicate.
     */
    @Nullable
    public static JetSqlRow evaluate(
            @Nullable Expression<Boolean> predicate,
            @Nonnull List<Expression<?>> projection,
            @Nonnull Row row,
            @Nonnull ExpressionEvalContext context
    ) {
        if (predicate != null && !Boolean.TRUE.equals(evaluate(predicate, row, context))) {
            return null;
        }

        Object[] result = new Object[projection.size()];
        for (int i = 0; i < projection.size(); i++) {
            result[i] = evaluate(projection.get(i), row, context);
        }
        return new JetSqlRow(context.getSerializationService(), result);
    }

    public static Object evaluate(
            @Nonnull Expression<?> expression,
            @Nonnull Row row,
            @Nonnull ExpressionEvalContext context
    ) {
        return expression.evalTop(row, context);
    }
}
