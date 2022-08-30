/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.predicate;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.BiExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.RowValue;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Objects;

import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.OBJECT;
import static com.hazelcast.sql.impl.type.QueryDataTypeFamily.ROW;

/**
 * Implements evaluation of SQL comparison predicates.
 *
 * @see ComparisonMode
 */
public final class ComparisonPredicate extends BiExpression<Boolean> implements IdentifiedDataSerializable {

    private ComparisonMode mode;

    public ComparisonPredicate() {
        // No-op.
    }

    private ComparisonPredicate(Expression<?> left, Expression<?> right, ComparisonMode mode) {
        super(left, right);
        this.mode = mode;
    }

    public static ComparisonPredicate create(Expression<?> left, Expression<?> right, ComparisonMode comparisonMode) {
        assert left.getType().equals(right.getType());
        return new ComparisonPredicate(left, right, comparisonMode);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_COMPARISON;
    }

    @SuppressWarnings({
            "rawtypes",
            "unchecked",
            "checkstyle:CyclomaticComplexity",
            "checkstyle:NPathComplexity",
            "checkstyle:ReturnCount"
    })
    @SuppressFBWarnings(value = "NP_BOOLEAN_RETURN_NULL", justification = "Any SQL expression may return null")
    @Override
    public Boolean eval(Row row, ExpressionEvalContext context) {
        Object left = operand1.eval(row, context);
        if (left == null) {
            return null;
        }

        Object right = operand2.eval(row, context);
        if (right == null) {
            return null;
        }

        if (operand1.getType().getTypeFamily().equals(ROW)
                || operand2.getType().getTypeFamily().equals(ROW)) {
            if (operand1.getType().getTypeFamily() != operand2.getType().getTypeFamily()) {
                throw QueryException.error(operand1.getType().getTypeFamily() + " can not be compared to "
                        + operand2.getType().getTypeFamily());
            }

            return compareRows((RowValue) left, (RowValue) right);
        }

        if (this.operand1.getType().getTypeFamily() == OBJECT) {
            Class<?> leftClass = left.getClass();
            Class<?> rightClass = right.getClass();

            if (!leftClass.equals(rightClass)) {
                throw QueryException.error(
                        "Cannot compare two OBJECT values, because "
                                + "left operand has " + leftClass + " type and "
                                + "right operand has " + rightClass + " type");
            }

            if (!(left instanceof Comparable)) {
                throw QueryException.error(
                        "Cannot compare OBJECT value because " + leftClass + " doesn't implement Comparable interface");
            }

        }

        Comparable leftComparable = (Comparable) left;
        Comparable rightComparable = (Comparable) right;

        int order = leftComparable.compareTo(rightComparable);

        switch (mode) {
            case EQUALS:
                return order == 0;

            case NOT_EQUALS:
                return order != 0;

            case GREATER_THAN:
                return order > 0;

            case GREATER_THAN_OR_EQUAL:
                return order >= 0;

            case LESS_THAN:
                return order < 0;

            case LESS_THAN_OR_EQUAL:
                return order <= 0;

            default:
                throw new IllegalStateException("unexpected comparison mode: " + mode);
        }
    }

    private boolean compareRows(RowValue left, RowValue right) {
        if (left.getValues().size() != right.getValues().size()) {
            return false;
        }

        for (int i = 0; i < left.getValues().size(); i++) {
            final Object leftVal = left.getValues().get(i);
            final Object rightVal = right.getValues().get(i);

            if (Objects.equals(leftVal, rightVal)) {
                continue;
            }

            if (leftVal == null || rightVal == null) {
                return false;
            }

            if (leftVal instanceof RowValue && rightVal instanceof RowValue) {
                if (!compareRows((RowValue) leftVal, (RowValue) rightVal)) {
                    return false;
                } else {
                    continue;
                }
            }

            final Converter leftConverter = Converters.getConverter(leftVal.getClass());
            final Converter rightConverter = Converters.getConverter(rightVal.getClass());

            if (!leftConverter.canConvertTo(rightConverter.getTypeFamily())) {
                return false;
            }

            final Object newRightVal = leftConverter.convertToSelf(rightConverter, rightVal);
            if (!leftVal.equals(newRightVal)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.BOOLEAN;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(mode.getId());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        mode = ComparisonMode.getById(in.readInt());
    }

    @Override
    public int hashCode() {
        return Objects.hash(operand1, operand2, mode);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ComparisonPredicate that = (ComparisonPredicate) o;

        return Objects.equals(operand1, that.operand1) && Objects.equals(operand2, that.operand2) && mode == that.mode;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{mode=" + mode + ", operand1=" + operand1 + ", operand2=" + operand2 + '}';
    }

}
