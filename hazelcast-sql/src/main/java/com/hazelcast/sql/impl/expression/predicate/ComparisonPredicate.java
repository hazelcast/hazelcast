/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.sql.impl.expression.predicate;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.Comparables;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.BiExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Objects;

/**
 * Implements evaluation of SQL comparison predicates.
 *
 * @see ComparisonMode
 */
public final class ComparisonPredicate extends BiExpression<Boolean> {

    private ComparisonMode mode;

    public ComparisonPredicate() {
        // No-op.
    }

    private ComparisonPredicate(Expression<?> left, Expression<?> right, ComparisonMode mode) {
        super(left, right);
        this.mode = mode;
    }

    public static ComparisonPredicate create(Expression<?> left, Expression<?> right, ComparisonMode comparisonMode) {
        assert left.getType().getTypeFamily() == right.getType().getTypeFamily();
        return new ComparisonPredicate(left, right, comparisonMode);
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_COMPARISON;
    }

    @SuppressWarnings({"rawtypes"})
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

        if (this.operand1.getType().getTypeFamily() == QueryDataTypeFamily.OBJECT) {
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

        int order = Comparables.compare(leftComparable, rightComparable);

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
