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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.BiExpressionWithType;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.math.BigDecimal;

import static com.hazelcast.sql.impl.type.QueryDataTypeUtils.DECIMAL_MATH_CONTEXT;

/**
 * Implements evaluation of SQL divide operator.
 */
public final class DivideFunction<T> extends BiExpressionWithType<T> {

    public DivideFunction() {
        // No-op.
    }

    private DivideFunction(Expression<?> operand1, Expression<?> operand2, QueryDataType resultType) {
        super(operand1, operand2, resultType);
    }

    public static DivideFunction<?> create(Expression<?> operand1, Expression<?> operand2, QueryDataType resultType) {
        return new DivideFunction<>(operand1, operand2, resultType);
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_DIVIDE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        Object left = operand1.eval(row, context);
        if (left == null) {
            return null;
        }

        Object right = operand2.eval(row, context);
        if (right == null) {
            return null;
        }

        QueryDataTypeFamily family = resultType.getTypeFamily();
        if (family.isTemporal()) {
            throw new UnsupportedOperationException("temporal types are unsupported currently");
        }

        return (T) evalNumeric((Number) left, (Number) right, family);
    }

    private static Object evalNumeric(Number left, Number right, QueryDataTypeFamily family) {
        try {
            switch (family) {
                case TINYINT:
                    return (byte) (left.byteValue() / right.longValue());
                case SMALLINT:
                    return (short) (left.shortValue() / right.longValue());
                case INTEGER:
                    return (int) (left.intValue() / right.longValue());
                case BIGINT:
                    return ExpressionMath.divideExact(left.longValue(), right.longValue());
                case REAL:
                    return ExpressionMath.divideExact(left.floatValue(), right.floatValue());
                case DOUBLE:
                    return ExpressionMath.divideExact(left.doubleValue(), right.doubleValue());
                case DECIMAL:
                    return ((BigDecimal) left).divide((BigDecimal) right, DECIMAL_MATH_CONTEXT);
                default:
                    throw new IllegalArgumentException("Unexpected result family: " + family);
            }
        } catch (ArithmeticException e) {
            throw QueryException.error(SqlErrorCode.DATA_EXCEPTION, "Division by zero", e);
        }
    }

}
