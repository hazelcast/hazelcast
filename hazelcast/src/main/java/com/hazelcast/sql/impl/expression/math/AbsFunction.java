/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression.math;

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.expression.CastExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.converter.Converter;

public class AbsFunction<T> extends UniExpressionWithType<T> {

    @SuppressWarnings("unused")
    public AbsFunction() {
        // No-op.
    }

    private AbsFunction(Expression<?> operand, QueryDataType resultType) {
        super(operand, resultType);
    }

    public static Expression<?> create(Expression<?> operand) {
        QueryDataType operandType = operand.getType();

        if (operandType.getTypeFamily() == QueryDataTypeFamily.BIT) {
            // Bit always remain the same, just coerce it.
            return CastExpression.coerceExpression(operand, QueryDataTypeFamily.TINYINT);
        }

        return new AbsFunction<>(operand, inferResultType(operand.getType()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        Object operandValue = operand.eval(row, context);

        if (operandValue == null) {
            return null;
        }

        return (T) abs(operandValue, operand.getType(), resultType);
    }

    private static Object abs(Object operand, QueryDataType operandType, QueryDataType resultType) {
        Converter operandConverter = operandType.getConverter();

        switch (resultType.getTypeFamily()) {
            case TINYINT:
                return (byte) Math.abs(operandConverter.asTinyint(operand));

            case SMALLINT:
                return (short) Math.abs(operandConverter.asSmallint(operand));

            case INT:
                return Math.abs(operandConverter.asInt(operand));

            case BIGINT:
                return Math.abs(operandConverter.asBigint(operand));

            case DECIMAL:
                return operandConverter.asDecimal(operand).abs();

            case REAL:
                return Math.abs(operandConverter.asReal(operand));

            case DOUBLE:
                return Math.abs(operandConverter.asDouble(operand));

            default:
                throw HazelcastSqlException.error("Unexpected result type: " + resultType);
        }
    }

    /**
     * Infer result type.
     *
     * @param operandType Operand type.
     * @return Result type.
     */
    private static QueryDataType inferResultType(QueryDataType operandType) {
        if (!MathFunctionUtils.canConvertToNumber(operandType)) {
            throw HazelcastSqlException.error("Operand is not numeric: " + operandType);
        }

        if (operandType.getTypeFamily() == QueryDataTypeFamily.VARCHAR) {
            return QueryDataType.DECIMAL;
        }

        return operandType;
    }

}
