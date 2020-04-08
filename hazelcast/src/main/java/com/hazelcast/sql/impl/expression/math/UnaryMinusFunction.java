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

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.converter.Converter;

import java.math.BigDecimal;

/**
 * Unary minus operation.
 */
public class UnaryMinusFunction<T> extends UniExpressionWithType<T> {

    @SuppressWarnings("unused")
    public UnaryMinusFunction() {
        // No-op.
    }

    private UnaryMinusFunction(Expression<?> operand, QueryDataType resultType) {
        super(operand, resultType);
    }

    public static UnaryMinusFunction<?> create(Expression<?> operand) {
        return new UnaryMinusFunction<>(operand, inferResultType(operand.getType()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        Object value = operand.eval(row, context);

        if (value == null) {
            return null;
        }

        return (T) doMinus(value, operand.getType(), resultType);
    }

    @Override
    public QueryDataType getType() {
        return operand.getType();
    }

    private static Object doMinus(Object operandValue, QueryDataType operandType, QueryDataType resultType) {
        Converter operandConverter = operandType.getConverter();

        switch (resultType.getTypeFamily()) {
            case TINYINT:
                return (byte) (-operandConverter.asTinyint(operandValue));

            case SMALLINT:
                return (short) (-operandConverter.asSmallint(operandValue));

            case INT:
                return -operandConverter.asInt(operandValue);

            case BIGINT:
                return -operandConverter.asBigint(operandValue);

            case DECIMAL:
                BigDecimal opDecimal = operandConverter.asDecimal(operandValue);

                return opDecimal.negate();

            case REAL:
                return -operandConverter.asReal(operandValue);

            case DOUBLE:
                return -operandConverter.asDouble(operandValue);

            default:
                throw QueryException.error("Invalid type: " + resultType);
        }
    }

    private static QueryDataType inferResultType(QueryDataType operandType) {
        if (!MathFunctionUtils.canConvertToNumber(operandType)) {
            throw QueryException.error("Operand is not numeric: " + operandType);
        }

        if (operandType.getTypeFamily() == QueryDataTypeFamily.VARCHAR) {
            return QueryDataType.DECIMAL;
        }

        return MathFunctionUtils.expandPrecision(operandType);
    }

}
