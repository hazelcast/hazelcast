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
import com.hazelcast.sql.impl.expression.UniCallExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.DataTypeUtils;
import com.hazelcast.sql.impl.type.GenericType;
import com.hazelcast.sql.impl.type.accessor.Converter;

public class AbsFunction<T> extends UniCallExpressionWithType<T> {
    public AbsFunction() {
        // No-op.
    }

    private AbsFunction(Expression<?> operand, DataType resultType) {
        super(operand, resultType);
    }

    public static Expression<?> create(Expression<?> operand) {
        DataType operandType = operand.getType();

        if (operandType.getType() == GenericType.BIT) {
            // Bit alway remain the same, just coerce it.
            return CastExpression.coerce(operand, DataType.TINYINT);
        }

        return new AbsFunction<>(operand, inferResultType(operand.getType()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row) {
        Object operandValue = operand.eval(row);

        if (operandValue == null) {
            return null;
        }

        return (T) abs(operandValue, operand.getType(), resultType);
    }

    /**
     * Get absolute value.
     *
     * @param operand Value.
     * @param operandType Type of the operand.
     * @param resultType Result type.
     * @return Absolute value of the target.
     */
    private static Object abs(Object operand, DataType operandType, DataType resultType) {
        if (operandType.getType() == GenericType.LATE) {
            // Special handling for late binding.
            operandType = DataTypeUtils.resolveType(operand);

            if (operandType.getType() == GenericType.BIT) {
                // Bit alway remain the same, just coerce it.
                return CastExpression.coerce(operand, operandType, DataType.TINYINT);
            }

            resultType = inferResultType(operandType);
        }

        Converter operandConverter = operandType.getConverter();

        switch (resultType.getType()) {
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
    private static DataType inferResultType(DataType operandType) {
        if (!operandType.isNumeric()) {
            throw HazelcastSqlException.error("Operand is not numeric: " + operandType);
        }

        if (operandType.getType() == GenericType.VARCHAR) {
            return DataType.DECIMAL;
        }

        return operandType;
    }
}
