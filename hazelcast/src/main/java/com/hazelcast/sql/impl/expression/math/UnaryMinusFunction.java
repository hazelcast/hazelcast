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
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.UniCallExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.DataTypeUtils;
import com.hazelcast.sql.impl.type.GenericType;
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.math.BigDecimal;

import static com.hazelcast.sql.impl.type.DataType.PRECISION_UNLIMITED;

/**
 * Unary minus operation.
 */
public class UnaryMinusFunction<T> extends UniCallExpressionWithType<T> {
    public UnaryMinusFunction() {
        // No-op.
    }

    private UnaryMinusFunction(Expression<?> operand, DataType resultType) {
        super(operand, resultType);
    }

    public static UnaryMinusFunction<?> create(Expression<?> operand) {
        return new UnaryMinusFunction<>(operand, inferResultType(operand.getType()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row) {
        Object value = operand.eval(row);

        if (value == null) {
            return null;
        }

        return (T) doMinus(value, operand.getType(), resultType);
    }

    @Override
    public DataType getType() {
        return operand.getType();
    }

    /**
     * Execute unary minus operation.
     *
     * @param operandValue Operand value.
     * @param operandType Operand type.
     * @param resultType Result type.
     * @return Result.
     */
    private static Object doMinus(Object operandValue, DataType operandType, DataType resultType) {
        if (resultType.getType() == GenericType.LATE) {
            // Special handling for late binding.
            operandType = DataTypeUtils.resolveType(operandValue);

            resultType = inferResultType(operandType);
        }

        Converter operandConverter = operandType.getConverter();

        switch (resultType.getType()) {
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
                throw HazelcastSqlException.error("Invalid type: " + resultType);
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

        switch (operandType.getType()) {
            case BIT:
                return DataType.TINYINT;

            case VARCHAR:
                return DataType.DECIMAL;

            case LATE:
                return DataType.LATE;

            default:
                break;
        }

        if (operandType.getScale() == 0) {
            // Integer type.
            int precision = operandType.getPrecision();

            if (precision != PRECISION_UNLIMITED) {
                precision++;
            }

            return DataType.integerType(precision);
        } else {
            // DECIMAL, REAL or DOUBLE. REAL is expanded to DOUBLE. DECIMAL and DOUBLE are already the widest.
            return operandType == DataType.REAL ? DataType.DOUBLE : operandType;
        }
    }
}
