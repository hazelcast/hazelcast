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
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.UniCallExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.math.BigDecimal;

import static com.hazelcast.sql.impl.type.DataType.PRECISION_UNLIMITED;

/**
 * Unary minus operation.
 */
public class UnaryMinusFunction<T> extends UniCallExpressionWithType<T> {
    /** Operand type. */
    private transient DataType operandType;

    public UnaryMinusFunction() {
        // No-op.
    }

    public UnaryMinusFunction(Expression operand) {
        super(operand);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(QueryContext ctx, Row row) {
        Object operandValue = operand.eval(ctx, row);

        if (operandValue == null) {
            return null;
        }

        if (resType == null) {
            operandType = operand.getType();

            resType = inferResultType(operandType);
        }

        return (T) doMinus(operandValue, operandType, resType);
    }

    /**
     * Infer result type.
     *
     * @param operandType Operand type.
     * @return Result type.
     */
    private DataType inferResultType(DataType operandType) {
        if (!operandType.isCanConvertToNumeric()) {
            throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand 1 is not numeric.");
        }

        if (operandType == DataType.VARCHAR) {
            operandType = DataType.DECIMAL;
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

    /**
     * Execute unary minus operation.
     *
     * @param operandValue Operand value.
     * @param operandType Operand type.
     * @param resType Result type.
     * @return Result.
     */
    @SuppressWarnings("unchecked")
    private static Object doMinus(Object operandValue, DataType operandType, DataType resType) {
        Converter operandConverter = operandType.getConverter();

        switch (resType.getType()) {
            case TINYINT:
                return (byte) (-operandConverter.asTinyInt(operandValue));

            case SMALLINT:
                return (short) (-operandConverter.asSmallInt(operandValue));

            case INT:
                return -operandConverter.asInt(operandValue);

            case BIGINT:
                return -operandConverter.asBigInt(operandValue);

            case DECIMAL:
                BigDecimal opDecimal = operandConverter.asDecimal(operandValue);

                return opDecimal.negate();

            case REAL:
                return -operandConverter.asReal(operandValue);

            case DOUBLE:
                return -operandConverter.asDouble(operandValue);

            default:
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Invalid type: " + resType);
        }
    }

    @Override
    public int operator() {
        return CallOperator.UNARY_MINUS;
    }
}
