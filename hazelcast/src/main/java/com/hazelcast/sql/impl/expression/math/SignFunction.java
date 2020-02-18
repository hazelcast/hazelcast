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

public class SignFunction extends UniCallExpressionWithType<Number> {
    public SignFunction() {
        // No-op.
    }

    private SignFunction(Expression<?> operand, DataType resultType) {
        super(operand, resultType);
    }

    public static SignFunction create(Expression<?> operand) {
        return new SignFunction(operand, inferResultType(operand.getType()));
    }

    @Override
    public Number eval(Row row) {
        Object operandValue = operand.eval(row);

        if (operandValue == null) {
            return null;
        }

        return doSign(operandValue, operand.getType(), resultType);
    }

    /**
     * Get absolute value.
     *
     * @param operandValue Value.
     * @param operandType Operand type.
     * @param resultType Type.
     * @return Absolute value of the target.
     */
    private static Number doSign(Object operandValue, DataType operandType, DataType resultType) {
        if (resultType.getType() == GenericType.LATE) {
            // Special handling for late binding.
            operandType = DataTypeUtils.resolveType(operandValue);

            resultType = inferResultType(operandType);
        }

        Converter operandConverter = operandType.getConverter();

        switch (resultType.getType()) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INT:
                return Integer.signum(operandConverter.asInt(operandValue));

            case BIGINT:
                return Long.signum(operandConverter.asBigint(operandValue));

            case DECIMAL:
                return operandConverter.asDecimal(operandValue).signum();

            case REAL:
                return Math.signum(operandConverter.asReal(operandValue));

            case DOUBLE:
                return Math.signum(operandConverter.asDouble(operandValue));

            default:
                throw HazelcastSqlException.error("Unexpected type: " + resultType);
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

            default:
                break;
        }

        return operandType;
    }
}
