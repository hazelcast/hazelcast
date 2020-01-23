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
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.UniCallExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.Converter;

public class AbsFunction extends UniCallExpressionWithType<Number> {
    /** Operand type. */
    private transient DataType operandType;

    public AbsFunction() {
        // No-op.
    }

    public AbsFunction(Expression operand) {
        super(operand);
    }

    @Override
    public Number eval(Row row) {
        Object operandValue = operand.eval(row);

        if (operandValue == null) {
            return null;
        }

        if (resType == null) {
            DataType type = operand.getType();

            if (!type.isCanConvertToNumeric()) {
                throw new HazelcastSqlException(-1, "Operand is not numeric: " + operandValue);
            }

            if (type == DataType.BIT) {
                resType = DataType.TINYINT;
            } else if (type == DataType.DECIMAL_SCALE_0_BIG_INTEGER) {
                resType = DataType.DECIMAL_SCALE_0_BIG_DECIMAL;
            } else if (type == DataType.VARCHAR) {
                resType = DataType.DECIMAL;
            } else {
                resType = type;
            }

            operandType = type;
        }

        return abs(operandValue, operandType, resType);
    }

    /**
     * Get absolute value.
     *
     * @param operand Value.
     * @param operandType Type of the operand.
     * @param resType Result type.
     * @return Absolute value of the target.
     */
    private Number abs(Object operand, DataType operandType, DataType resType) {
        Converter operandConverter = operandType.getConverter();

        switch (resType.getType()) {
            case TINYINT:
                return (byte) Math.abs(operandConverter.asTinyInt(operand));

            case SMALLINT:
                return (short) Math.abs(operandConverter.asSmallInt(operand));

            case INT:
                return Math.abs(operandConverter.asInt(operand));

            case BIGINT:
                return Math.abs(operandConverter.asBigInt(operand));

            case DECIMAL:
                return operandConverter.asDecimal(operand).abs();

            case REAL:
                return Math.abs(operandConverter.asReal(operand));

            case DOUBLE:
                return Math.abs(operandConverter.asDouble(operand));

            default:
                throw new HazelcastSqlException(-1, "Unexpected result type: " + resType);
        }
    }

    @Override
    public int operator() {
        return CallOperator.ABS;
    }
}
