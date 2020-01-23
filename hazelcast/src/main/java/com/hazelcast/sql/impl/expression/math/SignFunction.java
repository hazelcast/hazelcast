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
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.UniCallExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.Converter;

public class SignFunction extends UniCallExpressionWithType<Number> {
    /** Operand type. */
    private transient DataType operandType;

    public SignFunction() {
        // No-op.
    }

    public SignFunction(Expression operand) {
        super(operand);
    }

    @Override
    public Number eval(QueryContext ctx, Row row) {
        Object operandValue = operand.eval(ctx, row);

        if (operandValue == null) {
            return null;
        }

        if (resType == null) {
            operandType = operand.getType();

            resType = inferResultType(operandType);
        }

        return doSign(operandValue, operandType, resType);
    }

    /**
     * Infer result type.
     *
     * @param operandType Operand type.
     * @return Result type.
     */
    private DataType inferResultType(DataType operandType) {
        if (!operandType.isCanConvertToNumeric()) {
            throw new HazelcastSqlException(-1, "Operand is not numeric: " + operandType);
        }

        switch (operandType.getType()) {
            case TINYINT:
            case INT:
            case SMALLINT:
                return DataType.INT;

            default:
                return operandType;
        }
    }

    /**
     * Get absolute value.
     *
     * @param operandValue Value.
     * @param operandType Operand type.
     * @param resType Type.
     * @return Absolute value of the target.
     */
    private static Number doSign(Object operandValue, DataType operandType, DataType resType) {
        Converter operandConverter = operandType.getConverter();

        switch (resType.getType()) {
            case INT:
                return Integer.signum(operandConverter.asInt(operandValue));

            case BIGINT:
                return Long.signum(operandConverter.asBigInt(operandValue));

            case DECIMAL:
                return operandConverter.asDecimal(operandValue).signum();

            case REAL:
                return Math.signum(operandConverter.asReal(operandValue));

            case DOUBLE:
                return Math.signum(operandConverter.asDouble(operandValue));

            default:
                throw new HazelcastSqlException(-1, "Unexpected type: " + resType);
        }
    }

    @Override
    public int operator() {
        return CallOperator.SIGN;
    }
}
