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
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;

public class SignFunction extends UniExpressionWithType<Number> {

    @SuppressWarnings("unused")
    public SignFunction() {
        // No-op.
    }

    private SignFunction(Expression<?> operand, QueryDataType resultType) {
        super(operand, resultType);
    }

    public static SignFunction create(Expression<?> operand) {
        return new SignFunction(operand, inferResultType(operand.getType()));
    }

    @Override
    public Number eval(Row row, ExpressionEvalContext context) {
        Object operandValue = operand.eval(row, context);

        if (operandValue == null) {
            return null;
        }

        return doSign(operandValue, operand.getType(), resultType);
    }

    private static Number doSign(Object operandValue, QueryDataType operandType, QueryDataType resultType) {
        Converter operandConverter = operandType.getConverter();

        switch (resultType.getTypeFamily()) {
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
    private static QueryDataType inferResultType(QueryDataType operandType) {
        if (!MathFunctionUtils.canConvertToNumber(operandType)) {
            throw HazelcastSqlException.error("Operand is not numeric: " + operandType);
        }

        switch (operandType.getTypeFamily()) {
            case BIT:
                return QueryDataType.TINYINT;

            case VARCHAR:
                return QueryDataType.DECIMAL;

            default:
                break;
        }

        return operandType;
    }

}
