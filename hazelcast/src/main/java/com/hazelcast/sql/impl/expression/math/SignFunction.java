/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;

import java.math.BigDecimal;

public class SignFunction<T> extends UniExpressionWithType<T> implements IdentifiedDataSerializable {

    private static final BigDecimal DECIMAL_NEGATIVE = BigDecimal.ONE.negate();

    public SignFunction() {
        // No-op.
    }

    private SignFunction(Expression<?> operand, QueryDataType resultType) {
        super(operand, resultType);
    }

    public static SignFunction<?> create(Expression<?> operand, QueryDataType returnType) {
        return new SignFunction<>(operand, returnType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        Object operandValue = operand.eval(row, context);

        if (operandValue == null) {
            return null;
        }

        return (T) doSign(operandValue, operand.getType(), resultType);
    }

    private static Number doSign(Object operandValue, QueryDataType operandType, QueryDataType resultType) {
        Converter operandConverter = operandType.getConverter();

        switch (resultType.getTypeFamily()) {
            case TINYINT:
                return (byte) Integer.signum(operandConverter.asInt(operandValue));

            case SMALLINT:
                return (short) Integer.signum(operandConverter.asInt(operandValue));

            case INTEGER:
                return Integer.signum(operandConverter.asInt(operandValue));

            case BIGINT:
                return (long) Long.signum(operandConverter.asBigint(operandValue));

            case DECIMAL:
                int res = operandConverter.asDecimal(operandValue).signum();

                return res == 0 ? BigDecimal.ZERO : res == 1 ? BigDecimal.ONE : DECIMAL_NEGATIVE;

            case REAL:
                return Math.signum(operandConverter.asReal(operandValue));

            case DOUBLE:
                return Math.signum(operandConverter.asDouble(operandValue));

            default:
                throw QueryException.error("Unexpected type: " + resultType);
        }
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_SIGN;
    }
}
