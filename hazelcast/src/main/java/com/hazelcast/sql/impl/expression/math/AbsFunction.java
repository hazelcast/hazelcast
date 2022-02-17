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
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;

import static com.hazelcast.sql.impl.expression.math.ExpressionMath.DECIMAL_MATH_CONTEXT;

public class AbsFunction<T> extends UniExpressionWithType<T> implements IdentifiedDataSerializable {
    @SuppressWarnings("unused")
    public AbsFunction() {
        // No-op.
    }

    private AbsFunction(Expression<?> operand, QueryDataType resultType) {
        super(operand, resultType);
    }

    public static AbsFunction<?> create(Expression<?> operand, QueryDataType resultType) {
        return new AbsFunction<>(operand, resultType);
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

            case INTEGER:
                return Math.abs(operandConverter.asInt(operand));

            case BIGINT:
                long res = Math.abs(operandConverter.asBigint(operand));

                if (res < 0) {
                    throw QueryException.error(SqlErrorCode.DATA_EXCEPTION,
                        "BIGINT overflow in ABS function (consider adding an explicit CAST to DECIMAL)");
                }

                return res;

            case DECIMAL:
                return operandConverter.asDecimal(operand).abs(DECIMAL_MATH_CONTEXT);

            case REAL:
                return Math.abs(operandConverter.asReal(operand));

            case DOUBLE:
                return Math.abs(operandConverter.asDouble(operand));

            default:
                throw QueryException.error("Unexpected result type: " + resultType);
        }
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_ABS;
    }
}
