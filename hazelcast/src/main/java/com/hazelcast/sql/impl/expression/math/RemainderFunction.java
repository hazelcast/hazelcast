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
import com.hazelcast.sql.impl.expression.BiExpressionWithType;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;

import java.math.BigDecimal;

/**
 * Division.
 */
public class RemainderFunction<T> extends BiExpressionWithType<T> {

    @SuppressWarnings("unused")
    public RemainderFunction() {
        // No-op.
    }

    private RemainderFunction(Expression<?> operand1, Expression<?> operand2, QueryDataType resultType) {
        super(operand1, operand2, resultType);
    }

    public static RemainderFunction<?> create(Expression<?> operand1, Expression<?> operand2) {
        QueryDataType resultType = MathFunctionUtils.inferRemainderResultType(operand1.getType(), operand2.getType());

        return new RemainderFunction<>(operand1, operand2, resultType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        Object operand1Value = operand1.eval(row, context);

        if (operand1Value == null) {
            return null;
        }

        Object operand2Value = operand2.eval(row, context);

        if (operand2Value == null) {
            return null;
        }

        return (T) doRemainder(operand1Value, operand1.getType(), operand2Value, operand2.getType(), resultType);
    }

    private static Object doRemainder(Object operand1, QueryDataType operand1Type, Object operand2, QueryDataType operand2Type,
                                      QueryDataType resultType) {
        // Handle numeric.
        return doRemainderNumeric(operand1, operand1Type, operand2, operand2Type, resultType);
    }

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    private static Object doRemainderNumeric(Object operand1, QueryDataType operand1Type, Object operand2,
                                             QueryDataType operand2Type, QueryDataType resultType) {
        Converter operand1Converter = operand1Type.getConverter();
        Converter operand2Converter = operand2Type.getConverter();

        try {
            switch (resultType.getTypeFamily()) {
                case TINYINT:
                    return (byte) (operand1Converter.asTinyint(operand1) % operand2Converter.asTinyint(operand2));

                case SMALLINT:
                    return (short) (operand1Converter.asSmallint(operand1) % operand2Converter.asSmallint(operand2));

                case INT:
                    return operand1Converter.asInt(operand1) % operand2Converter.asInt(operand2);

                case BIGINT:
                    return operand1Converter.asBigint(operand1) % operand2Converter.asBigint(operand2);

                case DECIMAL:
                    BigDecimal op1Decimal = operand1Converter.asDecimal(operand1);
                    BigDecimal op2Decimal = operand2Converter.asDecimal(operand2);

                    return op1Decimal.remainder(op2Decimal);

                case REAL: {
                    float res = operand1Converter.asReal(operand1) % operand2Converter.asReal(operand2);

                    if (Float.isInfinite(res)) {
                        throw QueryException.error("Division by zero.");
                    }

                    return res;
                }

                case DOUBLE: {
                    double res = operand1Converter.asDouble(operand1) % operand2Converter.asDouble(operand2);

                    if (Double.isInfinite(res)) {
                        throw QueryException.error("Division by zero.");
                    }

                    return res;
                }

                default:
                    throw QueryException.error("Invalid type: " + resultType);
            }
        } catch (ArithmeticException e) {
            throw QueryException.error("Division by zero.");
        }
    }

}
