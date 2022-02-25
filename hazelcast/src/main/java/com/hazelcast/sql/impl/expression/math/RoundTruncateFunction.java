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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.BiExpressionWithType;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Implementation of ROUND/TRUNCATE functions.
 */
public class RoundTruncateFunction<T> extends BiExpressionWithType<T> implements IdentifiedDataSerializable {

    private boolean truncate;

    public RoundTruncateFunction() {
        // No-op.
    }

    private RoundTruncateFunction(Expression<?> operand1, Expression<?> operand2, QueryDataType resultType, boolean truncate) {
        super(operand1, operand2, resultType);

        this.truncate = truncate;
    }

    public static Expression<?> create(
        Expression<?> operand1,
        Expression<?> operand2,
        QueryDataType resultType,
        boolean truncate
    ) {
        if (operand2 == null) {
            QueryDataType operand1Type = operand1.getType();

            // No conversion is expected for non-fractional types when the length operand is not defined.
            if (MathFunctionUtils.notFractional(operand1Type)) {
                assert operand1Type == resultType;

                return operand1;
            }
        }

        return new RoundTruncateFunction<>(operand1, operand2, resultType, truncate);
    }

    @SuppressWarnings({"unchecked", "checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount"})
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        Object operand1Value = operand1.eval(row, context);

        if (operand1Value == null) {
            return null;
        }

        int len = getLength(row, context);

        switch (resultType.getTypeFamily()) {
            case TINYINT:
                try {
                    return (T) (Byte) execute(operand1Value, len).byteValueExact();
                } catch (ArithmeticException e) {
                    throw overflow(QueryDataTypeFamily.TINYINT, QueryDataTypeFamily.SMALLINT, e);
                }

            case SMALLINT:
                try {
                    return (T) (Short) execute(operand1Value, len).shortValueExact();
                } catch (ArithmeticException e) {
                    throw overflow(QueryDataTypeFamily.SMALLINT, QueryDataTypeFamily.INTEGER, e);
                }

            case INTEGER:
                try {
                    return (T) (Integer) execute(operand1Value, len).intValueExact();
                } catch (ArithmeticException e) {
                    throw overflow(QueryDataTypeFamily.INTEGER, QueryDataTypeFamily.BIGINT, e);
                }

            case BIGINT:
                try {
                    return (T) (Long) execute(operand1Value, len).longValueExact();
                } catch (ArithmeticException e) {
                    throw overflow(QueryDataTypeFamily.BIGINT, QueryDataTypeFamily.DECIMAL, e);
                }

            case DECIMAL:
                return (T) execute(operand1Value, len);

            case REAL:
                float floatValue = (float) operand1Value;

                if (Float.isNaN(floatValue) || Float.isInfinite(floatValue)) {
                    return (T) (Float) floatValue;
                }

                return (T) (Float) execute(operand1Value, len).floatValue();

            case DOUBLE:
                double doubleValue = (double) operand1Value;

                if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
                    return (T) (Double) doubleValue;
                }

                return (T) (Double) execute(operand1Value, len).doubleValue();

            default:
                throw QueryException.error("Unsupported result type for " + getFunctionName() + " function: " + resultType);
        }
    }

    private int getLength(Row row, ExpressionEvalContext context) {
        try {
            Integer operand2Value = operand2 != null ? MathFunctionUtils.asInt(operand2, row, context) : null;

            return operand2Value != null ? operand2Value : 0;
        } catch (Exception e) {
            throw QueryException.dataException("Cannot convert the second operand of " + getFunctionName() + " function to "
                + QueryDataTypeFamily.INTEGER + ": " + e.getMessage(), e);
        }
    }

    private BigDecimal execute(Object operand1Value, int len) {
        BigDecimal value = operand1.getType().getConverter().asDecimal(operand1Value);

        RoundingMode roundingMode = truncate ? RoundingMode.DOWN : RoundingMode.HALF_UP;

        if (len == 0) {
            return value.setScale(0, roundingMode);
        } else {
            return value.movePointRight(len).setScale(0, roundingMode).movePointLeft(len);
        }
    }

    private QueryException overflow(
        QueryDataTypeFamily resultTypeFamily,
        QueryDataTypeFamily proposedTypeFamily,
        ArithmeticException cause
    ) {
        return QueryException.dataException(resultTypeFamily + " overflow in " + getFunctionName()
            + " function (consider adding an explicit CAST to " + proposedTypeFamily + ")", cause);
    }

    private String getFunctionName() {
        return truncate ? "TRUNCATE" : "ROUND";
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_ROUND_TRUNCATE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeBoolean(truncate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        truncate = in.readBoolean();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        if (!super.equals(o)) {
            return false;
        }

        RoundTruncateFunction<?> that = (RoundTruncateFunction<?>) o;

        return truncate == that.truncate;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (truncate ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " {operand1=" + operand1 + ", operand2=" + operand2 + ", truncate=" + truncate + '}';
    }
}
