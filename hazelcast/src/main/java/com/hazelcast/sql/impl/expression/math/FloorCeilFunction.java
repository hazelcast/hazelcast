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
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.UniExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Implementation of FLOOR/CEIL functions.
 */
public class FloorCeilFunction<T> extends UniExpressionWithType<T> implements IdentifiedDataSerializable {

    private boolean ceil;

    public FloorCeilFunction() {
        // No-op.
    }

    private FloorCeilFunction(Expression<?> operand, QueryDataType resultType, boolean ceil) {
        super(operand, resultType);

        this.ceil = ceil;
    }

    public static Expression<?> create(Expression<?> operand, QueryDataType resultType, boolean ceil) {
        QueryDataType operandType = operand.getType();

        // Non-fractional operand will not be changed.
        if (MathFunctionUtils.notFractional(operandType) && operandType == resultType) {
            return operand;
        }

        return new FloorCeilFunction<>(operand, resultType, ceil);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        Object value = operand.eval(row, context);

        if (value == null) {
            return null;
        }

        return (T) floorCeil(value, operand.getType(), resultType, ceil);
    }

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    private static Object floorCeil(Object operandValue, QueryDataType operandType, QueryDataType resultType, boolean ceil) {
        Converter operandConverter = operandType.getConverter();

        switch (resultType.getTypeFamily()) {
            case DECIMAL: {
                BigDecimal operand0 = operandConverter.asDecimal(operandValue);

                RoundingMode roundingMode = ceil ? RoundingMode.CEILING : RoundingMode.FLOOR;

                return operand0.setScale(0, roundingMode);
            }

            case REAL: {
                float operand0 = operandConverter.asReal(operandValue);

                return (float) (ceil ? Math.ceil(operand0) : Math.floor(operand0));
            }

            case DOUBLE: {
                double operand0 = operandConverter.asDouble(operandValue);

                return ceil ? Math.ceil(operand0) : Math.floor(operand0);
            }

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
        return SqlDataSerializerHook.EXPRESSION_FLOOR_CEIL;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeBoolean(ceil);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        ceil = in.readBoolean();
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

        FloorCeilFunction<?> that = (FloorCeilFunction<?>) o;

        return ceil == that.ceil;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();

        result = 31 * result + (ceil ? 1 : 0);

        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{operand=" + operand + ", ceil=" + ceil + '}';
    }
}
