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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryException;
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
public abstract class FloorCeilFunction<T> extends UniExpressionWithType<T> {

    private boolean ceil;

    protected FloorCeilFunction() {
        // No-op.
    }

    protected FloorCeilFunction(Expression<?> operand, QueryDataType resultType) {
        super(operand, resultType);
    }

    public static Expression<?> create(Expression<?> operand, boolean ceil) {
        QueryDataType operandType = operand.getType();

        switch (operandType.getTypeFamily()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                // Integer types are always unchanged.
                return operand;

            default:
                break;
        }

        QueryDataType resultType = inferResultType(operandType);

        return ceil ? new CeilFunction<>(operand, resultType) : new FloorFunction<>(operand, resultType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        Object value = operand.eval(row, context);

        if (value == null) {
            return null;
        }

        return (T) floorCeil(value, operand.getType(), resultType, isCeil());
    }

    protected abstract boolean isCeil();

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    private static Object floorCeil(Object operandValue, QueryDataType operandType, QueryDataType resultType, boolean ceil) {
        Converter operandConverter = operandType.getConverter();

        switch (resultType.getTypeFamily()) {
            case DECIMAL: {
                BigDecimal operand0 = operandConverter.asDecimal(operandValue);

                RoundingMode roundingMode = ceil ? RoundingMode.CEILING : RoundingMode.FLOOR;

                return operand0.setScale(0, roundingMode);
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
    public String toString() {
        return getClass().getSimpleName() + "{operand=" + operand + ", resultType=" + resultType + '}';
    }

    private static QueryDataType inferResultType(QueryDataType operandType) {
        if (!ExpressionMath.canConvertToNumber(operandType)) {
            throw QueryException.error("Operand is not numeric: " + operandType);
        }

        switch (operandType.getTypeFamily()) {
            case REAL:
                return QueryDataType.DOUBLE;

            case VARCHAR:
                return QueryDataType.DECIMAL;

            default:
                break;
        }

        return operandType;
    }

}
