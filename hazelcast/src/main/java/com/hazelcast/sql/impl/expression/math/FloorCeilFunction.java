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
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.expression.CastExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.UniCallExpressionWithType;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.DataTypeUtils;
import com.hazelcast.sql.impl.type.GenericType;
import com.hazelcast.sql.impl.type.accessor.Converter;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Implementation of FLOOR/CEIL functions.
 */
public abstract class FloorCeilFunction<T> extends UniCallExpressionWithType<T> {
    /** If this is the CEIL call. */
    private boolean ceil;

    protected FloorCeilFunction() {
        // No-op.
    }

    protected FloorCeilFunction(Expression<?> operand, DataType resultType) {
        super(operand, resultType);
    }

    public static Expression<?> create(Expression<?> operand, boolean ceil) {
        DataType operandType = operand.getType();

        switch (operandType.getType()) {
            case BIT:
                // Bit alway remain the same, just coerce it.
                return CastExpression.coerce(operand, DataType.TINYINT);

            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                // Integer types are always unchanged.
                return operand;

            default:
                break;
        }

        DataType resultType = inferResultType(operandType);

        return ceil ? new CeilFunction<>(operand, resultType) : new FloorFunction<>(operand, resultType);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row) {
        Object value = operand.eval(row);

        if (value == null) {
            return null;
        }

        return (T) floorCeil(value, operand.getType(), resultType, isCeil());
    }

    protected abstract boolean isCeil();

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    private static Object floorCeil(Object operandValue, DataType operandType, DataType resultType, boolean ceil) {
        if (resultType.getType() == GenericType.LATE) {
            // Special handling for late binding.
            operandType = DataTypeUtils.resolveType(operandValue);

            switch (operandType.getType()) {
                case BIT:
                    // Bit alway remain the same, just coerce it.
                    return CastExpression.coerce(operandValue, operandType, DataType.TINYINT);

                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    // Integer types are always unchanged.
                    return operandValue;

                default:
                    break;
            }

            resultType = inferResultType(operandType);
        }

        Converter operandConverter = operandType.getConverter();

        switch (resultType.getType()) {
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
                throw HazelcastSqlException.error("Unexpected type: " + resultType);
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
            case REAL:
                return DataType.DOUBLE;

            case VARCHAR:
                return DataType.DECIMAL;

            default:
                break;
        }

        return operandType;
    }
}
