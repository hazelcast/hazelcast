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
import com.hazelcast.sql.impl.expression.UniExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;

/**
 * Family of functions which accept a single double operand and return double result:
 *     COS, SIN, TAN, COT, ACOS, ASIN, ATAN, SQRT, EXP, LN, LOG10, DEGREES, RADIANS
 */
public class DoubleFunction extends UniExpression<Double> {
    /** Function type. */
    private DoubleFunctionType type;

    public DoubleFunction() {
        // No-op.
    }

    public DoubleFunction(Expression<?> operand, DoubleFunctionType type) {
        super(operand);

        this.type = type;
    }

    public static DoubleFunction create(Expression<?> operand, DoubleFunctionType type) {
        QueryDataType operandType = operand.getType();

        if (!ExpressionMath.canConvertToNumber(operandType)) {
            throw QueryException.error("Operand is not numeric: " + operandType);
        }

        return new DoubleFunction(operand, type);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount"})
    @Override
    public Double eval(Row row, ExpressionEvalContext context) {
        Object value = operand.eval(row, context);

        if (value == null) {
            return null;
        }

        double valueDouble = operand.getType().getConverter().asDouble(value);

        switch (type) {
            case COS:
                return Math.cos(valueDouble);

            case SIN:
                return Math.sin(valueDouble);

            case TAN:
                return Math.tan(valueDouble);

            case COT:
                return 1.0d / Math.tan(valueDouble);

            case ACOS:
                return Math.acos(valueDouble);

            case ASIN:
                return Math.asin(valueDouble);

            case ATAN:
                return Math.atan(valueDouble);

            case SQRT:
                return Math.sqrt(valueDouble);

            case EXP:
                return Math.exp(valueDouble);

            case LN:
                return Math.log(valueDouble);

            case LOG10:
                return Math.log10(valueDouble);

            case DEGREES:
                return Math.toDegrees(valueDouble);

            case RADIANS:
                return Math.toRadians(valueDouble);

            default:
                throw QueryException.error("Unsupported type: " + type);
        }
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.DOUBLE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeInt(type.getId());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        type = DoubleFunctionType.getById(in.readInt());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{type=" + type + ", operand=" + operand + '}';
    }
}
