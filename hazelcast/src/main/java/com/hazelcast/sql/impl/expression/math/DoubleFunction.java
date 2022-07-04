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
import com.hazelcast.sql.impl.expression.UniExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;

/**
 * Family of functions which accept a single double operand and return double result.
 */
public class DoubleFunction extends UniExpression<Double> implements IdentifiedDataSerializable {

    public static final int COS = 0;
    public static final int SIN = 1;
    public static final int TAN = 2;
    public static final int COT = 3;
    public static final int ACOS = 4;
    public static final int ASIN = 5;
    public static final int ATAN = 6;
    public static final int EXP = 7;
    public static final int LN = 8;
    public static final int LOG10 = 9;
    public static final int DEGREES = 10;
    public static final int RADIANS = 11;
    public static final int SQUARE = 12;
    public static final int SQRT = 13;
    public static final int CBRT = 14;

    private int type;

    public DoubleFunction() {
        // No-op.
    }

    public DoubleFunction(Expression<?> operand, int type) {
        super(operand);

        this.type = type;
    }

    public static DoubleFunction create(Expression<?> operand, int type) {
        return new DoubleFunction(operand, type);
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount"})
    @Override
    public Double eval(Row row, ExpressionEvalContext context) {
        Object value = operand.eval(row, context);

        if (value == null) {
            return null;
        }

        assert value instanceof Number;

        double valueDouble = ((Number) value).doubleValue();

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

            case SQUARE:
                return valueDouble * valueDouble;

            case SQRT:
                return Math.sqrt(valueDouble);

            case CBRT:
                return Math.cbrt(valueDouble);

            default:
                throw QueryException.error("Unsupported function type: " + type);
        }
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.DOUBLE;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_DOUBLE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeInt(type);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        type = in.readInt();
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

        DoubleFunction that = (DoubleFunction) o;

        return type == that.type;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();

        result = 31 * result + type;

        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{type=" + type + ", operand=" + operand + '}';
    }
}
