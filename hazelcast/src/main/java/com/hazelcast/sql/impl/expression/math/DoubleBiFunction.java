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
import com.hazelcast.sql.impl.expression.BiExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;

/**
 * Family of functions which accept two double operands and return double result.
 */
public class DoubleBiFunction extends BiExpression<Double> implements IdentifiedDataSerializable {
    public static final int POWER = 0;
    public static final int ATAN2 = 1;

    private int type;

    public DoubleBiFunction() {
    }

    public DoubleBiFunction(Expression<?> operand1, Expression<?> operand2, int type) {
        super(operand1, operand2);
        this.type = type;
    }

    public static DoubleBiFunction create(Expression<?> operand1, Expression<?> operand2, int type) {
        return new DoubleBiFunction(operand1, operand2, type);
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_DOUBLE_DOUBLE;
    }

    @Override
    public QueryDataType getType() {
        return QueryDataType.DOUBLE;
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
    public Double eval(Row row, ExpressionEvalContext context) {
        Object lhsValue = operand1.eval(row, context);
        if (lhsValue == null) {
            return null;
        }

        Object rhsValue = operand2.eval(row, context);
        if (rhsValue == null) {
            return null;
        }

        assert lhsValue instanceof Number;
        assert rhsValue instanceof Number;

        double lhsDouble = ((Number) lhsValue).doubleValue();
        double rhsDouble = ((Number) rhsValue).doubleValue();

        switch (type) {
            case POWER:
                return Math.pow(lhsDouble, rhsDouble);
            case ATAN2:
                return Math.atan2(lhsDouble, rhsDouble);
            default:
                throw QueryException.error("Unsupported function type: " + type);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DoubleBiFunction)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        DoubleBiFunction that = (DoubleBiFunction) o;

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
        return "DoubleBiFunction{ operand1=" + operand1
                + ", operand2=" + operand2
                + ", type=" + type + '}';
    }
}
