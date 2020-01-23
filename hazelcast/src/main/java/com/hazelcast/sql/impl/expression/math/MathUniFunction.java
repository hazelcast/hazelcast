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
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.UniCallExpression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;

/**
 * Family of functions which accept a single double operand and return double result: COS, SIN, TAN, COT, ACOS, ASIN,
 * ATAN, SQRT, EXP, LN, LOG10, DEGREES, RADIANS.
 */
public class MathUniFunction extends UniCallExpression<Double> {
    /** Operator. */
    private int operator;

    /** Operand type. */
    private transient DataType operandType;

    public MathUniFunction() {
        // No-op.
    }

    public MathUniFunction(Expression operand, int operator) {
        super(operand);

        this.operator = operator;
    }

    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:ReturnCount"})
    @Override
    public Double eval(QueryContext ctx, Row row) {
        Object operandValue = operand.eval(ctx, row);

        if (operandValue == null) {
            return null;
        } else if (operandType == null) {
            DataType type = operand.getType();

            if (!type.isCanConvertToNumeric()) {
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand is not numeric: " + type);
            }

            operandType = type;
        }

        double operandValueDouble = operandType.getConverter().asDouble(operandValue);

        switch (operator) {
            case CallOperator.COS:
                return Math.cos(operandValueDouble);

            case CallOperator.SIN:
                return Math.sin(operandValueDouble);

            case CallOperator.TAN:
                return Math.tan(operandValueDouble);

            case CallOperator.COT:
                return 1.0d / Math.tan(operandValueDouble);

            case CallOperator.ACOS:
                return Math.acos(operandValueDouble);

            case CallOperator.ASIN:
                return Math.asin(operandValueDouble);

            case CallOperator.ATAN:
                return Math.atan(operandValueDouble);

            case CallOperator.SQRT:
                return Math.sqrt(operandValueDouble);

            case CallOperator.EXP:
                return Math.exp(operandValueDouble);

            case CallOperator.LN:
                return Math.log(operandValueDouble);

            case CallOperator.LOG10:
                return Math.log10(operandValueDouble);

            case CallOperator.DEGREES:
                return Math.toDegrees(operandValueDouble);

            case CallOperator.RADIANS:
                return Math.toRadians(operandValueDouble);

            default:
                throw new HazelcastSqlException(-1, "Unsupported operator: " + operator);
        }
    }

    @Override
    public DataType getType() {
        return DataType.DOUBLE;
    }

    @Override
    public int operator() {
        return operator;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeInt(operator);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        operator = in.readInt();
    }
}
