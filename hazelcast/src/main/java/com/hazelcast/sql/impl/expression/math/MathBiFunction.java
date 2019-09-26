/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.expression.BiCallExpression;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

import java.io.IOException;

/**
 * Family of functions accepting two double arguments and returning double result (POWER, ATAN2).
 */
public class MathBiFunction extends BiCallExpression<Double> {
    /** Operator. */
    private int operator;

    /** Type of the first argument. */
    private transient DataType operandType1;

    /** Type of the second argument. */
    private transient DataType operandType2;

    public MathBiFunction() {
        // No-op.
    }

    public MathBiFunction(Expression operand1, Expression operand2, int operator) {
        super(operand1, operand2);

        this.operator = operator;
    }

    @Override
    public Double eval(QueryContext ctx, Row row) {
        Object operand1Value = operand1.eval(ctx, row);

        if (operand1Value == null) {
            return null;
        } else if (operandType1 == null) {
            DataType type = operand1.getType();

            if (!type.isCanConvertToNumeric()) {
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand 1 is not numeric: " + type);
            }

            operandType1 = type;
        }

        Object operand2Value = operand2.eval(ctx, row);

        if (operand2Value == null) {
            return null;
        } else if (operandType2 == null) {
            DataType type = operand2.getType();

            if (!type.isCanConvertToNumeric()) {
                throw new HazelcastSqlException(SqlErrorCode.GENERIC, "Operand 2 is not numeric: " + type);
            }

            operandType2 = type;
        }

        double operand1ValueDouble = operandType1.getConverter().asDouble(operand1Value);
        double operand2ValueDouble = operandType2.getConverter().asDouble(operand1Value);

        switch (operator) {
            case CallOperator.ATAN2:
                return Math.atan2(operand1ValueDouble, operand2ValueDouble);

            case CallOperator.POWER:
                return Math.pow(operand1ValueDouble, operand2ValueDouble);

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
