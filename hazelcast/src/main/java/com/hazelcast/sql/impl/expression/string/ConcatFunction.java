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

package com.hazelcast.sql.impl.expression.string;

import com.hazelcast.sql.impl.expression.BiCallExpression;
import com.hazelcast.sql.impl.expression.CallOperator;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;

/**
 * A function which accepts a string, and return another string.
 */
public class ConcatFunction extends BiCallExpression<String> {
    /** Type of operand 1. */
    private transient DataType operand1Type;

    /** Type of operand 2. */
    private transient DataType operand2Type;

    public ConcatFunction() {
        // No-op.
    }

    public ConcatFunction(Expression operand1, Expression operand2) {
        super(operand1, operand2);
    }

    @Override
    public String eval(Row row) {
        Object operand1Value = operand1.eval(row);
        Object operand2Value = operand2.eval(row);

        if (operand1Value != null && operand1Type == null) {
            operand1Type = operand1.getType();
        }

        if (operand2Value != null && operand2Type == null) {
            operand2Type = operand2.getType();
        }

        if (operand1Value == null) {
            if (operand2Value == null) {
                return "";
            } else {
                return operand2Type.getConverter().asVarchar(operand2Value);
            }
        } else {
            if (operand2Value == null) {
                return operand1Type.getConverter().asVarchar(operand1Value);
            } else {
                return operand1Type.getConverter().asVarchar(operand1Value)
                    + operand2Type.getConverter().asVarchar(operand2Value);
            }
        }
    }

    @Override
    public DataType getType() {
        return DataType.VARCHAR;
    }

    @Override
    public int operator() {
        return CallOperator.CONCAT;
    }
}
