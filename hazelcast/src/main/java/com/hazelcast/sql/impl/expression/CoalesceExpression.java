/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;

public class CoalesceExpression implements Expression, IdentifiedDataSerializable {
    private Expression<?>[] operands;

    public static CoalesceExpression create(Expression<?>[] operands) {
        assert operands.length > 1;
        return new CoalesceExpression(operands);
    }

    public CoalesceExpression() {
    }

    private CoalesceExpression(Expression<?>[] operands) {
        this.operands = operands;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }

    @Override
    public int getFactoryId() {
        return 0;
    }

    @Override
    public int getClassId() {
        return 0;
    }

    @Override
    public Object eval(Row row, ExpressionEvalContext context) {
        for (Expression<?> expr : operands) {
            Object value = expr.eval(row, context);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    @Override
    public QueryDataType getType() {
        return operands[0].getType();
    }
}
