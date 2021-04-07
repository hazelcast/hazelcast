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

public class NullIfExpression<T> implements Expression<T>, IdentifiedDataSerializable {
    private Expression<T> left;
    private Expression<T> right;

    public NullIfExpression() {
    }

    public static NullIfExpression<?> create(Expression<?> left, Expression<?> right) {
        return new NullIfExpression(left, right);
    }

    private NullIfExpression(Expression<T> left, Expression<T> right) {
        this.left = left;
        this.right = right;
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
    public T eval(Row row, ExpressionEvalContext context) {
        T leftResult = left.eval(row, context);
        T rightResult = right.eval(row, context);
        if (leftResult.equals(rightResult)) {
            return null;
        }
        return leftResult;
    }

    @Override
    public QueryDataType getType() {
        return left.getType();
    }
}
