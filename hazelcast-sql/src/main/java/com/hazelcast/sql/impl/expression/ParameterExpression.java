/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.expression;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;

/**
 * Dynamic parameter expression.
 */
public final class ParameterExpression<T> implements Expression<T> {

    private int index;
    private QueryDataType type;

    public ParameterExpression() {
        // No-op.
    }

    private ParameterExpression(int index, QueryDataType type) {
        this.index = index;
        this.type = type;
    }

    public static ParameterExpression<?> create(int index, QueryDataType type) {
        return new ParameterExpression<>(index, type);
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_PARAMETER;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        return (T) context.getArgument(index);
    }

    @Override
    public QueryDataType getType() {
        return type;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(index);
        out.writeObject(type);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        index = in.readInt();
        type = in.readObject();
    }

    @Override
    public int hashCode() {
        int result = index;
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ParameterExpression<?> that = (ParameterExpression<?>) o;

        return index == that.index && type.equals(that.type);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{index=" + index + '}';
    }

    @Override
    public boolean isCooperative() {
        return true;
    }

    public int getIndex() {
        return index;
    }
}
