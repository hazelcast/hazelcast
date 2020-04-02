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

package com.hazelcast.sql.impl.expression;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.Objects;

/**
 * Dynamic parameter expression.
 */
public class ParameterExpression<T> implements Expression<T> {

    private int index;
    // TODO: remove type?
    private QueryDataType type;

    @SuppressWarnings("unused")
    public ParameterExpression() {
        // No-op.
    }

    public ParameterExpression(int index, QueryDataType type) {
        this.index = index;
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        return (T) context.getArguments().get(index);
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
        return Objects.hash(index);
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

        return index == that.index;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{index=" + index + '}';
    }

}
