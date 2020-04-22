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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.Objects;

/**
 * Column access expression.
 */
public class ColumnExpression<T> implements Expression<T>, IdentifiedDataSerializable {
    /** Index in the row. */
    private int index;

    /** Type of the returned value. */
    private QueryDataType type;

    public ColumnExpression() {
        // No-op.
    }

    private ColumnExpression(int index, QueryDataType type) {
        this.index = index;
        this.type = type;
    }

    public static ColumnExpression<?> create(int index, QueryDataType type) {
        return new ColumnExpression<>(index, type);
    }

    @SuppressWarnings("unchecked")
    @Override public T eval(Row row, ExpressionEvalContext context) {
        return (T) row.get(index);
    }

    @Override
    public QueryDataType getType() {
        return type;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.EXPRESSION_COLUMN;
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
        return Objects.hash(index, type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ColumnExpression<?> that = (ColumnExpression<?>) o;

        return index == that.index && type.equals(that.type);
    }
}
