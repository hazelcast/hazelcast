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
import com.hazelcast.sql.impl.LazyTarget;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;

import java.io.IOException;

/**
 * Column access expression.
 */
public final class ColumnExpression<T> implements Expression<T> {
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
        // Canonicalize the column type: currently values of non-canonical types,
        // like QueryDataType.VARCHAR_CHARACTER, are canonicalized to values of
        // some other canonical type, like QueryDataType.VARCHAR. That kind of
        // changes the observed type of a column to a canonical one.
        if (type.getTypeFamily() == QueryDataTypeFamily.OBJECT) {
            return new ColumnExpression<>(index, type);
        } else {
            Class<?> canonicalClass = type.getConverter().getNormalizedValueClass();
            QueryDataType canonicalType = QueryDataTypeUtils.resolveTypeForClass(canonicalClass);

            return new ColumnExpression<>(index, canonicalType);
        }
    }

    @Override
    public Object evalTop(Row row, ExpressionEvalContext context) {
        // Don't use lazy deserialization for compact and portable, we need to return a deserialized generic record
        // if the column expression is the top expression.
        Object res = row.get(index, false);
        if (res instanceof LazyTarget) {
            assert type.getTypeFamily() == QueryDataTypeFamily.OBJECT;
            LazyTarget lazyTarget = (LazyTarget) res;
            res = lazyTarget.getDeserialized() != null ? lazyTarget.getDeserialized() : lazyTarget.getSerialized();
        }
        return res;
    }

    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        // Lazy deserialization is disabled by default, and it has to be requested explicitly.
        return eval(row, context, false);
    }

    @Override
    public T eval(Row row, ExpressionEvalContext context, boolean useLazyDeserialization) {
        Object res = row.get(index, useLazyDeserialization);

        if (res instanceof LazyTarget) {
            assert type.getTypeFamily() == QueryDataTypeFamily.OBJECT;
            res = ((LazyTarget) res).deserialize(context.getSerializationService());
        }

        return (T) res;
    }

    @Override
    public QueryDataType getType() {
        return type;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_COLUMN;
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

        ColumnExpression<?> that = (ColumnExpression<?>) o;

        return index == that.index && type.equals(that.type);
    }

    @Override
    public String toString() {
        return "$" + index;
    }

    @Override
    public boolean isCooperative() {
        return true;
    }
}
