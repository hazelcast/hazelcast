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
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;

import java.io.IOException;
import java.util.Objects;

/**
 * Constant expression.
 *
 * @param <T> Return type.
 */
public final class ConstantExpression<T> implements Expression<T> {

    @SuppressWarnings("unchecked")
    public static final ConstantExpression<Boolean> TRUE = (ConstantExpression<Boolean>) create(true, QueryDataType.BOOLEAN);
    @SuppressWarnings("unchecked")
    public static final ConstantExpression<Boolean> FALSE = (ConstantExpression<Boolean>) create(false, QueryDataType.BOOLEAN);
    @SuppressWarnings("unchecked")
    public static final ConstantExpression<Boolean> NULL = (ConstantExpression<Boolean>) create(null, QueryDataType.BOOLEAN);

    private QueryDataType type;
    private T value;

    public ConstantExpression() {
        // No-op.
    }

    private ConstantExpression(T value, QueryDataType type) {
        this.type = type;
        this.value = value;
    }

    public static ConstantExpression<?> create(Object value, QueryDataType type) {
        if (value == null) {
            return new ConstantExpression<>(null, type);
        }
        assert type.getTypeFamily() != QueryDataTypeFamily.NULL;

        Converter valueConverter = Converters.getConverter(value.getClass());
        Converter typeConverter = type.getConverter();
        value = typeConverter.convertToSelf(valueConverter, value);

        return new ConstantExpression<>(value, type);
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.EXPRESSION_CONSTANT;
    }

    @Override
    public T eval(Row row, ExpressionEvalContext context) {
        return value;
    }

    @Override
    public QueryDataType getType() {
        return type;
    }

    public T getValue() {
        return value;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(value);
        out.writeObject(type);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        value = in.readObject();
        type = in.readObject();
    }

    @Override
    public int hashCode() {
        int result = type.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
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

        ConstantExpression<?> that = (ConstantExpression<?>) o;

        return Objects.equals(type, that.type) && Objects.equals(value, that.value);
    }

    @Override
    public String toString() {
        return "ConstantExpression{type=" + type + ", value=" + value + '}';
    }

    @Override
    public boolean isCooperative() {
        return true;
    }
}
