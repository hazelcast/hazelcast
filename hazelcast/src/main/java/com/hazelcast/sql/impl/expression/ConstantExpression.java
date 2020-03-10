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
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;

import java.io.IOException;
import java.util.Objects;

/**
 * Constant expression.
 *
 * @param <T> Return type.
 */
public class ConstantExpression<T> implements Expression<T> {
    /** Value. */
    private T val;

    public ConstantExpression() {
        // No-op.
    }

    private ConstantExpression(T val) {
        this.val = val;
    }

    public static ConstantExpression<?> create(Object val) {
        // Normalize the constant to well-known type.
        if (val != null) {
            Converter converter = Converters.getConverter(val.getClass());

            val = converter.convertToSelf(converter, val);
        }

        return new ConstantExpression<>(val);
    }

    @Override
    public T eval(Row row) {
        return val;
    }

    @Override
    public QueryDataType getType() {
        // TODO: We may have a problem with NULL here, because it is resolved to LATE.
        //  Probably we should introduce another type for NULL, which could be converted to any other type?
        return QueryDataTypeUtils.resolveType(val);
    }

    public T getValue() {
        return val;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(val);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        val = in.readObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(val);
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

        return Objects.equals(val, that.val);
    }

    @Override
    public String toString() {
        return "ConstantExpression{val=" + val + '}';
    }
}
