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
import com.hazelcast.sql.impl.QueryFragmentContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.type.accessor.Converter;
import com.hazelcast.sql.impl.type.accessor.Converters;

import java.io.IOException;
import java.util.Objects;

/**
 * Dynamic parameter expression.
 */
public class ParameterExpression<T> implements Expression<T> {
    /** Index. */
    private int index;

    /** Cached value. */
    private transient volatile T cachedValue;

    public ParameterExpression() {
        // No-op.
    }

    public ParameterExpression(int index) {
        this.index = index;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row) {
        if (cachedValue != null) {
            return cachedValue;
        }

        T value = (T) QueryFragmentContext.getCurrentContext().getArgument(index);

        if (value == null) {
            return null;
        } else {
            // Normalize and save to cache.
            Converter converter = Converters.getConverter(value.getClass());

            value = (T) converter.convertToSelf(converter, value);

            cachedValue = value;

            return value;
        }
    }

    @Override
    public DataType getType() {
        return DataType.LATE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(index);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        index = in.readInt();
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
