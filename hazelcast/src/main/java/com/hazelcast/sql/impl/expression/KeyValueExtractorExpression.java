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
import com.hazelcast.sql.impl.row.KeyValueRow;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;

import java.io.IOException;
import java.util.Objects;

/**
 * Specialized expression type which extract a value from the key-value pair.
 *
 * @param <T> Return type.
 */
// TODO: This classshould not be an Expression. It should be inlined into KeyValueRow instead.
public class KeyValueExtractorExpression<T> implements Expression<T> {
    /** Path for extractor. */
    private final String path;

    /** Type of the returned object. */
    private final QueryDataType type;

    private transient Class<?> lastValClass;
    private transient Converter lastValConverter;

    public KeyValueExtractorExpression(String path, QueryDataType type) {
        assert path != null;

        this.path = path;
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(Row row) {
        assert row instanceof KeyValueRow;

        KeyValueRow row0 = (KeyValueRow) row;

        T val = (T) row0.extract(path);

        // TODO: This duplicates ColumnExpression!
        if (val == null) {
            return null;
        }

        // TODO: This piece of code may cause severe slowdown. Need to investigate the reason.
        Class<?> valClass = val.getClass();

        if (lastValClass != valClass) {
            lastValConverter = Converters.getConverter(valClass);
            lastValClass = valClass;
        }

        return (T) type.getConverter().convertToSelf(lastValConverter, val);
    }

    @Override
    public QueryDataType getType() {
        return type;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException("Should not be called.");
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException("Should not be called.");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        KeyValueExtractorExpression<?> that = (KeyValueExtractorExpression<?>) o;

        return path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }

    @Override
    public String toString() {
        return "KeyValueExtractorExpression{path=" + path + '}';
    }
}
