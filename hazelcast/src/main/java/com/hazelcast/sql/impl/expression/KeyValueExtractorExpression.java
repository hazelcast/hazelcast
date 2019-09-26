/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.type.DataType;
import com.hazelcast.sql.impl.row.KeyValueRow;
import com.hazelcast.sql.impl.row.Row;

import java.io.IOException;

/**
 * Specialized expression type which extract a value from the key-value pair.
 *
 * @param <T> Return type.
 */
public class KeyValueExtractorExpression<T> implements Expression<T> {
    /** Path for extractor. */
    private String path;

    /** Type of the returned object. */
    private transient DataType type;

    public KeyValueExtractorExpression() {
        // No-op.
    }

    public KeyValueExtractorExpression(String path) {
        this.path = path;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T eval(QueryContext ctx, Row row) {
        assert row instanceof KeyValueRow;

        KeyValueRow row0 = (KeyValueRow) row;

        T res = (T) row0.extract(path);

        if (res != null && type == null) {
            type = DataType.resolveType(res);
        }

        return res;
    }

    @Override
    public DataType getType() {
        return DataType.notNullOrLate(type);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(path);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        path = in.readUTF();
    }
}
