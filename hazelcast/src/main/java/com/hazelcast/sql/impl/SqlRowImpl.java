/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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


package com.hazelcast.sql.impl;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.row.JetSqlRow;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.StringJoiner;

/**
 * Default implementation of the SQL row which is exposed to users. We merely wrap the internal row, but add more checks which
 * is important for user-facing code, but which could cause performance degradation if implemented in the internal classes.
 */
public class SqlRowImpl implements SqlRow {

    private final SqlRowMetadata rowMetadata;
    private final JetSqlRow row;

    public SqlRowImpl(SqlRowMetadata rowMetadata, JetSqlRow row) {
        this.rowMetadata = rowMetadata;
        this.row = row;
    }

    @Nullable
    @Override
    public <T> T getObject(int columnIndex) {
        checkIndex(columnIndex);

        return getObject0(columnIndex, true);
    }

    @Nullable
    @Override
    public <T> T getObject(@Nonnull String columnName) {
        int columnIndex = resolveIndex(columnName);

        return getObject0(columnIndex, true);
    }

    /**
     * Get column's value without forcing of the deserialization {@link Data}.
     */
    public <T> T getObjectRaw(int columnIndex) {
        checkIndex(columnIndex);

        return getObject0(columnIndex, false);
    }

    @SuppressWarnings("unchecked")
    private <T> T getObject0(int columnIndex, boolean deserialize) {
        if (deserialize) {
            try {
                return (T) row.get(columnIndex);
            } catch (HazelcastSerializationException e) {
                throw new HazelcastSerializationException("Failed to deserialize query result value: " + e.getMessage(), e);
            }
        } else {
            return (T) row.getMaybeSerialized(columnIndex);
        }
    }

    private int resolveIndex(String columnName) {
        int index = rowMetadata.findColumn(columnName);

        if (index == SqlRowMetadata.COLUMN_NOT_FOUND) {
            throw new IllegalArgumentException("Column \"" + columnName + "\" doesn't exist");
        }

        return index;
    }

    @Nonnull
    @Override
    public SqlRowMetadata getMetadata() {
        return rowMetadata;
    }

    private void checkIndex(int index) {
        if (index < 0 || index >= rowMetadata.getColumnCount()) {
            throw new IndexOutOfBoundsException("Column index is out of range: " + index);
        }
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");

        for (int i = 0; i < rowMetadata.getColumnCount(); i++) {
            SqlColumnMetadata columnMetadata = rowMetadata.getColumn(i);
            // toString() is often called by the debugger, it must not mutate the state by serializing or deserializing.
            Object columnValue = row.getMaybeSerialized(i);

            joiner.add(columnMetadata.getName() + ' ' + columnMetadata.getType() + '=' + columnValue);
        }

        return joiner.toString();
    }
}
