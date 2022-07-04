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

package com.hazelcast.sql;

import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.spi.annotation.PrivateApi;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SQL row metadata.
 */
public final class SqlRowMetadata {
    /** Constant indicating that the column is not found. */
    public static final int COLUMN_NOT_FOUND = -1;

    private final List<SqlColumnMetadata> columns;
    private final Map<String, Integer> nameToIndex;

    @PrivateApi
    @SuppressWarnings("ConstantConditions")
    public SqlRowMetadata(@Nonnull List<SqlColumnMetadata> columns) {
        assert columns != null && !columns.isEmpty();

        this.columns = Collections.unmodifiableList(columns);

        nameToIndex = new HashMap<>(columns.size());

        for (int i = 0; i < columns.size(); i++) {
            nameToIndex.put(columns.get(i).getName(), i);
        }
    }

    /**
     * Gets the number of columns in the row.
     *
     * @return the number of columns in the row
     */
    public int getColumnCount() {
        return columns.size();
    }

    /**
     * Gets column metadata.
     *
     * @param index column index, zero-based
     * @return column metadata
     * @throws IndexOutOfBoundsException If the column index is out of bounds
     */
    @Nonnull
    public SqlColumnMetadata getColumn(int index) {
        if (index < 0 || index >= columns.size()) {
            throw new IndexOutOfBoundsException("Column index is out of bounds: " + index);
        }

        return columns.get(index);
    }

    /**
     * Gets columns metadata.
     *
     * @return columns metadata
     */
    @Nonnull
    public List<SqlColumnMetadata> getColumns() {
        return columns;
    }

    /**
     * Find index of the column with the given name. Returned index can be used to get column value
     * from {@link SqlRow}.
     *
     * @param columnName column name (case sensitive)
     * @return column index or {@link #COLUMN_NOT_FOUND} if a column with the given name is not found
     * @throws NullPointerException if column name is null
     *
     * @see SqlRow
     */
    public int findColumn(@Nonnull String columnName) {
        Preconditions.checkNotNull(columnName, "Column name cannot be null");

        return nameToIndex.getOrDefault(columnName, COLUMN_NOT_FOUND);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SqlRowMetadata that = (SqlRowMetadata) o;

        return columns.equals(that.columns);
    }

    @Override
    public int hashCode() {
        return columns.hashCode();
    }

    @Override
    public String toString() {
        return columns.stream()
            .map((column) -> column.getName() + ' ' + column.getType())
            .collect(Collectors.joining(", ", "[", "]"));
    }
}
