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

package com.hazelcast.sql;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SQL row metadata.
 */
public class SqlRowMetadata implements Serializable {
    /** Constant indicating that the column is not found. */
    public static final int COLUMN_NOT_FOUND = -1;

    private static final long serialVersionUID = 1595632576318481583L;

    private final List<SqlColumnMetadata> columns;
    private final Map<String, Integer> nameToIndex;

    public SqlRowMetadata(List<SqlColumnMetadata> columns) {
        assert columns != null && !columns.isEmpty();

        this.columns = new ArrayList<>(columns);

        nameToIndex = new HashMap<>(columns.size());

        for (int i = 0; i < columns.size(); i++) {
            nameToIndex.put(columns.get(i).getName(), i);
        }
    }

    /**
     * Gets the number of columns in the row.
     *
     * @return The number of columns in the row.
     */
    public int getColumnCount() {
        return columns.size();
    }

    /**
     * Get column metadata.
     *
     * @param index Column index, 0-based.
     * @return Column metadata.
     */
    @Nonnull
    public SqlColumnMetadata getColumn(int index) {
        if (index < 0) {
            throw new IndexOutOfBoundsException("Column index cannot be negative: " + index);
        }

        if (index >= columns.size()) {
            throw new IndexOutOfBoundsException("Column index is out of bounds: " + index);
        }

        return columns.get(index);
    }

    /**
     * Find index of the column with the given name. Returned index can be used to get column value
     * from {@link SqlRow}.
     *
     * @see SqlRow
     * @param name Column name (case sensitive).
     * @return Column index or {@link #COLUMN_NOT_FOUND} if a column with the given name is not found.
     */
    public int findColumn(String name) {
        Integer index = nameToIndex.get(name);

        return index != null ? index : COLUMN_NOT_FOUND;
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
            .map((column) -> column.getName() + ":" + column.getType())
            .collect(Collectors.joining(", ", "[", "]"));
    }
}
