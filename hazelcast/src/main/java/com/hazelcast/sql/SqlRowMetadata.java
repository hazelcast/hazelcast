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

import java.util.List;

/**
 * SQL row metadata.
 */
public class SqlRowMetadata {

    private final List<SqlColumnMetadata> columns;

    public SqlRowMetadata(List<SqlColumnMetadata> columns) {
        assert columns != null && !columns.isEmpty();

        this.columns = columns;
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
    public SqlColumnMetadata getColumn(int index) {
        if (index < 0) {
            throw new IllegalArgumentException("Column index cannot be negative: " + index);
        }

        if (index >= columns.size()) {
            throw new IndexOutOfBoundsException("Column index is out of bounds: " + index);
        }

        return columns.get(index);
    }
}
