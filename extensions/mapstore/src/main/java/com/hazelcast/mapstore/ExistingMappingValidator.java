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

package com.hazelcast.mapstore;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.sql.SqlColumnMetadata;

import java.util.Map;
import java.util.Set;

/**
 * Validates if database and GenericMapStoreProperties columns match
 */
final class ExistingMappingValidator {

    private ExistingMappingValidator() {
    }

    /**
     * Validate if database rows contain all column names in GenericMapStoreProperties
     */
    public static void validateColumnsExist(Map<String, SqlColumnMetadata> columnMap, Set<String> allColumns) {
        // All columns must exist on the database
        allColumns.forEach(columnName -> validateColumn(columnMap, columnName));
    }

    /**
     * Validate if columnName exists in the database row
     */
    public static SqlColumnMetadata validateColumn(Map<String, SqlColumnMetadata> columnMap, String columnName) {
        SqlColumnMetadata column = columnMap.get(columnName);
        if (column == null) {
            throw new HazelcastException("Column '" + columnName + "' not found");
        }
        return column;
    }
}
