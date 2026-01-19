/*
 * Copyright 2026 Hazelcast Inc.
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

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.internal.tpcengine.util.Preconditions.checkNotNull;
import static com.hazelcast.mapstore.GenericMapLoader.COLUMNS_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.DATA_CONNECTION_REF_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.EXTERNAL_NAME_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.ID_COLUMN_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.LOAD_ALL_KEYS_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.TYPE_NAME_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.SINGLE_COLUMN_AS_VALUE;
import static java.lang.System.lineSeparator;
import static java.util.stream.Collectors.joining;

/**
 * Holds the properties for GenericMapStore and GenericMapLoader
 */
class GenericMapStoreProperties {

    final String dataConnectionRef;
    final String tableName;
    final String idColumn;
    final List<String> columns;

    final Set<String> allColumns = new HashSet<>();
    final boolean idColumnInColumns;
    final String compactTypeName;

    /**
     * Flag that indicates if {@link GenericMapLoader#loadAllKeys()} should run or not
     */
    final boolean loadAllKeys;

    /**
     * Flag that indicates if a single column or a GenericRecord should be returned as the value
     */
    final boolean singleColumnAsValue;

    GenericMapStoreProperties(Properties properties, String mapName) {
        dataConnectionRef = properties.getProperty(DATA_CONNECTION_REF_PROPERTY);
        tableName = properties.getProperty(EXTERNAL_NAME_PROPERTY, mapName);
        idColumn = properties.getProperty(ID_COLUMN_PROPERTY, "id");

        String columnsProperty = properties.getProperty(COLUMNS_PROPERTY);
        if (columnsProperty != null) {
            List<String> columnsList = Arrays.asList(columnsProperty.split(","));
            this.columns = Collections.unmodifiableList(columnsList);
        } else {
            columns = Collections.emptyList();
        }

        allColumns.add(idColumn);
        allColumns.addAll(columns);

        idColumnInColumns = columns.isEmpty() || columns.contains(idColumn);
        compactTypeName = properties.getProperty(TYPE_NAME_PROPERTY, mapName);

        String value = properties.getProperty(LOAD_ALL_KEYS_PROPERTY, "true");
        loadAllKeys = Boolean.parseBoolean(value);

        String singleColumnAsValueString = properties.getProperty(SINGLE_COLUMN_AS_VALUE, "false");
        singleColumnAsValue = Boolean.parseBoolean(singleColumnAsValueString);
    }

    boolean hasColumns() {
        return !columns.isEmpty();
    }

    /**
     * Get all columns including id column
     */
    public Set<String> getAllColumns() {
        return allColumns;
    }

    public void validateColumns(final Map<String, SqlColumnMetadata> columnMap) {
        checkNotNull(columnMap, "columnMap can't be null");
        String errors = allColumns.stream()
                                  .map(columnName -> validateColumn(columnMap, columnName))
                                  .filter(Objects::nonNull)
                                  .collect(joining(", " + lineSeparator()));
        if (!errors.isEmpty()) {
            throw new HazelcastException(errors);
        }
    }

    @Nullable
    private String validateColumn(Map<String, SqlColumnMetadata> columnMap, String columnName) {
        SqlColumnMetadata column = columnMap.get(columnName);
        if (column == null) {
            String text = "Column '" + columnName + "' not found";
            if (columnName.equals(idColumn)) {
                text += (", but is configured as id column or mentioned in the column list property. "
                        + "You need to either add '%s' column to the mapping "
                        + "used by GenericMapStore or change the '%s' or '%s' property of the GenericMapStore")
                        .formatted(idColumn, ID_COLUMN_PROPERTY, COLUMNS_PROPERTY);
            } else {
                text += (", but is mentioned in the column list property. "
                        + "You need to either add '%s' column to the mapping "
                        + "used by GenericMapStore or change the '%s' property of the GenericMapStore")
                        .formatted(idColumn, COLUMNS_PROPERTY);
            }
            return text;
        }
        return null;
    }
}
