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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.mapstore.GenericMapLoader.COLUMNS_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.DATA_CONNECTION_REF_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.EXTERNAL_NAME_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.ID_COLUMN_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.LOAD_ALL_KEYS_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.TYPE_NAME_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.SINGLE_COLUMN_AS_VALUE;

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

}
