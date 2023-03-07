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
import static com.hazelcast.mapstore.GenericMapLoader.DATA_LINK_REF_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.ID_COLUMN_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.MAPPING_TYPE_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.TABLE_NAME_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.TYPE_NAME_PROPERTY;

/**
 * Holds the properties for GenericMapStore and GenericMapLoader
 */
class GenericMapStoreProperties {

    final String dataLinkRef;
    final String tableName;
    final String mappingType;
    final String idColumn;
    final List<String> columns;

    final Set<String> allColumns = new HashSet<>();
    final boolean idColumnInColumns;
    final String compactTypeName;

    GenericMapStoreProperties(Properties properties, String mapName) {
        dataLinkRef = properties.getProperty(DATA_LINK_REF_PROPERTY);
        tableName = properties.getProperty(TABLE_NAME_PROPERTY, mapName);
        mappingType = properties.getProperty(MAPPING_TYPE_PROPERTY);
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
