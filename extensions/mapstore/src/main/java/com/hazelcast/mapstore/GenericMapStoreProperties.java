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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static com.hazelcast.mapstore.GenericMapLoader.COLUMNS_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.DATA_LINK_REF_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.ID_COLUMN_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.MAPPING_TYPE_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.TABLE_NAME_PROPERTY;
import static com.hazelcast.mapstore.GenericMapLoader.TYPE_NAME_PROPERTY;

/**
 * Holds the properties for GenericMapStore and GenericMapLoader
 */
public class GenericMapStoreProperties {

    final String dataLinkRef;
    final String tableName;
    final String mappingType;
    final String idColumn;
    final Collection<String> columns;
    final boolean idColumnInColumns;
    final String compactTypeName;

    GenericMapStoreProperties(Properties properties, String mapName) {
        dataLinkRef = properties.getProperty(DATA_LINK_REF_PROPERTY);
        tableName = properties.getProperty(TABLE_NAME_PROPERTY, mapName);
        this.mappingType = properties.getProperty(MAPPING_TYPE_PROPERTY);
        idColumn = properties.getProperty(ID_COLUMN_PROPERTY, "id");

        String columnsProperty = properties.getProperty(COLUMNS_PROPERTY);
        if (columnsProperty != null) {
            List<String> columnsList = Arrays.asList(columnsProperty.split(","));
            this.columns = Collections.unmodifiableList(columnsList);
        } else {
            columns = Collections.emptyList();
        }
        idColumnInColumns = columns.isEmpty() || columns.contains(idColumn);
        compactTypeName = properties.getProperty(TYPE_NAME_PROPERTY, mapName);
    }

    /**
     * Return the column name for primary key
     */
    public String getIdColumn() {
        return idColumn;
    }

    /**
     * Returns the column names
     */
    public Collection<String> getColumns() {
        return columns;
    }

    boolean hasColumns() {
        return !columns.isEmpty();
    }
}
