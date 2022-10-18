/*
 * Copyright 2021 Hazelcast Inc.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.unmodifiableList;

class GenericMapStoreProperties {

    static final String EXTERNAL_REF_ID_PROPERTY = "external-data-store-ref";
    static final String TABLE_NAME_PROPERTY = "table-name";
    static final String MAPPING_TYPE_PROPERTY = "mapping-type";
    static final String ID_COLUMN_PROPERTY = "id-column";
    static final String COLUMNS_PROPERTY = "columns";
    static final String TYPE_NAME_PROPERTY = "type-name";

    final String externalDataStoreRef;
    final String tableName;
    final String mappingType;
    final String idColumn;
    final Collection<String> columns;
    final boolean idColumnInColumns;
    final String compactTypeName;

    GenericMapStoreProperties(Properties properties, String mapName) {
        externalDataStoreRef = properties.getProperty(EXTERNAL_REF_ID_PROPERTY);
        tableName = properties.getProperty(TABLE_NAME_PROPERTY, mapName);
        this.mappingType = properties.getProperty(MAPPING_TYPE_PROPERTY);
        idColumn = properties.getProperty(ID_COLUMN_PROPERTY, "id");

        String columnsProperty = properties.getProperty(COLUMNS_PROPERTY);
        if (columnsProperty != null) {
            List<String> columnsList = new ArrayList<>();
            Collections.addAll(columnsList, columnsProperty.split(","));
            this.columns = unmodifiableList(columnsList);
        } else {
            columns = Collections.emptyList();
        }
        idColumnInColumns = columns.isEmpty() || columns.contains(idColumn);
        compactTypeName = properties.getProperty(TYPE_NAME_PROPERTY, mapName);
    }

    public boolean hasColumns() {
        return !columns.isEmpty();
    }
}
