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

public class GenericMapStoreProperties {

    final String dataLinkRef;
    final String tableName;
    final String mappingType;
    public final String idColumn;
    public final Collection<String> columns;
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

    public boolean hasColumns() {
        return !columns.isEmpty();
    }
}
