package com.hazelcast.mapstore;

import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlService;

import java.util.List;

import static com.hazelcast.mapstore.MappingHelper.loadMetadataFromMapping;
import static com.hazelcast.mapstore.validators.ExistingMappingValidator.validateColumnsExist;

class ExistingMappingReader {

    static final String MAPPING_NAME_COLUMN = "name";

    protected List<SqlColumnMetadata> columnMetadataList;

    public List<SqlColumnMetadata> getColumnMetadataList() {
        return columnMetadataList;
    }

    public void readExistingMapping(SqlService sqlService, GenericMapStoreProperties genericMapStoreProperties,
                                    String mappingName) {
        try (SqlResult mappings = sqlService.execute("SHOW MAPPINGS")) {
            for (SqlRow sqlRow : mappings) {
                String name = sqlRow.getObject(MAPPING_NAME_COLUMN);
                if (name.equals(mappingName)) {
                    SqlRowMetadata rowMetadata = loadMetadataFromMapping(sqlService, name);
                    validateColumnsExist(rowMetadata, genericMapStoreProperties);
                    columnMetadataList = rowMetadata.getColumns();
                    break;
                }
            }
        }
    }
}
