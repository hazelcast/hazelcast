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

import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlService;

import java.util.List;

import static com.hazelcast.mapstore.MappingHelper.loadRowMetadataFromMapping;
import static com.hazelcast.mapstore.validators.ExistingMappingValidator.validateColumnsExist;

class ExistingMappingReader {

    private List<SqlColumnMetadata> columnMetadataList;

    public List<SqlColumnMetadata> getColumnMetadataList() {
        return columnMetadataList;
    }

    public void readExistingMapping(SqlService sqlService, GenericMapStoreProperties genericMapStoreProperties,
                                    String mappingName) {
        try (SqlResult mappings = sqlService.execute("SHOW MAPPINGS")) {
            for (SqlRow sqlRow : mappings) {
                String name = sqlRow.getObject("name");
                if (name.equals(mappingName)) {
                    SqlRowMetadata rowMetadata = loadRowMetadataFromMapping(sqlService, name);
                    validateColumnsExist(rowMetadata, genericMapStoreProperties);
                    columnMetadataList = rowMetadata.getColumns();
                    break;
                }
            }
        }
    }
}
