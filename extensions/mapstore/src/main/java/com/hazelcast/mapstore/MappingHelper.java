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
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.SqlService;

import java.util.List;

import static com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnector.OPTION_DATA_LINK_NAME;

final class MappingHelper {

    private final SqlService sqlService;

    MappingHelper(SqlService sqlService) {
        this.sqlService = sqlService;
    }

    public void createMapping(String mappingName, String tableName, String mappingType, String dataLinkRef,
                              String idColumn) {
        sqlService.execute(
                "CREATE MAPPING \"" + mappingName + "\""
                + " EXTERNAL NAME \"" + tableName + "\" "
                + " TYPE " + mappingType
                + " OPTIONS ("
                + "    '" + OPTION_DATA_LINK_NAME + "' = '" + dataLinkRef + "', "
                + "    'idColumn' = '" + idColumn + "' "
                + ")"
        ).close();
    }

    public void createMappingWithColumns(String mappingName, String tableName, String mappingColumns,
                                         String mappingType, String dataLinkRef, String idColumn) {
        sqlService.execute(
                "CREATE MAPPING \"" + mappingName + "\" "
                + "EXTERNAL NAME \"" + tableName + "\" "
                + (mappingColumns != null ? " ( " + mappingColumns + " ) " : "")
                + "TYPE " + mappingType + " "
                + "OPTIONS ("
                + "    '" + OPTION_DATA_LINK_NAME + "' = '" + dataLinkRef + "', "
                + "    'idColumn' = '" + idColumn + "' "
                + ")"
        ).close();
    }

    public void dropMapping(String mappingName) {
        sqlService.execute("DROP MAPPING IF EXISTS \"" + mappingName + "\"").close();
    }

    public List<SqlColumnMetadata> loadColumnMetadataFromMapping(String mapping) {
        return loadRowMetadataFromMapping(mapping).getColumns();
    }

    public SqlRowMetadata loadRowMetadataFromMapping(String mapping) {
        try (SqlResult result = sqlService.execute("SELECT * FROM \"" + mapping + "\" LIMIT 0")) {
            return result.getRowMetadata();
        }
    }
}
