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
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;

import java.util.List;

final class MappingHelper {

    private final SqlDialect dialect = CalciteSqlDialect.DEFAULT;
    private final SqlService sqlService;

    MappingHelper(SqlService sqlService) {
        this.sqlService = sqlService;
    }

    public void createMapping(
            String mappingName,
            String tableName,
            List<SqlColumnMetadata> mappingColumns,
            String dataConnectionRef,
            String idColumn
    ) {

        sqlService.execute(
                createMappingQuery(mappingName, tableName, mappingColumns, dataConnectionRef, idColumn)
        ).close();
    }

    private String createMappingQuery(
            String mappingName,
            String tableName,
            List<SqlColumnMetadata> mappingColumns,
            String dataConnectionRef,
            String idColumn
    ) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE MAPPING ");
        dialect.quoteIdentifier(sb, mappingName);
        sb.append(" EXTERNAL NAME ");
        dialect.quoteIdentifier(sb, tableName);
        if (mappingColumns != null) {
            sb.append(" ( ");
            for (SqlColumnMetadata mc : mappingColumns) {
                dialect.quoteIdentifier(sb, mc.getName());
                sb.append(' ');
                sb.append(mc.getType());
            }
            sb.append(" )");
        }
        sb.append(" DATA CONNECTION ");
        dialect.quoteIdentifier(sb, dataConnectionRef);
        sb.append(" OPTIONS (");
        sb.append(" 'idColumn' = ");
        dialect.quoteStringLiteral(sb, null, idColumn);
        sb.append(" )");
        String createMappingQuery = sb.toString();
        return createMappingQuery;
    }

    public void dropMapping(String mappingName) {
        StringBuilder sb = new StringBuilder()
                .append("DROP MAPPING IF EXISTS ");
        dialect.quoteIdentifier(sb, mappingName);
        sqlService.execute(sb.toString()).close();
    }

    public List<SqlColumnMetadata> loadColumnMetadataFromMapping(String mapping) {
        return loadRowMetadataFromMapping(mapping).getColumns();
    }

    public SqlRowMetadata loadRowMetadataFromMapping(String mapping) {
        StringBuilder sb = new StringBuilder()
                .append("SELECT * FROM ");
        dialect.quoteIdentifier(sb, mapping);
        sb.append(" LIMIT 0");
        try (SqlResult result = sqlService.execute(sb.toString())) {
            return result.getRowMetadata();
        }
    }
}
