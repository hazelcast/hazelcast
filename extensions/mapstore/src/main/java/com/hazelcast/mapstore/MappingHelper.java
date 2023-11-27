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
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

final class MappingHelper {

    private static final SqlDialect DIALECT = CalciteSqlDialect.DEFAULT;
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
        DIALECT.quoteIdentifier(sb, mappingName);
        sb.append(" EXTERNAL NAME ");
        quoteExternalName(sb, tableName);
        if (mappingColumns != null) {
            sb.append(" ( ");
            for (Iterator<SqlColumnMetadata> iterator = mappingColumns.iterator(); iterator.hasNext(); ) {
                SqlColumnMetadata mc = iterator.next();
                DIALECT.quoteIdentifier(sb, mc.getName());
                sb.append(' ');
                sb.append(mc.getType());
                if (iterator.hasNext()) {
                    sb.append(", ");
                }
            }
            sb.append(" )");
        }
        sb.append(" DATA CONNECTION ");
        DIALECT.quoteIdentifier(sb, dataConnectionRef);
        sb.append(" OPTIONS (");
        sb.append(" 'idColumn' = ");
        DIALECT.quoteStringLiteral(sb, null, idColumn);
        sb.append(" )");
        return sb.toString();

    }

    //package-private just for testing
    static void quoteExternalName(StringBuilder sb, String externalName) {
        List<String> parts = splitByNonQuotedDots(externalName);
        for (int i = 0; i < parts.size(); i++) {
            String unescaped = unescapeQuotes(parts.get(i));
            String unquoted = unquoteIfQuoted(unescaped);
            DIALECT.quoteIdentifier(sb, unquoted);
            if (i < parts.size() - 1) {
                sb.append(".");
            }
        }
    }

    private static List<String> splitByNonQuotedDots(String input) {
        List<String> result = new ArrayList<>();
        int tokenStart = 0;
        boolean inQuotes = false;
        for (int i = 0; i < input.length(); i++) {
            switch (input.charAt(i)) {
                case '\"':
                    inQuotes = !inQuotes;
                    break;
                case '.':
                    if (!inQuotes) {
                        result.add(input.substring(tokenStart, i));
                        tokenStart = i + 1;
                    }
                    break;
                default:
            }
        }
        result.add(input.substring(tokenStart));
        return result;
    }

    private static String unescapeQuotes(String input) {
        return input.replaceAll("\"\"", "\"");
    }

    private static String unquoteIfQuoted(String input) {
        return input.replaceAll("^\"|\"$", "");
    }

    public void dropMapping(String mappingName) {
        StringBuilder sb = new StringBuilder()
                .append("DROP MAPPING IF EXISTS ");
        DIALECT.quoteIdentifier(sb, mappingName);
        sqlService.execute(sb.toString()).close();
    }

    public List<SqlColumnMetadata> loadColumnMetadataFromMapping(String mapping) {
        String query = "SELECT * FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position ASC";
        try (SqlResult result = sqlService.execute(query, mapping)) {
            return StreamSupport
                    .stream(result.spliterator(), false)
                    .map(row -> {
                        String name = row.getObject("column_name");
                        String typeString = ((String) row.getObject("data_type")).replaceAll(" ", "_");
                        SqlColumnType type = SqlColumnType.valueOf(typeString);
                        boolean isNullable = Boolean.parseBoolean(row.getObject("is_nullable"));
                        return new SqlColumnMetadata(name, type, isNullable);
                    })
                    .collect(Collectors.toList());
        }
    }
}
