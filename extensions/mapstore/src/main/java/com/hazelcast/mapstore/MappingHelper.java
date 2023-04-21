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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

final class MappingHelper {

    private final SqlService sqlService;

    MappingHelper(SqlService sqlService) {
        this.sqlService = sqlService;
    }

    public void createMapping(String mappingName, String tableName, String mappingColumns,
                              String dataConnectionRef, String idColumn) {

        sqlService.execute(
                "CREATE MAPPING \"" + mappingName + "\" "
                        + "EXTERNAL NAME " + externalName(tableName) + " "
                        + (mappingColumns != null ? " ( " + mappingColumns + " ) " : "")
                        + " DATA CONNECTION \"" + dataConnectionRef + "\" "
                        + " OPTIONS ("
                        + "    'idColumn' = '" + idColumn + "' "
                        + ")"
        ).close();
    }

    //package-private just for testing
    static String externalName(String tableName) {
        return splitByNonQuotedDots(tableName).stream()
                .map(unwrapFromQuotesIfPresent())
                .map(wrapWithQuotes())
                .collect(Collectors.joining("."));
    }

    private static List<String> splitByNonQuotedDots(String input) {
        List<String> result = new ArrayList<>();
        int tokenStart = 0;
        boolean inQuotes = false;
        for (int i = 0; i < input.length(); i++) {
            switch (input.charAt(i)) {
                case '\"':
                case '`':
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

    private static Function<String, String> unwrapFromQuotesIfPresent() {
        return s -> s.replaceAll("^\"|\"$|^`|`$", "");
    }

    private static Function<String, String> wrapWithQuotes() {
        return s -> "\"" + s + "\"";
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
