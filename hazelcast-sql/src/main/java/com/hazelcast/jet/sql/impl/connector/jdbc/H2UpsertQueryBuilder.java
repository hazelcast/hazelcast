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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import org.apache.calcite.sql.SqlDialect;

import java.util.List;
import java.util.stream.Collectors;

class H2UpsertQueryBuilder {

    private final String query;

    private final String quotedTableName;
    private final List<String> quotedColumnNames;

    private final List<String> quotedPrimaryKeys;

    H2UpsertQueryBuilder(JdbcTable jdbcTable, SqlDialect dialect) {
        // Quote identifiers
        quotedTableName = dialect.quoteIdentifier(jdbcTable.getExternalName());
        quotedColumnNames = jdbcTable.dbFieldNames()
                .stream()
                .map(dialect::quoteIdentifier)
                .collect(Collectors.toList());
        quotedPrimaryKeys = jdbcTable.getPrimaryKeyList()
                .stream()
                .map(dialect::quoteIdentifier)
                .collect(Collectors.toList());


        StringBuilder stringBuilder = new StringBuilder();

        getMergeClause(stringBuilder);
        getKeyClause(stringBuilder);
        getValuesClause(stringBuilder);

        query = stringBuilder.toString();
    }

    void getMergeClause(StringBuilder stringBuilder) {
        stringBuilder.append("MERGE INTO ")
                .append(quotedTableName)
                .append(" (")
                .append(String.join(",", quotedColumnNames))
                .append(") ");
    }

    void getKeyClause(StringBuilder stringBuilder) {
        stringBuilder.append("KEY (")
                .append(String.join(",", quotedPrimaryKeys))
                .append(") ");
    }

    void getValuesClause(StringBuilder stringBuilder) {
        String values = quotedColumnNames.stream()
                .map(dbFieldName -> "?")
                .collect(Collectors.joining(","));

        stringBuilder.append("VALUES (").append(values).append(")");
    }

    String query() {
        return query;
    }
}
