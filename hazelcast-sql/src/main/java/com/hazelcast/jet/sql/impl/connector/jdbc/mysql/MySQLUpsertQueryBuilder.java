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

package com.hazelcast.jet.sql.impl.connector.jdbc.mysql;

import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcTable;
import org.apache.calcite.sql.SqlDialect;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Builder for upsert statement
 */
public class MySQLUpsertQueryBuilder {

    private final String query;

    private final String quotedTableName;
    private final List<String> quotedColumnNames;

    public MySQLUpsertQueryBuilder(JdbcTable jdbcTable) {
        SqlDialect sqlDialect = jdbcTable.sqlDialect();

        // Quote identifiers
        quotedTableName = sqlDialect.quoteIdentifier(jdbcTable.getExternalName());
        quotedColumnNames = jdbcTable.dbFieldNames()
                .stream()
                .map(sqlDialect::quoteIdentifier)
                .collect(Collectors.toList());

        StringBuilder stringBuilder = new StringBuilder();

        getInsertClause(stringBuilder);
        getValuesClause(stringBuilder);
        getOnDuplicateClause(stringBuilder);

        query = stringBuilder.toString();
    }

    void getInsertClause(StringBuilder stringBuilder) {
        stringBuilder.append("INSERT INTO ")
                .append(quotedTableName)
                .append(" (")
                .append(String.join(",", quotedColumnNames))
                .append(") ");
    }

    void getValuesClause(StringBuilder stringBuilder) {
        String values = quotedColumnNames.stream()
                .map(dbFieldName -> "?")
                .collect(Collectors.joining(","));

        stringBuilder.append("VALUES (").append(values).append(") ");
    }

    void getOnDuplicateClause(StringBuilder stringBuilder) {
        String values = quotedColumnNames.stream()
                .map(dbFieldName -> dbFieldName + " = VALUES(" + dbFieldName + ")")
                .collect(Collectors.joining(","));

        stringBuilder.append("ON DUPLICATE KEY UPDATE ").append(values);
    }

    /**
     * Returns the built upsert statement
     */
    public String query() {
        return query;
    }
}
