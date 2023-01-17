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

import java.util.List;
import java.util.stream.Collectors;

class MySqlUpsertQueryBuilder {

    private final String query;

    MySqlUpsertQueryBuilder(JdbcTable jdbcTable) {
        StringBuilder stringBuilder = new StringBuilder();

        getInsertClause(jdbcTable, stringBuilder);
        getValuesClause(jdbcTable, stringBuilder);
        getOnDuplicateClause(jdbcTable, stringBuilder);

        query = stringBuilder.toString();
    }

    protected void getInsertClause(JdbcTable jdbcTable, StringBuilder stringBuilder) {

        stringBuilder.append("INSERT INTO ")
                .append(jdbcTable.getExternalName())
                .append(" (")
                .append(String.join(",", jdbcTable.dbFieldNames()))
                .append(") ");
    }

    protected void getValuesClause(JdbcTable jdbcTable, StringBuilder stringBuilder) {

        List<String> dbFieldNames = jdbcTable.dbFieldNames();

        String values = dbFieldNames.stream()
                .map(dbFieldName -> "?")
                .collect(Collectors.joining(","));

        String format = String.format("VALUES (%s) ", values);
        stringBuilder.append(format);
    }

    protected void getOnDuplicateClause(JdbcTable jdbcTable, StringBuilder stringBuilder) {
        String values = jdbcTable.dbFieldNames().stream()
                .map(dbFieldName -> String.format("%s = VALUES(%s)", dbFieldName, dbFieldName))
                .collect(Collectors.joining(","));

        String clause = String.format("ON DUPLICATE KEY UPDATE %s", values);
        stringBuilder.append(clause);
    }

    String query() {
        return query;
    }
}
