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

class H2UpsertQueryBuilder {

    private final String query;

    H2UpsertQueryBuilder(JdbcTable jdbcTable) {
        StringBuilder stringBuilder = new StringBuilder();

        getMergeClause(jdbcTable, stringBuilder);
        getKeyClause(jdbcTable, stringBuilder);
        getValuesClause(jdbcTable, stringBuilder);

        query = stringBuilder.toString();
    }

    protected void getMergeClause(JdbcTable jdbcTable, StringBuilder stringBuilder) {

        stringBuilder.append("MERGE INTO ")
                .append(jdbcTable.getExternalName())
                .append(" ( ")
                .append(String.join(",", jdbcTable.dbFieldNames()))
                .append(" ) ");
    }

    protected void getKeyClause(JdbcTable jdbcTable, StringBuilder stringBuilder) {
        stringBuilder.append("KEY (")
                .append(String.join(",", jdbcTable.getPrimaryKeyList()))
                .append(" ) ");
    }

    protected void getValuesClause(JdbcTable jdbcTable, StringBuilder stringBuilder) {

        List<String> dbFieldNames = jdbcTable.dbFieldNames();

        stringBuilder.append(" VALUES (");

        for (int i = 0; i < dbFieldNames.size(); i++) {
            stringBuilder.append('?');
            if (i < (dbFieldNames.size() - 1)) {
                stringBuilder.append(", ");
            }
        }
        stringBuilder.append(')');
    }

    String query() {
        return query;
    }
}
