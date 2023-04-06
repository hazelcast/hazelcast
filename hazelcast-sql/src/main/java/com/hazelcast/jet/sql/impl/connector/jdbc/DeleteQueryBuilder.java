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

import java.util.Iterator;
import java.util.List;

class DeleteQueryBuilder {

    private final String query;

    DeleteQueryBuilder(JdbcTable table, List<String> pkFields) {
        SqlDialect dialect = table.sqlDialect();

        StringBuilder sb = new StringBuilder()
                .append("DELETE FROM ");
        dialect.quoteIdentifier(sb, table.getExternalNameList());
        sb.append(" WHERE ");
        Iterator<String> it = pkFields.iterator();
        while (it.hasNext()) {
            String pkField = it.next();
            dialect.quoteIdentifier(sb, pkField);
            sb.append("=?");
            if (it.hasNext()) {
                sb.append(" AND ");
            }
        }

        query = sb.toString();
    }

    String query() {
        return query;
    }
}
