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

import java.util.Arrays;
import java.util.Iterator;

class InsertQueryBuilder {

    private final String query;

    InsertQueryBuilder(JdbcTable jdbcTable, SqlDialect dialect) {
        StringBuilder sb = new StringBuilder()
                .append("INSERT INTO ");
        dialect.quoteIdentifier(sb, Arrays.asList(jdbcTable.getExternalName()));
        sb.append(" ( ");
        Iterator<String> it = jdbcTable.dbFieldNames().iterator();
        while (it.hasNext()) {
            String dbFieldName = it.next();
            dialect.quoteIdentifier(sb, dbFieldName);
            if (it.hasNext()) {
                sb.append(',');
            }
        }
        sb.append(" ) ")
          .append(" VALUES (");

        for (int i = 0; i < jdbcTable.dbFieldNames().size(); i++) {
            sb.append('?');
            if (i < (jdbcTable.dbFieldNames().size() - 1)) {
                sb.append(", ");
            }
        }
        sb.append(')');
        query = sb.toString();
    }

    String query() {
        return query;
    }
}
