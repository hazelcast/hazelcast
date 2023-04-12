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

public class AbstractQueryBuilder {

    protected final JdbcTable jdbcTable;
    protected final SqlDialect dialect;
    protected String query;

    public AbstractQueryBuilder(JdbcTable jdbcTable) {
        this.jdbcTable = jdbcTable;
        this.dialect = jdbcTable.sqlDialect();
    }

    protected void appendFieldNames(StringBuilder sb, List<String> fieldNames) {
        sb.append('(');
        Iterator<String> it = fieldNames.iterator();
        while (it.hasNext()) {
            String fieldName = it.next();
            dialect.quoteIdentifier(sb, fieldName);
            if (it.hasNext()) {
                sb.append(',');
            }
        }
        sb.append(')');
    }

    protected void appendValues(StringBuilder sb, int count) {
        sb.append('(');
        for (int i = 0; i < count; i++) {
            sb.append('?');
            if (i < (count - 1)) {
                sb.append(',');
            }
        }
        sb.append(')');
    }

    /**
     * Returns the built upsert statement
     */
    public String query() {
        return query;
    }
}
