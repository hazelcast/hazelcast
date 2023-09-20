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

package com.hazelcast.jet.sql.impl.connector.jdbc.postgres;

import com.hazelcast.jet.sql.impl.connector.jdbc.AbstractQueryBuilder;
import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcTable;
import org.apache.calcite.sql.SqlDialect;

import java.util.Iterator;

/**
 * Builder for upsert statement
 */
public class PostgresUpsertQueryBuilder extends AbstractQueryBuilder {

    public PostgresUpsertQueryBuilder(JdbcTable jdbcTable, SqlDialect dialect) {
        super(jdbcTable, dialect);

        StringBuilder sb = new StringBuilder();

        appendInsertClause(sb);
        sb.append(' ');
        appendValuesClause(sb);
        sb.append(' ');
        appendOnConflictClause(sb);

        query = sb.toString();
    }

    void appendInsertClause(StringBuilder sb) {
        sb.append("INSERT INTO ");
        dialect.quoteIdentifier(sb, jdbcTable.getExternalNameList());
        sb.append(' ');
        appendFieldNames(sb, jdbcTable.dbFieldNames());
    }

    void appendValuesClause(StringBuilder sb) {
        sb.append("VALUES ");
        appendValues(sb, jdbcTable.dbFieldNames().size());
    }

    void appendOnConflictClause(StringBuilder sb) {
        sb.append("ON CONFLICT ");
        appendFieldNames(sb, jdbcTable.getPrimaryKeyList());
        sb.append(" DO UPDATE SET ");

        Iterator<String> it = jdbcTable.dbFieldNames().iterator();
        while (it.hasNext()) {
            String dbFieldName = it.next();
            dialect.quoteIdentifier(sb, dbFieldName);
            sb.append(" = EXCLUDED.");
            dialect.quoteIdentifier(sb, dbFieldName);
            if (it.hasNext()) {
                sb.append(',');
            }
        }
    }
}
