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

package com.hazelcast.jet.sql.impl.connector.jdbc.mssql;

import com.hazelcast.jet.sql.impl.connector.jdbc.AbstractQueryBuilder;
import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcTable;
import org.apache.calcite.sql.SqlDialect;

import java.util.Iterator;
import java.util.List;

/**
 * Builder for upsert statement in Microsoft SQL Server syntax.
 */
public class MSSQLUpsertQueryBuilder extends AbstractQueryBuilder {

    public MSSQLUpsertQueryBuilder(JdbcTable jdbcTable, SqlDialect dialect) {
        super(jdbcTable, dialect);

        StringBuilder sb = new StringBuilder();

        appendIFClause(sb);
        sb.append(' ');
        appendValuesClause(sb);
        sb.append(' ');
        appendElseClause(sb);

        query = sb.toString();
    }

    void appendIFClause(StringBuilder sb) {
        sb.append("IF NOT EXISTS (SELECT 1 FROM ");
        dialect.quoteIdentifier(sb, jdbcTable.getExternalNameList());
        appendPrimaryKeys(sb);
        sb.append(") BEGIN INSERT INTO ");
        dialect.quoteIdentifier(sb, jdbcTable.getExternalNameList());
        sb.append(' ');
        appendFieldNames(sb, jdbcTable.dbFieldNames());
    }

    void appendValuesClause(StringBuilder sb) {
        sb.append("VALUES ");
        appendValues(sb, jdbcTable.dbFieldNames().size());
        sb.append(" END");
    }

    void appendElseClause(StringBuilder sb) {
        sb.append("ELSE BEGIN UPDATE ");
        dialect.quoteIdentifier(sb, jdbcTable.getExternalNameList());
        sb.append(" SET ");
        Iterator<String> it = jdbcTable.dbFieldNames().iterator();
        while (it.hasNext()) {
            String dbFieldName = it.next();
            dialect.quoteIdentifier(sb, dbFieldName);
            sb.append(" = ? ");
            if (it.hasNext()) {
                sb.append(',');
            }
        }
        appendPrimaryKeys(sb);
        sb.append(" END");
    }

    void appendPrimaryKeys(StringBuilder sb) {
        sb.append(" WHERE ");
        List<String> pkFields = jdbcTable.getPrimaryKeyList();
        for (int i = 0; i < pkFields.size(); i++) {
            String field = pkFields.get(i);
            sb.append(dialect.quoteIdentifier(field))
                    .append("=?");
            if (i < pkFields.size() - 1) {
                sb.append(" AND ");
            }
        }
    }
}
