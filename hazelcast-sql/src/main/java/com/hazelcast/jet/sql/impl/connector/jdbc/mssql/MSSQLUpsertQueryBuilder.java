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
 * Upsert statement builder in Microsoft SQL Server syntax.
 */
public class MSSQLUpsertQueryBuilder extends AbstractQueryBuilder {

    public MSSQLUpsertQueryBuilder(JdbcTable jdbcTable, SqlDialect dialect) {
        super(jdbcTable, dialect);

        StringBuilder sb = new StringBuilder();

        appendMergeClause(sb);
        sb.append(' ');
        appendMatchedClause(sb);

        query = sb.toString();
    }

    void appendMergeClause(StringBuilder sb) {
        sb.append("MERGE ");
        dialect.quoteIdentifier(sb, jdbcTable.getExternalNameList());
        sb.append(" USING (");
        appendValuesClause(sb);
        sb.append(") AS source ");
        appendFieldNames(sb, jdbcTable.dbFieldNames());
        sb.append(" ON ");
        appendPrimaryKeys(sb);
    }

    void appendValuesClause(StringBuilder sb) {
        sb.append("VALUES ");
        appendValues(sb, jdbcTable.dbFieldNames().size());
    }

    void appendMatchedClause(StringBuilder sb) {
        sb.append("WHEN MATCHED THEN ");
        sb.append("UPDATE ");
        sb.append("SET");
        Iterator<String> it = jdbcTable.dbFieldNames().iterator();
        while (it.hasNext()) {
            String dbFieldName = it.next();
            sb.append(' ');
            dialect.quoteIdentifier(sb, dbFieldName);
            sb.append(" = source.");
            dialect.quoteIdentifier(sb, dbFieldName);
            if (it.hasNext()) {
                sb.append(',');
            }
        }
        sb.append(" WHEN NOT MATCHED THEN INSERT ");
        appendFieldNames(sb, jdbcTable.dbFieldNames());
        sb.append(" VALUES");
        appendSourceFieldNames(sb, jdbcTable.dbFieldNames());
        sb.append(";");
    }

    void appendPrimaryKeys(StringBuilder sb) {
        List<String> pkFields = jdbcTable.getPrimaryKeyList();
        for (int i = 0; i < pkFields.size(); i++) {
            String field = pkFields.get(i);
            dialect.quoteIdentifier(sb, jdbcTable.getExternalNameList());
            sb.append(".");
            dialect.quoteIdentifier(sb, field);
            sb.append(" = source.");
            dialect.quoteIdentifier(sb, field);
            if (i < pkFields.size() - 1) {
                sb.append(" AND ");
            }
        }
    }

    void appendSourceFieldNames(StringBuilder sb, List<String> fieldNames) {
        sb.append('(');
        Iterator<String> it = fieldNames.iterator();
        while (it.hasNext()) {
            String fieldName = it.next();
            sb.append("source.");
            dialect.quoteIdentifier(sb, fieldName);
            if (it.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append(')');
    }

    @Override
    protected void appendFieldNames(StringBuilder sb, List<String> fieldNames) {
        sb.append('(');
        Iterator<String> it = fieldNames.iterator();
        while (it.hasNext()) {
            String fieldName = it.next();
            dialect.quoteIdentifier(sb, fieldName);
            if (it.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append(')');
    }

    @Override
    protected void appendValues(StringBuilder sb, int count) {
        sb.append('(');
        for (int i = 0; i < count; i++) {
            sb.append('?');
            if (i < (count - 1)) {
                sb.append(", ");
            }
        }
        sb.append(')');
    }
}
