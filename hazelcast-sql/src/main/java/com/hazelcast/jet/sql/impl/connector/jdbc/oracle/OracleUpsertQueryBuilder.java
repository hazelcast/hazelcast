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

package com.hazelcast.jet.sql.impl.connector.jdbc.oracle;

import com.hazelcast.jet.sql.impl.connector.jdbc.AbstractQueryBuilder;
import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcTable;
import org.apache.calcite.sql.SqlDialect;

import java.util.Iterator;
import java.util.List;

/**
 * Upsert statement builder for Oracle
 */
public class OracleUpsertQueryBuilder extends AbstractQueryBuilder {

    public OracleUpsertQueryBuilder(JdbcTable jdbcTable, SqlDialect dialect) {
        super(jdbcTable, dialect);

        StringBuilder sb = new StringBuilder();

        appendMergeClause(sb);
        sb.append(' ');
        appendMatchedClause(sb);

        query = sb.toString();
    }

    void appendMergeClause(StringBuilder sb) {
        sb.append("MERGE INTO ");
        dialect.quoteIdentifier(sb, jdbcTable.getExternalNameList());
        sb.append(" TARGET USING (SELECT");
        Iterator<String> it = jdbcTable.dbFieldNames().iterator();
        while (it.hasNext()) {
            String dbFieldName = it.next();
            sb.append(" ? as ");
            dialect.quoteIdentifier(sb, dbFieldName);

            if (it.hasNext()) {
                sb.append(',');
            }
        }
        sb.append(" FROM dual) SOURCE");
        sb.append(" ON (");
        appendPrimaryKeys(sb);
        sb.append(")");
    }

    void appendMatchedClause(StringBuilder sb) {
        sb.append("WHEN MATCHED THEN ");
        sb.append("UPDATE ");
        sb.append("SET ");
        List<String> pkList = jdbcTable.getPrimaryKeyList();
        Iterator<String> it = jdbcTable.dbFieldNames().iterator();
        while (it.hasNext()) {
            String dbFieldName = it.next();
            // Oracle doesn't allow updating the values referenced in the ON clause i.e. the primary keys
            // Skip the primary keys.
            if (pkList.contains(dbFieldName)) {
                continue;
            }

            sb.append("TARGET.");
            dialect.quoteIdentifier(sb, dbFieldName);
            sb.append(" = SOURCE.");
            dialect.quoteIdentifier(sb, dbFieldName);
            if (it.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append(" WHEN NOT MATCHED THEN INSERT ");
        appendFieldNames(sb, jdbcTable.dbFieldNames());
        sb.append(" VALUES");
        appendSourceFieldNames(sb, jdbcTable.dbFieldNames());
    }

    void appendPrimaryKeys(StringBuilder sb) {
        List<String> pkFields = jdbcTable.getPrimaryKeyList();
        for (int i = 0; i < pkFields.size(); i++) {
            String field = pkFields.get(i);
            sb.append("TARGET.");
            dialect.quoteIdentifier(sb, field);
            sb.append(" = SOURCE.");
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
            sb.append("SOURCE.");
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

}
