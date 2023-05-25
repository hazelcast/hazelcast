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

package com.hazelcast.jet.sql.impl.connector.jdbc.h2;

import com.hazelcast.jet.sql.impl.connector.jdbc.AbstractQueryBuilder;
import com.hazelcast.jet.sql.impl.connector.jdbc.JdbcTable;
import org.apache.calcite.sql.SqlDialect;

/**
 * Builder for upsert statement
 */
public class H2UpsertQueryBuilder extends AbstractQueryBuilder {

    public H2UpsertQueryBuilder(JdbcTable jdbcTable, SqlDialect dialect) {
        super(jdbcTable, dialect);

        StringBuilder sb = new StringBuilder();
        appendMergeClause(sb);
        sb.append(' ');
        appendKeyClause(sb);
        sb.append(' ');
        appendValuesClause(sb);

        query = sb.toString();
    }

    void appendMergeClause(StringBuilder sb) {
        sb.append("MERGE INTO ");
        dialect.quoteIdentifier(sb, jdbcTable.getExternalNameList());
        sb.append(' ');
        appendFieldNames(sb, jdbcTable.dbFieldNames());
    }

    void appendKeyClause(StringBuilder sb) {
        sb.append("KEY ");
        appendFieldNames(sb, jdbcTable.getPrimaryKeyList());
    }

    void appendValuesClause(StringBuilder sb) {
        sb.append("VALUES ");
        appendValues(sb, jdbcTable.dbFieldNames().size());
    }

}
