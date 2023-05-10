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

import com.google.common.primitives.Ints;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

class DeleteQueryBuilder {

    private final String query;
    private final ParamCollectingVisitor paramCollectingVisitor = new ParamCollectingVisitor();
    private final CustomContext context;
    private final List<Integer> parameterPositions;


    DeleteQueryBuilder(
            JdbcTable table,
            SqlDialect dialect,
            RexNode predicate,
            boolean hasInput) {


        context = new CustomContext(dialect, value -> {
            JdbcTableField field = table.getField(value);
            return new SqlIdentifier(field.externalName(), SqlParserPos.ZERO);
        });

        StringBuilder sb = new StringBuilder()
                .append("DELETE FROM ");
        dialect.quoteIdentifier(sb, table.getExternalNameList());
        if (predicate != null) {
            sb.append(" WHERE ");
            SqlNode sqlNode = context.toSql(null, predicate);
            sqlNode.accept(paramCollectingVisitor);
            sb.append(sqlNode.toSqlString(dialect).toString());
        }

        parameterPositions = paramCollectingVisitor.parameterPositions();

        if (hasInput) {
            sb.append(" WHERE ");
            List<String> pks = table.getPrimaryKeyList();
            for (int i = 0; i < pks.size(); i++) {
                String pkField = pks.get(i);
                dialect.quoteIdentifier(sb, pkField);
                sb.append("=?");
                if (i < pks.size() - 1) {
                    sb.append(" AND ");
                }
                parameterPositions.add(-i - 1);
            }
        }

        query = sb.toString();
    }

    String query() {
        return query;
    }


    int[] parameterPositions() {
        return Ints.toArray(parameterPositions);
    }
}
