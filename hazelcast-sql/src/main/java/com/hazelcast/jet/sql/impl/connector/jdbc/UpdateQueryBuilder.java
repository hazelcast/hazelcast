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
import org.apache.calcite.util.Pair;

import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.joining;

class UpdateQueryBuilder extends AbstractQueryBuilder {

    private final String query;
    private final ParamCollectingVisitor paramCollectingVisitor = new ParamCollectingVisitor();
    private final CustomContext context;
    private final List<Integer> parameterPositions;

    UpdateQueryBuilder(
            JdbcTable table,
            SqlDialect dialect,
            List<String> pkFields,
            List<String> fieldNames,
            List<RexNode> expressions,
            RexNode filter,
            boolean hasInput) {

        super(table, dialect);

        context = new CustomContext(dialect, value -> {
            JdbcTableField field = table.getField(value);
            return new SqlIdentifier(field.externalName(), SqlParserPos.ZERO);
        });

        assert fieldNames.size() == expressions.size();

        String setSqlFragment = Pair.zip(fieldNames, expressions).stream()
                .map(pair -> {
                    SqlNode sqlNode = context.toSql(null, pair.right);
                    sqlNode.accept(paramCollectingVisitor);
                    String externalFieldName = table.getField(pair.left).externalName();
                    return dialect.quoteIdentifier(externalFieldName)
                            + "="
                            + sqlNode.toSqlString(dialect).toString();
                })
                .collect(joining(", "));

        String whereClause = null;
        if (filter != null) {
            SqlNode sqlNode = context.toSql(null, filter);
            sqlNode.accept(paramCollectingVisitor);
            whereClause = sqlNode.toSqlString(dialect).toString();
        }

        parameterPositions = paramCollectingVisitor.parameterPositions();
        if (hasInput) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < pkFields.size(); i++) {
                String field = pkFields.get(i);
                String externalFieldName = table.getField(field).externalName();
                sb.append(dialect.quoteIdentifier(externalFieldName))
                        .append("=?");
                if (i < pkFields.size() - 1) {
                    sb.append(" AND ");
                }
                parameterPositions.add(-i - 1);
            }
            whereClause = sb.toString();
        }


        query = "UPDATE " + dialect.quoteIdentifier(new StringBuilder(), Arrays.asList(table.getExternalName())) +
                " SET " + setSqlFragment +
                (whereClause != null ? " WHERE " + whereClause : "");
    }

    public String query() {
        return query;
    }

    int[] parameterPositions() {
        return Ints.toArray(parameterPositions);
    }
}
