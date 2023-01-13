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

import org.apache.calcite.rel.rel2sql.SqlImplementor.SimpleContext;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.joining;

class UpdateQueryBuilder {

    private final String query;
    private final ParamCollectingVisitor paramCollectingVisitor = new ParamCollectingVisitor();

    UpdateQueryBuilder(JdbcTable table, List<String> pkFields, Map<String, RexNode> updates) {
        SqlDialect dialect = table.sqlDialect();
        SimpleContext simpleContext = new SimpleContext(dialect, value -> {
            JdbcTableField field = table.getField(value);
            return new SqlIdentifier(field.externalName(), SqlParserPos.ZERO);
        });

        String setSqlFragment = updates.entrySet().stream()
                                       .map(entry -> {
                                           SqlNode sqlNode = simpleContext.toSql(null, entry.getValue());
                                           sqlNode.accept(paramCollectingVisitor);
                                           return '\"' + table.getField(entry.getKey()).externalName() + "\" ="
                                                   + sqlNode.toSqlString(dialect).toString();
                                       })
                                       .collect(joining(", "));

        String whereClause = pkFields.stream().map(e -> '\"' + e + "\" = ?")
                                     .collect(joining(" AND "));

        query = "UPDATE " + table.getExternalName() +
                " SET " + setSqlFragment +
                " WHERE " + whereClause;
    }

    String query() {
        return query;
    }

    int[] parameterPositions() {
        return paramCollectingVisitor.parameterPositions();
    }
}
