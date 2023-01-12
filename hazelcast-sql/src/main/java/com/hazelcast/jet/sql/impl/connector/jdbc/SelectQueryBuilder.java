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

import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import org.apache.calcite.rel.rel2sql.SqlImplementor.SimpleContext;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

import static java.util.stream.Collectors.joining;

class SelectQueryBuilder {

    private final String query;
    private final ParamCollectingVisitor paramCollectingVisitor = new ParamCollectingVisitor();

    @SuppressWarnings("ExecutableStatementCount")
    SelectQueryBuilder(HazelcastTable hzTable) {
        JdbcTable table = hzTable.getTarget();
        SqlDialect dialect = table.sqlDialect();

        SimpleContext simpleContext = new SimpleContext(dialect, value -> {
            JdbcTableField field = table.getField(value);
            return new SqlIdentifier(field.externalName(), SqlParserPos.ZERO);
        });

        RexNode filter = hzTable.getFilter();
        String predicateFragment = null;
        if (filter != null) {
            SqlNode sqlNode = simpleContext.toSql(null, filter);
            sqlNode.accept(paramCollectingVisitor);
            predicateFragment = sqlNode.toSqlString(dialect).toString();
        }

        String projectionFragment = null;
        List<RexNode> projects = hzTable.getProjects();
        if (!projects.isEmpty()) {
            projectionFragment = projects.stream()
                                         .map(proj -> simpleContext.toSql(null, proj).toSqlString(dialect).toString())
                                         .collect(joining(","));
        }

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        if (projectionFragment != null) {
            sb.append(projectionFragment);
        } else {
            sb.append("*");
        }
        sb.append(" FROM ")
          .append(table.getExternalName());
        if (predicateFragment != null) {
            sb.append(" WHERE ")
              .append(predicateFragment);
        }
        query = sb.toString();
    }

    String query() {
        return query;
    }

    int[] parameterPositions() {
        return paramCollectingVisitor.parameterPositions();
    }
}
