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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Iterator;
import java.util.List;

class SelectQueryBuilder extends AbstractQueryBuilder {

    private final SimpleContext simpleContext;
    private final ParamCollectingVisitor paramCollectingVisitor = new ParamCollectingVisitor();

    @SuppressWarnings("ExecutableStatementCount")
    SelectQueryBuilder(JdbcTable jdbcTable, RexNode filter, List<RexNode> projects) {
        super(jdbcTable);

        simpleContext = new SimpleContext(dialect, value -> {
            JdbcTableField field = jdbcTable.getField(value);
            return new SqlIdentifier(field.externalName(), SqlParserPos.ZERO);
        });

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        if (!projects.isEmpty()) {
            appendProjection(sb, projects);
        } else {
            sb.append("*");
        }
        sb.append(" FROM ");
        dialect.quoteIdentifier(sb, jdbcTable.getExternalNameList());
        if (filter != null) {
            SqlNode sqlNode = simpleContext.toSql(null, filter);
            sqlNode.accept(paramCollectingVisitor);
            String predicateFragment = sqlNode.toSqlString(dialect).toString();

            sb.append(" WHERE ")
              .append(predicateFragment);
        }
        query = sb.toString();
    }

    private void appendProjection(StringBuilder sb, List<RexNode> projects) {
        Iterator<RexNode> it = projects.iterator();
        while (it.hasNext()) {
            RexNode node = it.next();
            sb.append(simpleContext.toSql(null, node).toSqlString(dialect).toString());
            if (it.hasNext()) {
                sb.append(',');
            }
        }
    }

    int[] parameterPositions() {
        return paramCollectingVisitor.parameterPositions();
    }
}
