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
import org.apache.calcite.rel.rel2sql.SqlImplementor.Context;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Iterator;
import java.util.List;

class SelectQueryBuilder extends AbstractQueryBuilder {

    private final Context context;
    private final ParamCollectingVisitor paramCollectingVisitor = new ParamCollectingVisitor();

    @SuppressWarnings("ExecutableStatementCount")
    SelectQueryBuilder(JdbcTable table, SqlDialect dialect, RexNode filter, List<RexNode> projects) {
        super(table, dialect);

        context = new CustomContext(this.dialect, value -> {
            JdbcTableField field = table.getField(value);
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
        this.dialect.quoteIdentifier(sb, table.getExternalNameList());
        if (filter != null) {
            SqlNode sqlNode = context.toSql(null, filter);
            sqlNode.accept(paramCollectingVisitor);
            String predicateFragment = sqlNode.toSqlString(this.dialect).toString();

            sb.append(" WHERE ")
              .append(predicateFragment);
        }
        query = sb.toString();
    }

    private void appendProjection(StringBuilder sb, List<RexNode> projects) {
        Iterator<RexNode> it = projects.iterator();
        while (it.hasNext()) {
            RexNode node = it.next();
            sb.append(context.toSql(null, node).toSqlString(dialect).toString());
            if (it.hasNext()) {
                sb.append(',');
            }
        }
    }

    int[] parameterPositions() {
        return Ints.toArray(paramCollectingVisitor.parameterPositions());
    }
}
