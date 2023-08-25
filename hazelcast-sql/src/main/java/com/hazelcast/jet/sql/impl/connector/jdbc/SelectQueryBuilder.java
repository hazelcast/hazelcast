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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class SelectQueryBuilder extends AbstractQueryBuilder {

    private final List<Integer> dynamicParams = new ArrayList<>();

    @SuppressWarnings("ExecutableStatementCount")
    SelectQueryBuilder(JdbcTable table, SqlDialect dialect, RexNode predicate, List<RexNode> projection) {
        super(table, dialect);

        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        if (!projection.isEmpty()) {
            appendProjection(sb, projection);
        } else {
            sb.append("*");
        }
        sb.append(" FROM ");
        dialect.quoteIdentifier(sb, table.getExternalNameList());
        if (predicate != null) {
            appendPredicate(sb, predicate, dynamicParams);
        }
        query = sb.toString();
    }

    private void appendProjection(StringBuilder sb, List<RexNode> projection) {
        Iterator<RexNode> it = projection.iterator();
        while (it.hasNext()) {
            RexNode node = it.next();
            sb.append(context.toSql(null, node).toSqlString(dialect).toString());
            if (it.hasNext()) {
                sb.append(',');
            }
        }
    }

    int[] parameterPositions() {
        return Ints.toArray(dynamicParams);
    }
}
