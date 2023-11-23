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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.predicate.OrPredicate;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds an SQL query that uses the given projection and predicate for the right side of a Join operation.
 * This SQL query also includes a hidden column, which is used by the processor to determine the correspondence
 * between rows on the left side and the right side.
 * <p>
 * For example:
 * <pre>
 * Left Side Row Position   |   Right Side
 * -------------------------|--------------
 * 0                        |   0
 *                          |   0
 *                          |   0
 * 1                        |   1
 * 2                        |   2
 *                          |   2
 *                          |   2
 * </pre>
 * <p>
 * In this scenario, we have received 3 rows from the left side. Then we execute an SQL query for each left row
 * with the UNION ALL clause. It's important to note that the UNION ALL clause does not guarantee the result to be
 * in order. Therefore, we sort the result by a secret column used as ROW_NUMBER_ALIAS to maintain order.
 */
class IndexScanSelectQueryBuilder extends AbstractQueryBuilder {
    protected static final String ROW_NUMBER = "replacerownumber";
    protected static final String ROW_NUMBER_ALIAS = "CONFIDENTIAL_ROWNUM";

    private final List<Integer> dynamicParams = new ArrayList<>();
    private final List<FunctionEx<Object, ?>> converters = new ArrayList<>();

    IndexScanSelectQueryBuilder(JdbcTable table,
                                SqlDialect dialect,
                                RexNode predicate,
                                List<RexNode> projection,
                                JetJoinInfo joinInfo) {
        super(table, dialect);

        // Add new column with an alias to SQL. This alias will be used by the processor to sort the results
        SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder(table, dialect, null, projection) {

            @Override
            protected void appendProjection(StringBuilder sb, List<RexNode> projection) {
                super.appendProjection(sb, projection);
                sb.append(",").append(ROW_NUMBER).append(" AS ").append(ROW_NUMBER_ALIAS);
            }
        };
        this.query = selectQueryBuilder.query();
        this.converters.addAll(selectQueryBuilder.converters());
        this.converters.add(FunctionEx.identity()); // for CONFIDENTIAL_ROWNUM

        // Create the where clause from indices
        StringBuilder stringBuilder = new StringBuilder(this.query);
        appendIndices(stringBuilder, joinInfo);
        appendPredicate(stringBuilder, predicate, dynamicParams);

        query = stringBuilder.toString();
    }


    private void appendIndices(StringBuilder stringBuilder, JetJoinInfo joinInfo) {
        stringBuilder.append(" WHERE ");

        // Join On Multiple Conditions usually use AND predicate
        // e.g.  "JOIN table2 ON table1.column = table2.column AND table1.column1 = table2.column1"
        String delimiter = "AND ";

        // Check it condition is OR
        if (joinInfo.condition() instanceof OrPredicate) {
            delimiter = "OR ";
        }
        int[] rightEquiJoinIndices = joinInfo.rightEquiJoinIndices();
        String whereClause = Arrays.stream(rightEquiJoinIndices)
                .mapToObj(index -> {
                    // Get the field defined with "CREATE MAPPING ...."
                    JdbcTableField jdbcTableField = jdbcTable.getField(index);
                    String quotedIdentifier = dialect.quoteIdentifier(jdbcTableField.externalName());
                    return quotedIdentifier + " = ? ";
                })
                .collect(Collectors.joining(delimiter));
        stringBuilder.append(whereClause);
    }

    @Override
    protected void appendPredicate(StringBuilder sb, RexNode predicate, List<Integer> parameterPositions) {
        if (predicate != null) {
            SqlNode sqlNode = context.toSql(null, predicate);
            sqlNode.accept(new ParamCollectingVisitor(parameterPositions));
            String predicateFragment = sqlNode.toSqlString(dialect).toString();

            sb.append(" AND ")
                    .append(predicateFragment);
        }
    }

    public List<FunctionEx<Object, ?>> converters() {
        return converters;
    }
}
