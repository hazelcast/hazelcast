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

import com.hazelcast.jet.sql.impl.HazelcastRexBuilder;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.sql.impl.expression.predicate.OrPredicate;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class IndexScanSelectQueryBuilder extends AbstractQueryBuilder {
    static final String ROW_NUMBER_LITERAL = "'replacerownumber'";

    private static final String ROW_NUMBER = "replacerownumber";

    IndexScanSelectQueryBuilder(JdbcTable table,
                                SqlDialect dialect,
                                List<RexNode> projection,
                                JetJoinInfo joinInfo) {
        super(table, dialect);

        List<RexNode> newProjection = new ArrayList<>(projection);
        RexLiteral rexLiteral = HazelcastRexBuilder.INSTANCE.makeLiteral(ROW_NUMBER);
        newProjection.add(rexLiteral);

        SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder(table, dialect, null, newProjection);
        this.query = selectQueryBuilder.query();

        // Create the where clause from indices
        StringBuilder stringBuilder = new StringBuilder(this.query);
        appendIndices(stringBuilder, joinInfo);
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
}
