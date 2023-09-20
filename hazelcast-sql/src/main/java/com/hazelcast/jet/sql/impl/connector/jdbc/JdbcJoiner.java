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

import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.HazelcastRexNode;
import com.hazelcast.jet.sql.impl.connector.SqlConnector.DagBuildContext;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;

import java.util.List;

import static com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnector.resolveDialect;

public final class JdbcJoiner {

    private JdbcJoiner() {
    }

    public static ProcessorSupplier createJoinProcessorSupplier(
            JetJoinInfo joinInfo,
            DagBuildContext context,
            HazelcastRexNode predicate,
            List<HazelcastRexNode> projection) {

        JdbcTable jdbcTable = context.getTable();
        SqlDialect dialect = resolveDialect(jdbcTable, context);

        if (!joinInfo.isEquiJoin()) {
            // Indices are not given
            SelectQueryBuilder queryBuilder = new SelectQueryBuilder(
                    jdbcTable,
                    dialect,
                    predicate == null ? null : predicate.unwrap(RexNode.class),
                    Util.toList(projection, n -> n.unwrap(RexNode.class))
            );
            String selectQuery = queryBuilder.query();
            return new JdbcJoinFullScanProcessorSupplier(
                    jdbcTable.getDataConnectionName(),
                    selectQuery,
                    joinInfo,
                    context.convertProjection(projection)
            );
        } else {
            // TODO predicate is not used in this branch - see failing test in
            // JdbcInnerEquiJoinTest.joinWithOtherJdbcWhereClauseOnRightSideColumn

            // Indices are given
            IndexScanSelectQueryBuilder queryBuilder = new IndexScanSelectQueryBuilder(
                    jdbcTable,
                    dialect,
                    Util.toList(projection, n -> n.unwrap(RexNode.class)),
                    joinInfo
            );
            String selectQuery = queryBuilder.query();
            return new JdbcJoinIndexScanProcessorSupplier(
                    jdbcTable.getDataConnectionName(),
                    selectQuery,
                    joinInfo,
                    context.convertProjection(projection)
            );
        }
    }

}
