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

/**
 * Provides the Processor for the right side of a Join operation
 */
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

        RexNode rexNodePredicate = predicate == null ? null : predicate.unwrap(RexNode.class);
        List<RexNode> rexNodeProjection = Util.toList(projection, n -> n.unwrap(RexNode.class));

        if (!joinInfo.isEquiJoin()) {
            // Indices are not given.
            SelectQueryBuilder queryBuilder = new SelectQueryBuilder(
                    jdbcTable,
                    dialect,
                    rexNodePredicate,
                    rexNodeProjection
            );
            String selectQueryForRightSide = queryBuilder.query();
            return new JdbcJoinFullScanProcessorSupplier(
                    jdbcTable.getDataConnectionName(),
                    selectQueryForRightSide,
                    queryBuilder.converters(),
                    joinInfo,
                    context.convertProjection(projection)
            );
        } else {
            // Indices are given.
            IndexScanSelectQueryBuilder queryBuilder = new IndexScanSelectQueryBuilder(
                    jdbcTable,
                    dialect,
                    rexNodePredicate,
                    rexNodeProjection,
                    joinInfo
            );
            String selectQueryForRightSide = queryBuilder.query();
            return new JdbcJoinPredicateScanProcessorSupplier(
                    jdbcTable.getDataConnectionName(),
                    selectQueryForRightSide,
                    queryBuilder.converters(),
                    joinInfo,
                    context.convertProjection(projection)
            );
        }
    }
}
