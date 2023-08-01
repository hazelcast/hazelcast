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

import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.connector.HazelcastRexNode;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.sql.impl.expression.Expression;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDialect;

import java.util.List;

public class NestedLoopReaderParams {

    private final SqlConnector.DagBuildContext context;
    private final HazelcastRexNode predicate;

    // Columns of table as HazelcastRexNode
    private final List<HazelcastRexNode> projection;
    private final JetJoinInfo joinInfo;

    // The derived parameters to help with coding are below
    private SqlDialect sqlDialect;

    private JdbcTable jdbcTable;

    private RexNode rexPredicate;

    // Columns of table as RexNode
    private List<RexNode> rexProjection;

    private List<Expression<?>> projections;

    public NestedLoopReaderParams(SqlConnector.DagBuildContext context,
                                  HazelcastRexNode predicate,
                                  List<HazelcastRexNode> projection,
                                  JetJoinInfo joinInfo) {
        this.context = context;
        this.predicate = predicate;
        this.projection = projection;
        this.joinInfo = joinInfo;
    }

    public JetJoinInfo getJoinInfo() {
        return joinInfo;
    }

    public SqlDialect getSqlDialect() {
        return sqlDialect;
    }

    public void setSqlDialect(SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    public JdbcTable getJdbcTable() {
        return jdbcTable;
    }

    public RexNode getRexPredicate() {
        return rexPredicate;
    }

    public List<RexNode> getRexProjection() {
        return rexProjection;
    }

    public List<Expression<?>> getProjections() {
        return projections;
    }

    public void setDerivedParameters() {
        jdbcTable = context.getTable();

        rexPredicate = predicate == null ? null : predicate.unwrap(RexNode.class);
        rexProjection = Util.toList(projection, n -> n.unwrap(RexNode.class));

        projections = context.convertProjection(projection);
    }
}
