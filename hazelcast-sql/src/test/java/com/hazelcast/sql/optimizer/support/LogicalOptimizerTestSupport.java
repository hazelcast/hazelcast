/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.optimizer.support;

import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.opt.logical.AggregateLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.logical.JoinLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.logical.LogicalRel;
import com.hazelcast.sql.impl.calcite.opt.logical.MapScanLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.logical.ProjectLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.logical.RootLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PlanCreateVisitor;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.RexToExpressionVisitor;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.aggregate.AggregateExpression;
import com.hazelcast.sql.support.TestPlanNodeSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

/**
 * Utility methods for logical optimizer tests.
 */
public abstract class LogicalOptimizerTestSupport extends OptimizerTestSupport {
    @Override
    protected final boolean isOptimizePhysical() {
        return false;
    }

    /**
     * Perform logical optimization.
     *
     * @param sql SQL.
     * @return Input of the root node
     */
    protected RelNode optimizeLogical(String sql) {
        LogicalRel rel = optimize(sql).getLogical();

        RootLogicalRel root = assertRoot(rel);

        return root.getInput();
    }

    protected static RootLogicalRel assertRoot(RelNode node) {
        return assertClass(node, RootLogicalRel.class);
    }

    protected ProjectLogicalRel assertProject(RelNode rel, List<Expression<?>> expProjects) {
        ProjectLogicalRel project = assertClass(rel, ProjectLogicalRel.class);

        List<Expression<?>> projects = new ArrayList<>();

        for (RexNode projectExpr : project.getProjects()) {
            projects.add(projectExpr.accept(expressionConverter()));
        }

        expProjects = expProjects != null ? new ArrayList<>(expProjects) : new ArrayList<>();

        assertEquals(expProjects, projects);

        return project;
    }

    protected JoinLogicalRel assertJoin(RelNode rel, JoinRelType expType, Expression<?> expFilter) {
        JoinLogicalRel join = assertClass(rel, JoinLogicalRel.class);

        assertEquals(expType, join.getJoinType());

        Expression<?> filter = join.getCondition() != null ? join.getCondition().accept(expressionConverter()) : null;

        assertEquals(expFilter, filter);

        return join;
    }

    protected AggregateLogicalRel assertAggregate(RelNode rel, List<Integer> expGroup, List<AggregateExpression> expAggExps) {
        AggregateLogicalRel agg = assertClass(rel, AggregateLogicalRel.class);

        assertEquals(expGroup, agg.getGroupSet().toList());

        List<AggregateExpression> aggExps = new ArrayList<>(agg.getAggCallList().size());
        for (AggregateCall aggCall : agg.getAggCallList()) {
            aggExps.add(PlanCreateVisitor.convertAggregateCall(aggCall, TestPlanNodeSchema.INSTANCE));
        }
        assertEquals(expAggExps, aggExps);

        return agg;
    }

    protected static void assertScan(
        RelNode node,
        List<Integer> expProjects,
        Expression<?> expFilter
    ) {
        assertScan(node, null, expProjects, expFilter);
    }

    protected static void assertScan(
        RelNode node,
        String expMapName,
        List<Integer> expProjects,
        Expression<?> expFilter
    ) {
        MapScanLogicalRel scan = assertClass(node, MapScanLogicalRel.class);

        if (expMapName != null) {
            assertEquals(expMapName, scan.getMap().getName());
        }

        assertFieldIndexes(expProjects, scan.getProjects());

        Expression<?> filter = scan.getFilter() != null ? scan.getFilter().accept(expressionConverter()) : null;

        assertEquals(expFilter, filter);
    }

    @SuppressWarnings("unchecked")
    protected static <T> T assertClass(RelNode rel, Class<? extends LogicalRel> expClass) {
        assertEquals(expClass, rel.getClass());

        return (T) rel;
    }

    private static RexToExpressionVisitor expressionConverter() {
        return new RexToExpressionVisitor(TestPlanNodeSchema.INSTANCE, QueryParameterMetadata.EMPTY);
    }
}
