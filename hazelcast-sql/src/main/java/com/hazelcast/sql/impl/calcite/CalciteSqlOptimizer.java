/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.JetSqlCoreBackend;
import com.hazelcast.sql.impl.calcite.opt.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;

import javax.annotation.Nullable;

import static com.hazelcast.jet.impl.util.Util.checkJetIsEnabled;

/**
 * SQL optimizer based on Apache Calcite.
 * <p>
 * After parsing and initial sql-to-rel conversion is finished, all relational nodes start with {@link Convention#NONE}
 * convention. Such nodes are typically referred as "abstract" in Apache Calcite, because they do not have any physical
 * properties.
 * <p>
 * The optimization process is split into two phases - logical and physical. During logical planning we normalize abstract
 * nodes and convert them to nodes with {@link HazelcastConventions#LOGICAL} convention. These new nodes are Hazelcast-specific
 * and hence may have additional properties. For example, at this stage we do filter pushdowns, introduce constrained scans,
 * etc.
 * <p>
 * During physical planning we look for specific physical implementations of logical nodes. Implementation nodes have
 * {@link HazelcastConventions#PHYSICAL} convention. The process contains the following fundamental steps:
 * <ul>
 *     <li>Choosing proper access methods for scan (normal scan, index scan, etc)</li>
 *     <li>Propagating physical properties from children nodes to their parents</li>
 *     <li>Choosing proper implementations of parent operators based on physical properties of children
 *     (local vs. distributed sorting, blocking vs. streaming aggregation, hash join vs. merge join, etc.)</li>
 *     <li>Enforcing exchange operators when data movement is necessary</li>
 * </ul>
 * <p>
 * Physical optimization stage uses {@link VolcanoPlanner}. This is a rule-based optimizer. However it doesn't share any
 * architectural traits with EXODUS/Volcano/Cascades papers, except for the rule-based nature. In classical Cascades algorithm
 * [1], the optimization process is performed in a top-down style. Parent operator may request implementations of children
 * operators with specific properties. This is not possible in {@code VolcanoPlanner}. Instead, in this planner the rules are
 * fired in effectively uncontrollable fashion, thus making propagation of physical properties difficult. To overcome this
 * problem we use several techniques that helps us emulate at least some parts of Cascades-style optimization.
 * <p>
 * First, {@link HazelcastConventions#PHYSICAL} convention overrides {@link Convention#canConvertConvention(Convention)} and
 * {@link Convention#useAbstractConvertersForConversion(RelTraitSet, RelTraitSet)} methods. Their implementations ensure that
 * whenever a new child node with {@code PHYSICAL} convention is created, the rule of the parent {@code LOGICAL} nodes
 * will be re-scheduled. Second, physical rules for {@code LOGICAL} nodes iterate over concrete physical implementations of
 * inputs and convert logical nodes to physical nodes with proper traits. Combined, these techniques ensure complete exploration
 * of a search space and proper propagation of physical properties from child nodes to parent nodes. The downside is that
 * the same rule on the same node could be fired multiple times, thus increase the optimization time.
 * <p>
 * For example, consider the following logical tree:
 * <pre>
 * LogicalFilter
 *   LogicalScan
 * </pre>
 * By default Apache Calcite will fire a rule on the logical filter first. But at this point we do not know the physical
 * properties of {@code LogicalScan} implementations, since they are not produced yet. As a result, we do not know what
 * physical properties should be set to the to-be-created {@code PhysicalFilter}. Then Apache Calcite will optimize
 * {@code LogicalScan}, producing physical implementations. However, by default these new physical implementations will not
 * re-trigger optimization of {@code LogicalFilter}. The result of the optimization will be:
 * <pre>
 * [LogicalFilter, PhysicalFilter(???)]
 *   [LogicalScan, PhysicalScan(PARTITIONED), PhysicalIndexScan(PARTITIONED, a ASC)]
 * </pre>
 * Notice how we failed to propagate important physical properties to the {@code PhysicalFilter}.
 * <p>
 * With the above-described techniques we force Apache Calcite to re-optimize the logical parent after a new physical child
 * has been created. This way we are able to pull-up physical properties. The result of the optimization will be:
 * <pre>
 * [LogicalFilter, PhysicalFilter(PARTITIONED), PhysicalFilter(PARTITIONED, a ASC)]
 *   [LogicalScan, PhysicalScan(PARTITIONED), PhysicalIndexScan(PARTITIONED, a ASC)]
 * </pre>
 * <p>
 * [1] Efficiency In The Columbia Database Query Optimizer (1998), chapters 2 and 3
 */
public class CalciteSqlOptimizer implements SqlOptimizer {

    private final NodeEngine nodeEngine;

    private final SqlBackend sqlBackend;
    private final SqlBackend jetSqlBackend;

    public CalciteSqlOptimizer(
            NodeEngine nodeEngine,
            @Nullable JetSqlCoreBackend jetSqlCoreBackend
    ) {
        this.nodeEngine = nodeEngine;

        this.sqlBackend = new HazelcastSqlBackend(nodeEngine);
        this.jetSqlBackend = jetSqlCoreBackend == null ? null : (SqlBackend) jetSqlCoreBackend.sqlBackend();
    }

    @Override
    public SqlPlan prepare(OptimizationTask task) {
        // 1. Prepare context.
        int memberCount = nodeEngine.getClusterService().getSize(MemberSelectors.DATA_MEMBER_SELECTOR);
        checkJetIsEnabled(nodeEngine);
        OptimizerContext context = OptimizerContext.create(
                task.getSchema(),
                task.getSearchPaths(),
                task.getArguments(),
                memberCount,
                sqlBackend,
                jetSqlBackend
        );

        // 2. Parse SQL string and validate it.
        QueryParseResult parseResult = context.parse(task.getSql());

        // 3. Create plan.
        return parseResult.getSqlBackend().createPlan(task, parseResult, context);
    }
}
