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

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.calcite.opt.HazelcastConventions;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.logical.LogicalRules;
import com.hazelcast.sql.impl.calcite.opt.logical.RootLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRules;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.NodeIdVisitor;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PlanCreateVisitor;
import com.hazelcast.sql.impl.calcite.parse.QueryConvertResult;
import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTableResolver;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
    private final List<TableResolver> tableResolvers;

    public CalciteSqlOptimizer(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;

        tableResolvers = createTableResolvers(nodeEngine);
    }

    @Override
    public SqlPlan prepare(OptimizationTask task) {
        // 1. Prepare context.
        int memberCount = nodeEngine.getClusterService().getSize(MemberSelectors.DATA_MEMBER_SELECTOR);

        OptimizerContext context = OptimizerContext.create(
            tableResolvers,
            task.getSearchPaths(),
            memberCount
        );

        // 2. Parse SQL string and validate it.
        QueryParseResult parseResult = context.parse(task.getSql());

        // 3. Convert parse tree to relational tree.
        QueryConvertResult convertResult = context.convert(parseResult.getNode());

        // 4. Perform optimization.
        PhysicalRel physicalRel = optimize(context, convertResult.getRel());

        // 5. Create plan.
        return createImdgPlan(parseResult.getParameterRowType(), physicalRel, convertResult.getFieldNames());
    }

    private PhysicalRel optimize(OptimizerContext context, RelNode rel) {
        // Logical part.
        RelNode logicalRel = context.optimize(rel, LogicalRules.getRuleSet(), OptUtils.toLogicalConvention(rel.getTraitSet()));

        RootLogicalRel logicalRootRel = new RootLogicalRel(logicalRel.getCluster(), logicalRel.getTraitSet(), logicalRel);

        // Physical part.
        RelTraitSet physicalTraitSet = OptUtils.toPhysicalConvention(
            logicalRootRel.getTraitSet(),
            OptUtils.getDistributionDef(logicalRootRel).getTraitRoot()
        );

        return (PhysicalRel) context.optimize(logicalRootRel, PhysicalRules.getRuleSet(), physicalTraitSet);
    }

    /**
     * Create plan from physical rel.
     *
     * @param rel Rel.
     * @return Plan.
     */
    private Plan createImdgPlan(RelDataType parameterRowType, PhysicalRel rel, List<String> rootColumnNames) {
        // Get partition mapping.
        Collection<Partition> parts = nodeEngine.getHazelcastInstance().getPartitionService().getPartitions();

        int partCnt = parts.size();

        LinkedHashMap<UUID, PartitionIdSet> partMap = new LinkedHashMap<>();

        for (Partition part : parts) {
            UUID ownerId = part.getOwner().getUuid();

            partMap.computeIfAbsent(ownerId, (key) -> new PartitionIdSet(partCnt)).add(part.getPartitionId());
        }

        // Assign IDs to nodes.
        NodeIdVisitor idVisitor = new NodeIdVisitor();
        rel.visit(idVisitor);
        Map<PhysicalRel, List<Integer>> relIdMap = idVisitor.getIdMap();

        // Create the plan.
        PlanCreateVisitor visitor = new PlanCreateVisitor(
            nodeEngine.getLocalMember().getUuid(),
            partMap,
            relIdMap,
            rootColumnNames
        );

        rel.visit(visitor);

        return visitor.getPlan();
    }

    private static List<TableResolver> createTableResolvers(NodeEngine nodeEngine) {
        List<TableResolver> res = new ArrayList<>(1);

        res.add(new PartitionedMapTableResolver(nodeEngine));

        return res;
    }
}
