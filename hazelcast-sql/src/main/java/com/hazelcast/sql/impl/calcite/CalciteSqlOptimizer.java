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
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.opt.logical.LogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.NodeIdVisitor;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PlanCreateVisitor;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTableResolver;
import com.hazelcast.sql.impl.schema.map.ReplicatedMapTableResolver;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Calcite-based SQL optimizer.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class CalciteSqlOptimizer implements SqlOptimizer {
    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Table resolvers used for schema resolution. */
    private final List<TableResolver> tableResolvers;

    public CalciteSqlOptimizer(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;

        tableResolvers = createTableResolvers(nodeEngine);
    }

    @Override
    public Plan prepare(OptimizationTask task) {
        // 1. Prepare context.
        int memberCount = nodeEngine.getClusterService().getSize(MemberSelectors.DATA_MEMBER_SELECTOR);

        OptimizerContext context = OptimizerContext.create(
            tableResolvers,
            task.getSearchPaths(),
            memberCount
        );

        // 2. Parse SQL string and validate it.
        QueryParseResult parseResult = context.parse(task.getSql());

        // 3. Convert to REL.
        RelNode rel = context.convert(parseResult.getNode());

        // 4. Perform logical optimization.
        LogicalRel logicalRel = context.optimizeLogical(rel);

        // 5. Perform physical optimization.
        PhysicalRel physicalRel = context.optimizePhysical(logicalRel);

        // 6. Create plan.
        return doCreatePlan(task.getSql(), parseResult.getParameterRowType(), physicalRel);
    }

    /**
     * Create plan from physical rel.
     *
     * @param rel Rel.
     * @return Plan.
     */
    private Plan doCreatePlan(String sql, RelDataType parameterRowType, PhysicalRel rel) {
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
        QueryDataType[] mappedParameterRowType = SqlToQueryType.mapRowType(parameterRowType);
        QueryParameterMetadata parameterMetadata = new QueryParameterMetadata(mappedParameterRowType);

        PlanCreateVisitor visitor = new PlanCreateVisitor(
            nodeEngine.getLocalMember().getUuid(),
            partMap,
            relIdMap,
            sql,
            parameterMetadata
        );

        rel.visit(visitor);

        return visitor.getPlan();
    }

    private static List<TableResolver> createTableResolvers(NodeEngine nodeEngine) {
        List<TableResolver> res = new ArrayList<>(2);

        res.add(new PartitionedMapTableResolver(nodeEngine));
        res.add(new ReplicatedMapTableResolver(nodeEngine));

        // TODO: Add Jet resolvers

        return res;
    }
}
