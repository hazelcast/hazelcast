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

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.SqlToQueryType;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.calcite.opt.logical.LogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.NodeIdVisitor;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PlanCreateVisitor;
import com.hazelcast.sql.impl.calcite.statistics.DefaultStatisticProvider;
import com.hazelcast.sql.impl.calcite.statistics.StatisticProvider;
import com.hazelcast.sql.impl.optimizer.OptimizerRuleCallTracker;
import com.hazelcast.sql.impl.optimizer.OptimizerStatistics;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import com.hazelcast.sql.impl.schema.ChainedSqlSchemaResolver;
import com.hazelcast.sql.impl.schema.PartitionedMapSqlSchemaResolver;
import com.hazelcast.sql.impl.schema.ReplicatedMapSqlSchemaResolver;
import com.hazelcast.sql.impl.schema.SqlSchemaResolver;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

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

    /** Statistics provider. */
    private final StatisticProvider statisticProvider;

    /** Schema resolver. */
    private final SqlSchemaResolver schemaResolver;

    public CalciteSqlOptimizer(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;

        statisticProvider = new DefaultStatisticProvider();

        schemaResolver = new ChainedSqlSchemaResolver(
            new PartitionedMapSqlSchemaResolver((InternalSerializationService) nodeEngine.getSerializationService()),
            new ReplicatedMapSqlSchemaResolver((InternalSerializationService) nodeEngine.getSerializationService())
        );
    }

    @Override
    public Plan prepare(String sql) {
        // 1. Prepare context.
        OptimizerContext context = OptimizerContext.create(nodeEngine, statisticProvider, schemaResolver);

        // 2. Parse SQL string and validate it.
        SqlNode node = context.parse(sql);
        RelDataType parameterRowType = context.getParameterRowType(node);

        // 3. Convert to REL.
        RelNode rel = context.convert(node);

        // 4. Perform logical optimization.
        LogicalRel logicalRel = context.optimizeLogical(rel);

        // 5. Perform physical optimization.
        long start = System.currentTimeMillis();

        boolean statsEnabled = context.getConfig().isStatisticsEnabled();

        OptimizerRuleCallTracker physicalRuleCallTracker = statsEnabled ? new OptimizerRuleCallTracker() : null;
        PhysicalRel physicalRel = context.optimizePhysical(logicalRel, physicalRuleCallTracker);

        // 6. Create plan.
        long dur = System.currentTimeMillis() - start;

        OptimizerStatistics stats = statsEnabled ? new OptimizerStatistics(dur, physicalRuleCallTracker) : null;

        return doCreatePlan(sql, parameterRowType, physicalRel, stats);
    }

    /**
     * Create plan from physical rel.
     *
     * @param rel Rel.
     * @return Plan.
     */
    private Plan doCreatePlan(
        String sql,
        RelDataType parameterRowType,
        PhysicalRel rel,
        OptimizerStatistics stats
    ) {
        // Get partition mapping.
        Collection<Partition> parts = nodeEngine.getHazelcastInstance().getPartitionService().getPartitions();

        int partCnt = parts.size();

        LinkedHashMap<UUID, PartitionIdSet> partMap = new LinkedHashMap<>();

        for (Partition part : parts) {
            UUID ownerId = part.getOwner().getUuid();

            partMap.computeIfAbsent(ownerId, (key) -> new PartitionIdSet(partCnt)).add(part.getPartitionId());
        }

        // Collect remote IDs.
        List<UUID> dataMemberIds = new ArrayList<>(partMap.size());

        for (UUID partMemberId : partMap.keySet()) {
            MemberImpl member = nodeEngine.getClusterService().getMember(partMemberId);

            if (member == null) {
                throw QueryException.memberLeave(partMemberId);
            }

            dataMemberIds.add(partMemberId);
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
            dataMemberIds,
            relIdMap,
            sql,
            parameterMetadata,
            stats
        );

        rel.visit(visitor);

        return visitor.getPlan();
    }
}
