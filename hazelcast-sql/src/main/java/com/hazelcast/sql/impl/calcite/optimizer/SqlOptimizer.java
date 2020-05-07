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

package com.hazelcast.sql.impl.calcite.optimizer;

import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.ExecutionContext;
import com.hazelcast.sql.impl.calcite.opt.logical.LogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.NodeIdVisitor;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PlanCreateVisitor;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SqlOptimizer {
    /** Node engine. */
    private final NodeEngine nodeEngine;

    public SqlOptimizer(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public Plan optimize(QueryParseResult parseResult, ExecutionContext context) {
        // 1. Convert to REL.
        RelNode rel = context.convert(parseResult.getNode());

        // 2. Perform logical optimization.
        LogicalRel logicalRel = context.optimizeLogical(rel);

        // 3. Perform physical optimization.
        PhysicalRel physicalRel = context.optimizePhysical(logicalRel);

        // 4. Create plan.
        return doCreatePlan(parseResult.getSql(), parseResult.getParameterRowType(), physicalRel);
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
}
