/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.HazelcastSqlTransientException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryPlan;
import com.hazelcast.sql.impl.SqlOptimizer;
import com.hazelcast.sql.impl.calcite.logical.rel.LogicalRel;
import com.hazelcast.sql.impl.calcite.physical.rel.PhysicalRel;
import com.hazelcast.sql.impl.calcite.statistics.DefaultStatisticProvider;
import com.hazelcast.sql.impl.calcite.statistics.StatisticProvider;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
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

    public CalciteSqlOptimizer(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;

        statisticProvider = new DefaultStatisticProvider();
    }

    @Override
    public QueryPlan prepare(String sql) {
        // 1. Prepare context.
        OptimizerContext context = OptimizerContext.create(nodeEngine, statisticProvider);

        // 2. Parse SQL string and validate it.
        SqlNode node = context.parse(sql);

        // 3. Convert to REL.
        RelNode rel = context.convert(node);

        // 4. Perform logical optimization.
        LogicalRel logicalRel = context.optimizeLogical(rel);

        // 5. Perform physical optimization.
        PhysicalRel physicalRel = context.optimizePhysical(logicalRel);

        // 6. Create plan.
        return doCreatePlan(physicalRel);
    }

    /**
     * Create plan from physical rel.
     *
     * @param rel Rel.
     * @return Plan.
     */
    private QueryPlan doCreatePlan(PhysicalRel rel) {
        // Get partition mapping.
        Collection<Partition> parts = nodeEngine.getHazelcastInstance().getPartitionService().getPartitions();

        int partCnt = parts.size();

        LinkedHashMap<UUID, PartitionIdSet> partMap = new LinkedHashMap<>();

        for (Partition part : parts) {
            UUID ownerId = part.getOwner().getUuid();

            partMap.computeIfAbsent(ownerId, (key) -> new PartitionIdSet(partCnt)).add(part.getPartitionId());
        }

        // Collect remote addresses.
        List<Address> dataMemberAddresses = new ArrayList<>(partMap.size());
        List<UUID> dataMemberIds = new ArrayList<>(partMap.size());

        for (UUID partMemberId : partMap.keySet()) {
            MemberImpl member = nodeEngine.getClusterService().getMember(partMemberId);

            if (member == null) {
                throw new HazelcastSqlTransientException(SqlErrorCode.MEMBER_LEAVE, "Participating member has "
                    + "left the topology: " + partMemberId);
            }

            dataMemberAddresses.add(member.getAddress());
            dataMemberIds.add(member.getUuid());
        }

        // Create the plan.
        PlanCreatePhysicalRelVisitor visitor = new PlanCreatePhysicalRelVisitor(partMap, dataMemberIds, dataMemberAddresses);

        rel.visit(visitor);

        return visitor.getPlan();
    }
}
