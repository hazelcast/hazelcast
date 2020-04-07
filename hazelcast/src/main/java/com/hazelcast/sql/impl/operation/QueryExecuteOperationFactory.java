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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.memory.MemoryPressure;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.PlanFragment;
import com.hazelcast.sql.impl.plan.node.PlanNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.sql.impl.operation.QueryExecuteOperationFragmentMapping.DATA_MEMBERS;
import static com.hazelcast.sql.impl.operation.QueryExecuteOperationFragmentMapping.EXPLICIT;

/**
 * Factory to create query execute operations.
 */
public class QueryExecuteOperationFactory {
    private static final int SMALL_TOPOLOGY_THRESHOLD = 8;
    private static final int MEDIUM_TOPOLOGY_THRESHOLD = 16;

    private static final int LOW_PRESSURE_CREDIT = 1024 * 1024;
    private static final int MEDIUM_PRESSURE_CREDIT = 512 * 1024;
    private static final int HIGH_PRESSURE_CREDIT = 256 * 1024;

    private final Plan plan;
    private final List<Object> args;
    private final long timeout;
    private final Map<Integer, Long> edgeInitialMemoryMap;

    public QueryExecuteOperationFactory(
        Plan plan,
        List<Object> args,
        long timeout,
        MemoryPressure memoryPressure
    ) {
        this.plan = plan;
        this.args = args;
        this.timeout = timeout;

        edgeInitialMemoryMap = createEdgeInitialMemoryMap(memoryPressure);
    }

    public QueryExecuteOperation create(QueryId queryId, UUID targetMemberId) {
        List<PlanFragment> planFragments = plan.getFragments();

        // Prepare descriptors.
        List<QueryExecuteOperationFragment> fragments = new ArrayList<>(planFragments.size());

        for (PlanFragment fragment : planFragments) {
            QueryExecuteOperationFragmentMapping mapping;
            Collection<UUID> memberIds;
            PlanNode node;

            if (fragment.getMapping().isDataMembers()) {
                mapping = DATA_MEMBERS;
                memberIds = null;
                node = fragment.getNode();
            } else {
                mapping = EXPLICIT;
                memberIds = fragment.getMapping().getMemberIds();
                node = memberIds.contains(targetMemberId) ? fragment.getNode() : null;
            }

            fragments.add(new QueryExecuteOperationFragment(node, mapping, memberIds));
        }

        return new QueryExecuteOperation(
            queryId,
            plan.getPartitionMap(),
            fragments,
            plan.getOutboundEdgeMap(),
            plan.getInboundEdgeMap(),
            edgeInitialMemoryMap,
            args,
            timeout
        );
    }

    private Map<Integer, Long> createEdgeInitialMemoryMap(MemoryPressure memoryPressure) {
        Map<Integer, Integer> inboundEdgeMemberCountMap = plan.getInboundEdgeMemberCountMap();

        Map<Integer, Long> res = new HashMap<>(inboundEdgeMemberCountMap.size());

        for (Map.Entry<Integer, Integer> entry : inboundEdgeMemberCountMap.entrySet()) {
            res.put(entry.getKey(), getCredit(memoryPressure, entry.getValue()));
        }

        return res;
    }

    // TODO: This is a very dumb heuristic based on presumed memory pressure and particpating member count
    private static long getCredit(MemoryPressure memoryPressure, int memberCount) {
        MemoryPressure memoryPressure0;

        if (memberCount <= SMALL_TOPOLOGY_THRESHOLD) {
            // Small topology. Do not adjust memory pressure.
            memoryPressure0 = memoryPressure;
        } else if (memberCount <= MEDIUM_TOPOLOGY_THRESHOLD) {
            // Medium topology. Treat LOW as MEDIUM.
            memoryPressure0 = memoryPressure == MemoryPressure.LOW ? MemoryPressure.MEDIUM : memoryPressure;
        } else {
            // Large topology. Tread everything as HIGH.
            memoryPressure0 = MemoryPressure.HIGH;
        }

        switch (memoryPressure0) {
            case LOW:
                // 1Mb
                return LOW_PRESSURE_CREDIT;

            case MEDIUM:
                // 512Kb
                return MEDIUM_PRESSURE_CREDIT;

            case HIGH:
                // 256Kb
                return HIGH_PRESSURE_CREDIT;

            default:
                throw new IllegalStateException("Invalid memory pressure: " + memoryPressure0);
        }
    }
}
