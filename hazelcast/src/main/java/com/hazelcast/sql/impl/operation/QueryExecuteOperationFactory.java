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

import com.hazelcast.sql.impl.plan.PlanFragment;
import com.hazelcast.sql.impl.plan.PlanFragmentMapping;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.memory.MemoryPressure;
import com.hazelcast.sql.impl.plan.node.PlanNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

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
    private final Map<Integer, Long> creditMap;

    public QueryExecuteOperationFactory(
        Plan plan,
        List<Object> args,
        long timeout,
        MemoryPressure memoryPressure
    ) {
        this.plan = plan;
        this.args = args;
        this.timeout = timeout;

        creditMap = createCreditMap(memoryPressure);
    }

    public IdentityHashMap<PlanFragment, Collection<UUID>> prepareFragmentMappings() {
        IdentityHashMap<PlanFragment, Collection<UUID>> mappings = new IdentityHashMap<>();

        for (PlanFragment fragment : plan.getFragments()) {
            PlanFragmentMapping mapping = fragment.getMapping();

            if (mapping.isStatic()) {
                mappings.put(fragment, mapping.getStaticMemberIds());
            } else {
                List<UUID> dataMemberIds = plan.getDataMemberIds();
                Set<UUID> memberIds = new HashSet<>(mapping.getDynamicMemberCount());

                int start = ThreadLocalRandom.current().nextInt(dataMemberIds.size());

                for (int i = 0; i < mapping.getDynamicMemberCount(); i++) {
                    int index = (start + i) % dataMemberIds.size();

                    memberIds.add(dataMemberIds.get(index));
                }

                mappings.put(fragment, memberIds);
            }

        }

        return mappings;
    }

    public QueryExecuteOperation create(
        QueryId queryId,
        IdentityHashMap<PlanFragment, Collection<UUID>> fragmentMappings,
        UUID targetMemberId
    ) {
        List<PlanFragment> fragments = plan.getFragments();

        // Prepare descriptors.
        List<QueryExecuteOperationFragment> descriptors = new ArrayList<>(fragments.size());

        for (PlanFragment fragment : fragments) {
            Collection<UUID> fragmentMemberIds = fragmentMappings.get(fragment);

            // Do not send node to a member which will not execute it.
            PlanNode node = fragmentMemberIds.contains(targetMemberId) ? fragment.getNode() : null;

            descriptors.add(new QueryExecuteOperationFragment(node, fragmentMemberIds));
        }

        return new QueryExecuteOperation(
            queryId,
            plan.getPartitionMap(),
            descriptors,
            plan.getOutboundEdgeMap(),
            plan.getInboundEdgeMap(),
            creditMap,
            args,
            timeout
        );
    }

    private Map<Integer, Long> createCreditMap(MemoryPressure memoryPressure) {
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
