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

import com.hazelcast.sql.impl.QueryFragment;
import com.hazelcast.sql.impl.QueryFragmentDescriptor;
import com.hazelcast.sql.impl.QueryFragmentMapping;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryPlan;
import com.hazelcast.sql.impl.memory.MemoryPressure;
import com.hazelcast.sql.impl.physical.PhysicalNode;

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

    private final QueryPlan plan;
    private final List<Object> args;
    private final long timeout;
    private final MemoryPressure memoryPressure;
    private final long epochWatermark;

    public QueryExecuteOperationFactory(
        QueryPlan plan,
        List<Object> args,
        long timeout,
        MemoryPressure memoryPressure,
        long epochWatermark
    ) {
        this.plan = plan;
        this.args = args;
        this.timeout = timeout;
        this.memoryPressure = memoryPressure;
        this.epochWatermark = epochWatermark;
    }

    public IdentityHashMap<QueryFragment, Collection<UUID>> prepareFragmentMappings() {
        IdentityHashMap<QueryFragment, Collection<UUID>> mappings = new IdentityHashMap<>();

        for (QueryFragment fragment : plan.getFragments()) {
            QueryFragmentMapping mapping = fragment.getMapping();

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
        IdentityHashMap<QueryFragment, Collection<UUID>> fragmentMappings,
        UUID targetMemberId
    ) {
        List<QueryFragment> fragments = plan.getFragments();

        // Prepare descriptors.
        List<QueryFragmentDescriptor> descriptors = new ArrayList<>(fragments.size());

        for (QueryFragment fragment : fragments) {
            Collection<UUID> mappedMemberIds = fragmentMappings.get(fragment);
            PhysicalNode node = mappedMemberIds.contains(targetMemberId) ? fragment.getNode() : null;

            descriptors.add(new QueryFragmentDescriptor(node, mappedMemberIds));
        }

        // Prepare initial edge credits.
        Map<Integer, Integer> inboundEdgeMemberCountMap = plan.getInboundEdgeMemberCountMap();

        Map<Integer, Long> edgeCreditMap = new HashMap<>(inboundEdgeMemberCountMap.size());

        for (Map.Entry<Integer, Integer> entry : inboundEdgeMemberCountMap.entrySet()) {
            edgeCreditMap.put(entry.getKey(), getCredit(entry.getValue()));
        }

        return new QueryExecuteOperation(
            epochWatermark,
            queryId,
            plan.getPartitionMap(),
            descriptors,
            plan.getOutboundEdgeMap(),
            plan.getInboundEdgeMap(),
            edgeCreditMap,
            args,
            timeout
        );
    }

    // TODO: This is a very dumb heuristic based on presumed memory pressure and particpating member count
    private long getCredit(int memberCount) {
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
