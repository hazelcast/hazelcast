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

package com.hazelcast.sql.impl.plan;

import com.hazelcast.internal.util.collection.PartitionIdSet;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Query plan implementation.
 */
public class Plan {
    /** Partition mapping. */
    private final Map<UUID, PartitionIdSet> partMap;

    /** Data member IDs. */
    private final List<UUID> dataMemberIds;

    /** Fragment nodes. */
    private final List<PlanFragment> fragments;

    /** Outbound edge mapping (from edge ID to owning fragment position). */
    private final Map<Integer, Integer> outboundEdgeMap;

    /** Inbound edge mapping (from edge ID to owning fragment position). */
    private final Map<Integer, Integer> inboundEdgeMap;

    /** Map from inbound edge ID to number of members which will write into it. */
    private final Map<Integer, Integer> inboundEdgeMemberCountMap;

    public Plan(
        Map<UUID, PartitionIdSet> partMap,
        List<UUID> dataMemberIds,
        List<PlanFragment> fragments,
        Map<Integer, Integer> outboundEdgeMap,
        Map<Integer, Integer> inboundEdgeMap,
        Map<Integer, Integer> inboundEdgeMemberCountMap
    ) {
        this.partMap = partMap;
        this.dataMemberIds = dataMemberIds;
        this.fragments = fragments;
        this.outboundEdgeMap = outboundEdgeMap;
        this.inboundEdgeMap = inboundEdgeMap;
        this.inboundEdgeMemberCountMap = inboundEdgeMemberCountMap;
    }

    public Map<UUID, PartitionIdSet> getPartitionMap() {
        return partMap;
    }

    public List<UUID> getDataMemberIds() {
        return dataMemberIds;
    }

    public List<PlanFragment> getFragments() {
        return fragments;
    }

    public Map<Integer, Integer> getOutboundEdgeMap() {
        return outboundEdgeMap;
    }

    public Map<Integer, Integer> getInboundEdgeMap() {
        return inboundEdgeMap;
    }

    public Map<Integer, Integer> getInboundEdgeMemberCountMap() {
        return inboundEdgeMemberCountMap;
    }
}
