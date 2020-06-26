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
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.optimizer.SqlPlanType;
import com.hazelcast.sql.impl.plan.node.PlanNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Query plan implementation.
 */
public class Plan implements SqlPlan {
    /** Partition mapping. */
    private final Map<UUID, PartitionIdSet> partMap;

    /** Fragment nodes. */
    private final List<PlanNode> fragments;

    /** Fragment mapping. */
    private final List<PlanFragmentMapping> fragmentMappings;

    /** Outbound edge mapping (from edge ID to owning fragment position). */
    private final Map<Integer, Integer> outboundEdgeMap;

    /** Inbound edge mapping (from edge ID to owning fragment position). */
    private final Map<Integer, Integer> inboundEdgeMap;

    /** Map from inbound edge ID to number of members which will write into it. */
    private final Map<Integer, Integer> inboundEdgeMemberCountMap;

    private final SqlRowMetadata rowMetadata;

    public Plan(
        Map<UUID, PartitionIdSet> partMap,
        List<PlanNode> fragments,
        List<PlanFragmentMapping> fragmentMappings,
        Map<Integer, Integer> outboundEdgeMap,
        Map<Integer, Integer> inboundEdgeMap,
        Map<Integer, Integer> inboundEdgeMemberCountMap,
        SqlRowMetadata rowMetadata
    ) {
        this.partMap = partMap;
        this.fragments = fragments;
        this.fragmentMappings = fragmentMappings;
        this.outboundEdgeMap = outboundEdgeMap;
        this.inboundEdgeMap = inboundEdgeMap;
        this.inboundEdgeMemberCountMap = inboundEdgeMemberCountMap;
        this.rowMetadata = rowMetadata;
    }

    @Override
    public SqlPlanType getType() {
        return SqlPlanType.IMDG;
    }

    public Map<UUID, PartitionIdSet> getPartitionMap() {
        return partMap;
    }

    public Collection<UUID> getMemberIds() {
        return partMap.keySet();
    }

    public int getFragmentCount() {
        return fragments.size();
    }

    public PlanNode getFragment(int index) {
        return fragments.get(index);
    }

    public PlanFragmentMapping getFragmentMapping(int index) {
        return fragmentMappings.get(index);
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

    public SqlRowMetadata getRowMetadata() {
        return rowMetadata;
    }
}
