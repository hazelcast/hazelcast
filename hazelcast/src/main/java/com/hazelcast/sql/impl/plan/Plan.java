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
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.explain.QueryExplain;
import com.hazelcast.sql.impl.SqlCacheablePlan;
import com.hazelcast.sql.impl.optimizer.SqlPlanType;
import com.hazelcast.sql.impl.plan.node.PlanNode;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Query plan implementation.
 */
public class Plan implements SqlCacheablePlan {

    private static final AtomicLong PLAN_TIMESTAMP_GENERATOR = new AtomicLong();

    /** Unique plan timestamp that used to compare older and newer plans.  */
    private final long planTimestamp = PLAN_TIMESTAMP_GENERATOR.incrementAndGet();

    /** Time when the plan was used for the last time. */
    private volatile long planLastUsed;

    private final String planSql;

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

    private final QueryParameterMetadata parameterMetadata;
    private final SqlRowMetadata rowMetadata;
    private final QueryExplain explain;

    public Plan(
        Map<UUID, PartitionIdSet> partMap,
        List<PlanNode> fragments,
        List<PlanFragmentMapping> fragmentMappings,
        Map<Integer, Integer> outboundEdgeMap,
        Map<Integer, Integer> inboundEdgeMap,
        Map<Integer, Integer> inboundEdgeMemberCountMap,
        QueryParameterMetadata parameterMetadata,
        SqlRowMetadata rowMetadata,
        String planSql,
        QueryExplain explain
    ) {
        this.partMap = partMap;
        this.fragments = fragments;
        this.fragmentMappings = fragmentMappings;
        this.outboundEdgeMap = outboundEdgeMap;
        this.inboundEdgeMap = inboundEdgeMap;
        this.inboundEdgeMemberCountMap = inboundEdgeMemberCountMap;
        this.parameterMetadata = parameterMetadata;
        this.rowMetadata = rowMetadata;
        this.planSql = planSql;
        this.explain = explain;
    }

    @Override
    public SqlPlanType getType() {
        return SqlPlanType.IMDG;
    }

    @Override
    public QueryExplain getExplain() {
        return explain;
    }

    @Override
    public String getPlanSql() {
        return planSql;
    }

    @Override
    public long getPlanCreationTimestamp() {
        return planTimestamp;
    }

    @Override
    public long getPlanLastUsed() {
        return planLastUsed;
    }

    @Override
    public void onPlanUsed() {
        planLastUsed = System.currentTimeMillis();
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

    public QueryParameterMetadata getParameterMetadata() {
        return parameterMetadata;
    }

    public SqlRowMetadata getRowMetadata() {
        return rowMetadata;
    }
}
