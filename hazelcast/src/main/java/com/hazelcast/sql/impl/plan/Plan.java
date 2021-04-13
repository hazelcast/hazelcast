/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.optimizer.PlanKey;
import com.hazelcast.sql.impl.optimizer.PlanCheckContext;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.security.SqlSecurityContext;

import java.security.Permission;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Query plan implementation.
 */
public class Plan extends SqlPlan {

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

    /** Result set metadata (columns name and types). */
    private final SqlRowMetadata rowMetadata;

    /** Parameter metadata (number of parameter and their types). */
    private final QueryParameterMetadata parameterMetadata;

    /** IDs of objects used in the plan. */
    private final Set<PlanObjectKey> objectKeys;

    /** Permissions that are required to execute this plan. */
    private final List<Permission> permissions;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public Plan(
        Map<UUID, PartitionIdSet> partMap,
        List<PlanNode> fragments,
        List<PlanFragmentMapping> fragmentMappings,
        Map<Integer, Integer> outboundEdgeMap,
        Map<Integer, Integer> inboundEdgeMap,
        Map<Integer, Integer> inboundEdgeMemberCountMap,
        SqlRowMetadata rowMetadata,
        QueryParameterMetadata parameterMetadata,
        PlanKey planKey,
        Set<PlanObjectKey> objectKeys,
        List<Permission> permissions
    ) {
        super(planKey);

        this.partMap = partMap;
        this.fragments = fragments;
        this.fragmentMappings = fragmentMappings;
        this.outboundEdgeMap = outboundEdgeMap;
        this.inboundEdgeMap = inboundEdgeMap;
        this.inboundEdgeMemberCountMap = inboundEdgeMemberCountMap;
        this.rowMetadata = rowMetadata;
        this.parameterMetadata = parameterMetadata;
        this.objectKeys = objectKeys;
        this.permissions = permissions;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    @Override
    public boolean isPlanValid(PlanCheckContext context) {
        return context.isValid(objectKeys, partMap);
    }

    @Override
    public void checkPermissions(SqlSecurityContext context) {
        for (Permission permission : permissions) {
            context.checkPermission(permission);
        }
    }

    @Override
    public boolean producesRows() {
        return true;
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

    public List<PlanNode> getFragments() {
        return fragments;
    }

    public PlanFragmentMapping getFragmentMapping(int index) {
        return fragmentMappings.get(index);
    }

    public List<PlanFragmentMapping> getFragmentMappings() {
        return fragmentMappings;
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

    public QueryParameterMetadata getParameterMetadata() {
        return parameterMetadata;
    }

    public Set<PlanObjectKey> getObjectKeys() {
        return objectKeys;
    }

    public List<Permission> getPermissions() {
        return permissions;
    }
}
