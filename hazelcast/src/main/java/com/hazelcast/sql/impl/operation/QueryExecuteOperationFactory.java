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
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.PlanFragmentMapping;
import com.hazelcast.sql.impl.plan.node.PlanNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.sql.impl.operation.QueryExecuteOperationFragmentMapping.DATA_MEMBERS;
import static com.hazelcast.sql.impl.operation.QueryExecuteOperationFragmentMapping.EXPLICIT;

/**
 * Factory to create query execute operations.
 */
public class QueryExecuteOperationFactory {

    private final Plan plan;
    private final List<Object> args;
    private final Map<Integer, Long> edgeInitialMemoryMap;

    public QueryExecuteOperationFactory(
        Plan plan,
        List<Object> args,
        Map<Integer, Long> edgeInitialMemoryMap
    ) {
        this.plan = plan;
        this.args = args;
        this.edgeInitialMemoryMap = edgeInitialMemoryMap;
    }

    public QueryExecuteOperation create(QueryId queryId, UUID targetMemberId) {
        int fragmentCount = plan.getFragmentCount();

        // Prepare descriptors.
        List<QueryExecuteOperationFragment> fragments = new ArrayList<>(fragmentCount);

        for (int i = 0; i < fragmentCount; i++) {
            PlanFragmentMapping planMapping = plan.getFragmentMapping(i);

            QueryExecuteOperationFragmentMapping mapping;
            Collection<UUID> memberIds;
            PlanNode node;

            if (planMapping.isDataMembers()) {
                mapping = DATA_MEMBERS;
                memberIds = null;
                node = plan.getFragment(i);
            } else {
                mapping = EXPLICIT;
                memberIds = planMapping.getMemberIds();
                node = memberIds.contains(targetMemberId) ? plan.getFragment(i) : null;
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
            args
        );
    }
}
