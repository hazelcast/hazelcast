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
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.physical.PhysicalNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Factory to create query execute operations.
 */
public class QueryExecuteOperationFactory {
    /** Service. */
    private final SqlServiceImpl service;

    /** Query plan. */
    private final QueryPlan plan;

    /** Arguments. */
    private final List<Object> args;

    /** Query ID. */
    private final QueryId queryId;

    /** Local member ID. */
    private final UUID localMemberId;

    /** Timeout. */
    private final long timeout;

    public QueryExecuteOperationFactory(
        SqlServiceImpl service,
        QueryPlan plan,
        List<Object> args,
        QueryId queryId,
        UUID localMemberId,
        long timeout
    ) {
        this.service = service;
        this.plan = plan;
        this.args = args;
        this.queryId = queryId;
        this.localMemberId = localMemberId;
        this.timeout = timeout;
    }

    /**
     * Create query execute operation for the given member.
     *
     * @param targetMemberId Target member ID.
     * @return Operation.
     */
    public QueryExecuteOperation create(UUID targetMemberId) {
        List<QueryFragment> fragments = plan.getFragments();

        List<QueryFragmentDescriptor> descriptors = new ArrayList<>(fragments.size());

        for (QueryFragment fragment : fragments) {
            QueryFragmentMapping mapping = fragment.getMapping();

            PhysicalNode node;
            List<UUID> mappedMemberIds;

            switch (mapping) {
                case ROOT:
                    // Fragment is only deployed on local node.
                    node = targetMemberId.equals(localMemberId) ? fragment.getNode() : null;
                    mappedMemberIds = Collections.singletonList(localMemberId);

                    break;

                case DATA_MEMBERS:
                    assert mapping == QueryFragmentMapping.DATA_MEMBERS;

                    // Fragment is only deployed on data node. Member IDs will be derived from partition mapping.
                    node = fragment.getNode();
                    mappedMemberIds = null;

                    break;

                default:
                    throw new IllegalArgumentException("Unsupported mappimg: " + mapping);
            }

            descriptors.add(new QueryFragmentDescriptor(node, mappedMemberIds));
        }

        return new QueryExecuteOperation(
            service.getEpochWatermark(), queryId,
            plan.getPartitionMap(),
            descriptors,
            plan.getOutboundEdgeMap(),
            plan.getInboundEdgeMap(),
            args,
            timeout
        );
    }
}
