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

package com.hazelcast.sql.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.util.collection.PartitionIdSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Query plan implementation.
 */
public class QueryPlan {
    /** Partition mapping. */
    private final Map<UUID, PartitionIdSet> partMap;

    /** Data member IDs. */
    private final List<UUID> dataMemberIds;

    /** Data member addresses. */
    private final List<Address> dataMemberAddresses;

    /** Fragment nodes. */
    private final List<QueryFragment> fragments;

    /** Outbound edge mapping (from edge ID to owning fragment position). */
    private final Map<Integer, Integer> outboundEdgeMap;

    /** Inbound edge mapping (from edge ID to owning fragment position). */
    private final Map<Integer, Integer> inboundEdgeMap;

    /** Number of parameters. */
    private final int parameterCount;

    /** Explain. */
    private final QueryExplain explain;

    /** Optimizer statistics. */
    private final OptimizerStatistics stats;

    /** Optional attachments. */
    private final Collection<Object> attachments;

    public QueryPlan(
        Map<UUID, PartitionIdSet> partMap,
        List<UUID> dataMemberIds,
        List<Address> dataMemberAddresses,
        List<QueryFragment> fragments,
        Map<Integer, Integer> outboundEdgeMap,
        Map<Integer, Integer> inboundEdgeMap,
        int parameterCount,
        QueryExplain explain,
        OptimizerStatistics stats,
        Collection<Object> attachments
    ) {
        this.partMap = partMap;
        this.dataMemberIds = dataMemberIds;
        this.dataMemberAddresses = dataMemberAddresses;
        this.fragments = fragments;
        this.outboundEdgeMap = outboundEdgeMap;
        this.inboundEdgeMap = inboundEdgeMap;
        this.parameterCount = parameterCount;
        this.explain = explain;
        this.stats = stats;
        this.attachments = attachments;
    }

    public Map<UUID, PartitionIdSet> getPartitionMap() {
        return partMap;
    }

    public List<UUID> getDataMemberIds() {
        return dataMemberIds;
    }

    public List<Address> getDataMemberAddresses() {
        return dataMemberAddresses;
    }

    public List<QueryFragment> getFragments() {
        return fragments;
    }

    public Map<Integer, Integer> getOutboundEdgeMap() {
        return outboundEdgeMap;
    }

    public Map<Integer, Integer> getInboundEdgeMap() {
        return inboundEdgeMap;
    }

    public int getParameterCount() {
        return parameterCount;
    }

    public QueryExplain getExplain() {
        return explain;
    }

    public OptimizerStatistics getStatistics() {
        return stats;
    }

    @SuppressWarnings("unchecked")
    public <T> T getAttachment(Class<T> cls) {
        if (attachments != null) {
            for (Object attachment : attachments) {
                if (attachment != null && cls.isAssignableFrom(attachment.getClass())) {
                    return (T) attachment;
                }
            }
        }

        return null;
    }
}
