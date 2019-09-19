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

import com.hazelcast.nio.Address;
import com.hazelcast.util.collection.PartitionIdSet;

import java.util.List;
import java.util.Map;

/**
 * Prepared query plan.
 */
public class QueryPlan {
    /** Partition mapping. */
    private final Map<String, PartitionIdSet> partMap;

    /** Data member IDs. */
    private final List<String> dataMemberIds;

    /** Data member addresses. */
    private final List<Address> dataMemberAddresses;

    /** Fragment nodes. */
    private final List<QueryFragment> fragments;

    /** Outbound edge mapping (from edge ID to owning fragment position). */
    private final Map<Integer, Integer> outboundEdgeMap;

    /** Inbound edge mapping (from edge ID to owning fragment position). */
    private final Map<Integer, Integer> inboundEdgeMap;

    public QueryPlan(
        Map<String, PartitionIdSet> partMap,
        List<String> dataMemberIds,
        List<Address> dataMemberAddresses,
        List<QueryFragment> fragments,
        Map<Integer, Integer> outboundEdgeMap,
        Map<Integer, Integer> inboundEdgeMap
    ) {
        this.partMap = partMap;
        this.dataMemberIds = dataMemberIds;
        this.dataMemberAddresses = dataMemberAddresses;
        this.fragments = fragments;
        this.outboundEdgeMap = outboundEdgeMap;
        this.inboundEdgeMap = inboundEdgeMap;
    }

    public Map<String, PartitionIdSet> getPartitionMap() {
        return partMap;
    }

    public List<String> getDataMemberIds() {
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
}
