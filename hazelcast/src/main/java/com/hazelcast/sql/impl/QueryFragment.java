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

import com.hazelcast.sql.impl.physical.PhysicalNode;

import java.util.Collections;
import java.util.List;

/**
 * Single query fragment. Represents a node to be executed, it's inbound and outbound edges, members where it is
 * to be executed.
 */
public class QueryFragment {
    /** Node to be executed (null for root fragment). */
    private final PhysicalNode node;

    /** Outbound edge (if any). */
    private final Integer outboundEdge;

    /** Inbound edges (if any). */
    private final List<Integer> inboundEdges;

    /** Mapping. */
    private final QueryFragmentMapping mapping;

    public QueryFragment(
        PhysicalNode node,
        Integer outboundEdge,
        List<Integer> inboundEdges,
        QueryFragmentMapping mapping
    ) {
        this.node = node;
        this.outboundEdge = outboundEdge;
        this.inboundEdges = inboundEdges;
        this.mapping = mapping;
    }

    public PhysicalNode getNode() {
        return node;
    }

    public Integer getOutboundEdge() {
        return outboundEdge;
    }

    public List<Integer> getInboundEdges() {
        return inboundEdges != null ? inboundEdges : Collections.emptyList();
    }

    public QueryFragmentMapping getMapping() {
        return mapping;
    }
}
