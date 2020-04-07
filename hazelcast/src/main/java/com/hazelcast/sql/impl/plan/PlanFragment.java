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

import com.hazelcast.sql.impl.plan.node.PlanNode;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Single query fragment. Represents a node to be executed, its inbound and outbound edges, members where it is
 * to be executed.
 */
public class PlanFragment {
    /** Node to be executed (null for root fragment). */
    private final PlanNode node;

    /** Outbound edge (if any). */
    private final Integer outboundEdge;

    /** Inbound edges (if any). */
    private final List<Integer> inboundEdges;

    /** Mapping. */
    private final PlanFragmentMapping mapping;

    public PlanFragment(
        PlanNode node,
        Integer outboundEdge,
        List<Integer> inboundEdges,
        PlanFragmentMapping mapping
    ) {
        this.node = node;
        this.outboundEdge = outboundEdge;
        this.inboundEdges = inboundEdges;
        this.mapping = mapping;
    }

    public PlanNode getNode() {
        return node;
    }

    public Integer getOutboundEdge() {
        return outboundEdge;
    }

    public List<Integer> getInboundEdges() {
        return inboundEdges != null ? inboundEdges : Collections.emptyList();
    }

    public PlanFragmentMapping getMapping() {
        return mapping;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PlanFragment fragment = (PlanFragment) o;

        return Objects.equals(node, fragment.node)
            && Objects.equals(outboundEdge, fragment.outboundEdge)
            && Objects.equals(inboundEdges, fragment.inboundEdges)
            && mapping == fragment.mapping;
    }

    @Override
    public int hashCode() {
        return Objects.hash(node, outboundEdge, inboundEdges, mapping);
    }

    @Override
    public String toString() {
        return "QueryFragment{node=" + node + ", outboundEdge=" + outboundEdge + ", inboundEdges=" + inboundEdges
            + ", mapping=" + mapping + '}';
    }
}
