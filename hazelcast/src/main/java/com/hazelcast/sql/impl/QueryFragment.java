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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Single query fragment. Represents a node to be executed, it's inbound and outbound edges, members where it is
 * to be executed.
 */
public class QueryFragment implements DataSerializable {
    /** Node to be executed (null for root fragment). */
    private PhysicalNode node;

    /** Outbound edge (if any). */
    private Integer outboundEdge;

    /** Inbound edges (if any). */
    private List<Integer> inboundEdges;

    /** Involved members. */
    private Set<String> memberIds;

    /** Per-member parallelism. */
    private int parallelism;

    public QueryFragment() {
        // No-op.
    }

    public QueryFragment(
        PhysicalNode node,
        Integer outboundEdge,
        List<Integer> inboundEdges,
        Set<String> memberIds,
        int parallelism
    ) {
        this.node = node;
        this.outboundEdge = outboundEdge;
        this.inboundEdges = inboundEdges;
        this.memberIds = memberIds;
        this.parallelism = parallelism;
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

    public Set<String> getMemberIds() {
        return memberIds;
    }

    public int getParallelism() {
        return parallelism;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(node);
        out.writeObject(outboundEdge);
        out.writeObject(inboundEdges);
        out.writeObject(memberIds);
        out.writeInt(parallelism);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        node = in.readObject();
        outboundEdge = in.readObject();
        inboundEdges = in.readObject();
        memberIds = in.readObject();
        parallelism = in.readInt();
    }
}
