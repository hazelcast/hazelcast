package com.hazelcast.internal.query;

import com.hazelcast.internal.query.physical.PhysicalNode;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

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

    @Override
    public String toString() {
        // TODO: Proper toString
        return "QueryFragment{node=" + node.getClass().getSimpleName() + ", memberIds=" + memberIds + "}";
    }
}
