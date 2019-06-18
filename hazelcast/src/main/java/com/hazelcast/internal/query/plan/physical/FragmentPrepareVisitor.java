package com.hazelcast.internal.query.plan.physical;

import java.util.ArrayList;
import java.util.List;

public class FragmentPrepareVisitor extends AbstractPhysicalNodeVisitor {

    private Integer outboundEdge;
    private List<Integer> inboundEdges;
    private int parallelism;

    public Integer getOutboundEdge() {
        return outboundEdge;
    }

    public List<Integer> getInboundEdges() {
        return inboundEdges;
    }

    public int getParallelism() {
        return parallelism;
    }

    @Override
    public void onReceiveNode(ReceivePhysicalNode node) {
        if (inboundEdges == null)
            inboundEdges = new ArrayList<>(1);

        inboundEdges.add(node.getEdgeId());

        parallelism = node.getParallelism();
    }

    @Override
    public void onSendNode(SendPhysicalNode node) {
        outboundEdge = node.getEdgeId();
    }

    @Override
    public void onMapScanNode(MapScanPhysicalNode node) {
        // TODO: Careful with parallelism override. This should not be the case for a valid plan.
        parallelism = node.getParallelism();
    }
}
