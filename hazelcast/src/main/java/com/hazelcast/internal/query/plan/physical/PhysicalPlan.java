package com.hazelcast.internal.query.plan.physical;

import java.util.ArrayList;
import java.util.List;

/**
 * Base physical plan.
 */
public class PhysicalPlan {
    private final List<PhysicalNode> nodes = new ArrayList<>();

    public PhysicalPlan() {
        // No-op.
    }

    public List<PhysicalNode> getNodes() {
        return nodes;
    }

    public void addNode(PhysicalNode node) {
        nodes.add(node);
    }
}
