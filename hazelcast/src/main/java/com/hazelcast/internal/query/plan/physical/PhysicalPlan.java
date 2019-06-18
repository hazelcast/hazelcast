package com.hazelcast.internal.query.plan.physical;

import java.util.List;

/**
 * Base physical plan.
 */
public class PhysicalPlan {
    private List<PhysicalNode> nodes;

    public PhysicalPlan() {
        // No-op.
    }

    public PhysicalPlan(List<PhysicalNode> nodes) {
        this.nodes = nodes;
    }

    public List<PhysicalNode> getNodes() {
        return nodes;
    }
}
