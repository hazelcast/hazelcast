package com.hazelcast.internal.query.plan.physical;

import com.hazelcast.nio.serialization.DataSerializable;

public interface PhysicalNode extends DataSerializable {
    /**
     * Visit the node.
     *
     * @param visitor Visitor.
     */
    void visit(PhysicalNodeVisitor visitor);
}
