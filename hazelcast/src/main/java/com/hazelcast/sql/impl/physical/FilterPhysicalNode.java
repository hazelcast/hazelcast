package com.hazelcast.sql.impl.physical;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.Expression;

import java.io.IOException;

/**
 * Filter.
 */
public class FilterPhysicalNode implements PhysicalNode {
    /** Upstream node. */
    private PhysicalNode upstream;

    /** Condition. */
    private Expression<Boolean> condition;

    public FilterPhysicalNode() {
        // No-op.
    }

    public FilterPhysicalNode(PhysicalNode upstream, Expression<Boolean> condition) {
        this.upstream = upstream;
        this.condition = condition;
    }

    public PhysicalNode getUpstream() {
        return upstream;
    }

    public Expression<Boolean> getCondition() {
        return condition;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        upstream.visit(visitor);

        visitor.onFilterNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(upstream);
        out.writeObject(condition);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        upstream = in.readObject();
        condition = in.readObject();
    }
}
