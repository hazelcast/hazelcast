package com.hazelcast.sql.impl.physical;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.expression.Expression;

import java.io.IOException;
import java.util.List;

/**
 * Projection.
 */
public class ProjectPhysicalNode implements PhysicalNode {
    /** Upstream node. */
    private PhysicalNode upstream;

    /** Projections. */
    private List<Expression> projections;

    public ProjectPhysicalNode() {
        // No-op.
    }

    public ProjectPhysicalNode(PhysicalNode upstream, List<Expression> projections) {
        this.upstream = upstream;
        this.projections = projections;
    }

    public PhysicalNode getUpstream() {
        return upstream;
    }

    public List<Expression> getProjections() {
        return projections;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        upstream.visit(visitor);

        visitor.onProjectNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(upstream);
        out.writeObject(projections);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        upstream = in.readObject();
        projections = in.readObject();
    }
}
