package com.hazelcast.internal.query.plan.physical;

import com.hazelcast.internal.query.exec.Exec;
import com.hazelcast.internal.query.expression.Expression;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.List;

public class SortPhysicalNode implements PhysicalNode {

    private PhysicalNode upstream;
    private List<Expression> expressions;
    private List<Boolean> ascs;

    public SortPhysicalNode() {
        // No-op.
    }

    public SortPhysicalNode(PhysicalNode upstream, List<Expression> expressions, List<Boolean> ascs) {
        this.upstream = upstream;
        this.expressions = expressions;
        this.ascs = ascs;
    }

    public PhysicalNode getUpstream() {
        return upstream;
    }

    public List<Expression> getExpressions() {
        return expressions;
    }

    public List<Boolean> getAscs() {
        return ascs;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        upstream.visit(visitor);

        visitor.onSortNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(upstream);
        out.writeObject(expressions);
        out.writeObject(ascs);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        upstream = in.readObject();
        expressions = in.readObject();
        ascs = in.readObject();
    }
}
