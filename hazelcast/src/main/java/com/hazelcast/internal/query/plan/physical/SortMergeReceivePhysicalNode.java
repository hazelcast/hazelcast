package com.hazelcast.internal.query.plan.physical;

import com.hazelcast.internal.query.expression.Expression;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.List;

public class SortMergeReceivePhysicalNode implements PhysicalNode {

    private List<Expression> expressions;
    private List<Boolean> ascs;
    private int parallelism;
    private int edgeId;

    public SortMergeReceivePhysicalNode() {
        // No-op.
    }

    public SortMergeReceivePhysicalNode(List<Expression> expressions, List<Boolean> ascs, int parallelism, int edgeId) {
        this.expressions = expressions;
        this.ascs = ascs;
        this.parallelism = parallelism;
        this.edgeId = edgeId;
    }

    public List<Expression> getExpressions() {
        return expressions;
    }

    public List<Boolean> getAscs() {
        return ascs;
    }

    public int getParallelism() {
        return parallelism;
    }

    public int getEdgeId() {
        return edgeId;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        visitor.onSortMergeReceiveNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(expressions);
        out.writeObject(ascs);
        out.writeInt(parallelism);
        out.writeInt(edgeId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        expressions = in.readObject();
        ascs = in.readObject();
        parallelism = in.readInt();
        edgeId = in.readInt();
    }
}
