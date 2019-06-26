package com.hazelcast.internal.query.physical;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class ReceivePhysicalNode implements PhysicalNode {

    private int edgeId;
    private int parallelism;

    public ReceivePhysicalNode() {
        // No-op.
    }

    public ReceivePhysicalNode(int edgeId, int parallelism) {
        this.edgeId = edgeId;
        this.parallelism = parallelism;
    }

    public int getEdgeId() {
        return edgeId;
    }

    public int getParallelism() {
        return parallelism;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        visitor.onReceiveNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(edgeId);
        out.writeInt(parallelism);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        edgeId = in.readInt();
        parallelism = in.readInt();
    }
}
