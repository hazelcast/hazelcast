package com.hazelcast.internal.query.physical;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class RootPhysicalNode implements PhysicalNode {

    private PhysicalNode delegate;

    public RootPhysicalNode() {
        // No-op.
    }

    public RootPhysicalNode(PhysicalNode delegate) {
        this.delegate = delegate;
    }

    public PhysicalNode getDelegate() {
        return delegate;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        delegate.visit(visitor);

        visitor.onRootNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(delegate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        delegate = in.readObject();
    }
}
