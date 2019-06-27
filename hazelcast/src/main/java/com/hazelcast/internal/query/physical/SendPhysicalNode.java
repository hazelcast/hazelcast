package com.hazelcast.internal.query.physical;

import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class SendPhysicalNode implements PhysicalNode {
    /** Edge ID. */
    private int edgeId;

    /** Child node. */
    private PhysicalNode delegate;

    /** Partition hasher (get partition hash from row). */
    private Expression<Integer> partitionHasher;

    /** Whether data partitions should be used to resolve target. */
    // TODO: Somethins is wrong here! partitionHasher + useDataPartitions should be the same?
    private boolean useDataPartitions;

    public SendPhysicalNode() {
        // No-op.
    }

    public SendPhysicalNode(int edgeId, PhysicalNode delegate, Expression<Integer> partitionHasher,
        boolean useDataPartitions) {
        this.edgeId = edgeId;
        this.delegate = delegate;
        this.partitionHasher = partitionHasher;
        this.useDataPartitions = useDataPartitions;
    }

    public int getEdgeId() {
        return edgeId;
    }

    public PhysicalNode getDelegate() {
        return delegate;
    }

    public Expression<Integer> getPartitionHasher() {
        return partitionHasher;
    }

    public boolean isUseDataPartitions() {
        return useDataPartitions;
    }

    @Override
    public void visit(PhysicalNodeVisitor visitor) {
        delegate.visit(visitor);

        visitor.onSendNode(this);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(edgeId);
        out.writeObject(delegate);
        out.writeObject(partitionHasher);
        out.writeBoolean(useDataPartitions);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        edgeId = in.readInt();
        delegate = in.readObject();
        partitionHasher = in.readObject();
        useDataPartitions = in.readBoolean();
    }
}
