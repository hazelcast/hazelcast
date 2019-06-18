package com.hazelcast.internal.query.plan.physical;

public abstract class AbstractPhysicalNodeVisitor implements PhysicalNodeVisitor {
    @Override
    public void onRootNode(RootPhysicalNode node) {
        // No-op.
    }

    @Override
    public void onReceiveNode(ReceivePhysicalNode node) {
        // No-op.
    }

    @Override
    public void onSendNode(SendPhysicalNode node) {
        // No-op.
    }

    @Override
    public void onMapScanNode(MapScanPhysicalNode node) {
        // No-op.
    }
}
