package com.hazelcast.internal.query.plan.physical;

public interface PhysicalNodeVisitor {
    void onRootNode(RootPhysicalNode node);
    void onReceiveNode(ReceivePhysicalNode node);
    void onSendNode(SendPhysicalNode node);
    void onMapScanNode(MapScanPhysicalNode node);
    void onSortNode(SortPhysicalNode node);
    void onSortMergeReceiveNode(SortMergeReceivePhysicalNode node);
}
