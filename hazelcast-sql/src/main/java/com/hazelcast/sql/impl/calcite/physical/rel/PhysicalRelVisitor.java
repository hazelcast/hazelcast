package com.hazelcast.sql.impl.calcite.physical.rel;

/**
 * Visitor over physical expressions.
 */
public interface PhysicalRelVisitor {
    void onRoot(RootPhysicalRel root);
    void onMapScan(MapScanPhysicalRel rel);
    void onSingletonExchange(SingletonExchangePhysicalRel rel);
    void onSortMergeExchange(SortMergeExchangePhysicalRel rel);
    void onSort(SortPhysicalRel rel);
}
