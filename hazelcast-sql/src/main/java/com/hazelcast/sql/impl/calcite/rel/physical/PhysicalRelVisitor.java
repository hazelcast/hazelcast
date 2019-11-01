/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.rel.physical;

import com.hazelcast.sql.impl.calcite.rel.physical.exchange.BroadcastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.exchange.SingletonSortMergeExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.exchange.UnicastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.index.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.join.HashJoinPhysicalRel;
import com.hazelcast.sql.impl.calcite.rel.physical.join.NestedLoopJoinPhysicalRel;

/**
 * Visitor over physical relations.
 */
public interface PhysicalRelVisitor {
    void onRoot(RootPhysicalRel root);
    void onMapScan(MapScanPhysicalRel rel);
    void onMapIndexScan(MapIndexScanPhysicalRel rel);
    void onReplicatedMapScan(ReplicatedMapScanPhysicalRel rel);
    void onUnicastExchange(UnicastExchangePhysicalRel rel);
    void onBroadcastExchange(BroadcastExchangePhysicalRel rel);
    void onSingletonSortMergeExchange(SingletonSortMergeExchangePhysicalRel rel);
    void onReplicatedToDistributed(ReplicatedToDistributedPhysicalRel rel);
    void onSort(SortPhysicalRel rel);
    void onProject(ProjectPhysicalRel rel);
    void onFilter(FilterPhysicalRel rel);
    void onCollocatedAggregate(CollocatedAggregatePhysicalRel rel);
    void onNestedLoopJoin(NestedLoopJoinPhysicalRel rel);
    void onHashJoin(HashJoinPhysicalRel rel);
    void onMaterializedInput(MaterializedInputPhysicalRel rel);
}
