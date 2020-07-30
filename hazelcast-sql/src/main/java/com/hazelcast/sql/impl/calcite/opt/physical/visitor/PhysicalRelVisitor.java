/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.opt.physical.visitor;

import com.hazelcast.sql.impl.calcite.opt.physical.FetchPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ValuesPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.agg.AggregatePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.FilterPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MaterializedInputPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ProjectPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ReplicatedMapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ReplicatedToDistributedPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.SortPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.BroadcastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.RootExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.SortMergeExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.UnicastExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.join.HashJoinPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.join.NestedLoopJoinPhysicalRel;

/**
 * Visitor over physical relations.
 */
public interface PhysicalRelVisitor {
    void onRoot(RootPhysicalRel rel);
    void onMapScan(MapScanPhysicalRel rel);
    void onMapIndexScan(MapIndexScanPhysicalRel rel);
    void onReplicatedMapScan(ReplicatedMapScanPhysicalRel rel);
    void onRootExchange(RootExchangePhysicalRel rel);
    void onUnicastExchange(UnicastExchangePhysicalRel rel);
    void onBroadcastExchange(BroadcastExchangePhysicalRel rel);
    void onSortMergeExchange(SortMergeExchangePhysicalRel rel);
    void onReplicatedToDistributed(ReplicatedToDistributedPhysicalRel rel);
    void onSort(SortPhysicalRel rel);
    void onProject(ProjectPhysicalRel rel);
    void onFilter(FilterPhysicalRel rel);
    void onValues(ValuesPhysicalRel rel);
    void onAggregate(AggregatePhysicalRel rel);
    void onNestedLoopJoin(NestedLoopJoinPhysicalRel rel);
    void onHashJoin(HashJoinPhysicalRel rel);
    void onMaterializedInput(MaterializedInputPhysicalRel rel);
    void onFetch(FetchPhysicalRel rel);
}
