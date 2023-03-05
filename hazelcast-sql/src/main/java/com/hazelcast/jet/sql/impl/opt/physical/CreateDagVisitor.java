/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.opt.physical;

public interface CreateDagVisitor<V> {
    V onValues(ValuesPhysicalRel rel);

    V onInsert(InsertPhysicalRel rel);

    V onSink(SinkPhysicalRel rel);

    V onUpdate(UpdatePhysicalRel rel);

    V onDelete(DeletePhysicalRel rel);

    V onFullScan(FullScanPhysicalRel rel);

    V onMapIndexScan(IndexScanMapPhysicalRel rel);

    V onCalc(CalcPhysicalRel rel);

    V onSort(SortPhysicalRel rel);

    V onAggregate(AggregatePhysicalRel rel);

    V onAccumulate(AggregateAccumulatePhysicalRel rel);

    V onCombine(AggregateCombinePhysicalRel rel);

    V onAggregateByKey(AggregateByKeyPhysicalRel rel);

    V onAccumulateByKey(AggregateAccumulateByKeyPhysicalRel rel);

    V onCombineByKey(AggregateCombineByKeyPhysicalRel rel);

    V onSlidingWindow(SlidingWindowPhysicalRel rel);

    V onSlidingWindowAggregate(SlidingWindowAggregatePhysicalRel rel);

    V onDropLateItems(DropLateItemsPhysicalRel rel);

    V onNestedLoopJoin(JoinNestedLoopPhysicalRel rel);

    V onHashJoin(JoinHashPhysicalRel rel);

    V onStreamToStreamJoin(StreamToStreamJoinPhysicalRel rel);

    V onUnion(UnionPhysicalRel rel);

    V onLimit(LimitPhysicalRel rel);

    V onRoot(RootRel rootRel);
}
