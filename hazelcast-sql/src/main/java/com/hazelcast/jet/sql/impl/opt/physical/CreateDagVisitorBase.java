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

import com.hazelcast.jet.core.DAG;

public abstract class CreateDagVisitorBase<V> implements CreateDagVisitor<V> {

    protected final DAG dag;

    protected CreateDagVisitorBase(DAG dag) {
        this.dag = dag;
    }

    @Override
    public V onValues(ValuesPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onInsert(InsertPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onSink(SinkPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onUpdate(UpdatePhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onDelete(DeletePhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onFullScan(FullScanPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onMapIndexScan(IndexScanMapPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onCalc(CalcPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onSort(SortPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onAggregate(AggregatePhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onAccumulate(AggregateAccumulatePhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onCombine(AggregateCombinePhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onAggregateByKey(AggregateByKeyPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onAccumulateByKey(AggregateAccumulateByKeyPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onCombineByKey(AggregateCombineByKeyPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onSlidingWindow(SlidingWindowPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onSlidingWindowAggregate(SlidingWindowAggregatePhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onDropLateItems(DropLateItemsPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onNestedLoopJoin(JoinNestedLoopPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onHashJoin(JoinHashPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onStreamToStreamJoin(StreamToStreamJoinPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onUnion(UnionPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onLimit(LimitPhysicalRel rel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public V onRoot(RootRel rootRel) {
        throw new UnsupportedOperationException();
    }

    public DAG getDag() {
        return dag;
    }
}
