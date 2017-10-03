/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.ComputeStage;
import com.hazelcast.jet.JoinClause;
import com.hazelcast.jet.Sink;
import com.hazelcast.jet.SinkStage;
import com.hazelcast.jet.Source;
import com.hazelcast.jet.Stage;
import com.hazelcast.jet.Transform;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.impl.transform.CoGroupTransform;
import com.hazelcast.jet.impl.transform.FilterTransform;
import com.hazelcast.jet.impl.transform.FlatMapTransform;
import com.hazelcast.jet.impl.transform.GroupByTransform;
import com.hazelcast.jet.impl.transform.HashJoinTransform;
import com.hazelcast.jet.impl.transform.MapTransform;
import com.hazelcast.jet.impl.transform.MultiTransform;
import com.hazelcast.jet.impl.transform.ProcessorTransform;
import com.hazelcast.jet.impl.transform.UnaryTransform;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

@SuppressWarnings("unchecked")
public class ComputeStageImpl<E> extends AbstractStage implements ComputeStage<E> {

    ComputeStageImpl(List<Stage> upstream, Transform transform, PipelineImpl pipeline) {
        super(upstream, new ArrayList<>(), transform, pipeline);
    }

    ComputeStageImpl(Source<E> source, PipelineImpl pipeline) {
        this(emptyList(), source, pipeline);
    }

    ComputeStageImpl(Stage upstream, Transform transform, PipelineImpl pipeline) {
        this(singletonList(upstream), transform, pipeline);
    }

    @Override
    public <R> ComputeStage<R> map(DistributedFunction<? super E, ? extends R> mapFn) {
        return attach(new MapTransform<>(mapFn));
    }

    @Override
    public ComputeStage<E> filter(DistributedPredicate<E> filterFn) {
        return attach(new FilterTransform<>(filterFn));
    }

    @Override
    public <R> ComputeStage<R> flatMap(DistributedFunction<? super E, Traverser<? extends R>> flatMapFn) {
        return attach(new FlatMapTransform<>(flatMapFn));
    }

    @Override
    public <K, A, R> ComputeStage<Entry<K, R>> groupBy(
            DistributedFunction<? super E, ? extends K> keyFn, AggregateOperation1<? super E, A, R> aggrOp
    ) {
        return attach(new GroupByTransform<>(keyFn, aggrOp));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, E1_IN, E1> ComputeStage<Tuple2<E, E1>> hashJoin(
            ComputeStage<E1_IN> stage1, JoinClause<K, E, E1_IN, E1> joinClause
    ) {
        return attach(new HashJoinTransform<E>(singletonList(joinClause), emptyList()), singletonList(stage1));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K1, E1_IN, E1, K2, E2_IN, E2> ComputeStage<Tuple3<E, E1, E2>> hashJoin(
            ComputeStage<E1_IN> stage1, JoinClause<K1, E, E1_IN, E1> joinClause1,
            ComputeStage<E2_IN> stage2, JoinClause<K2, E, E2_IN, E2> joinClause2
    ) {
        return attach(new HashJoinTransform<E>(asList(joinClause1, joinClause2), emptyList()), asList(stage1, stage2));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, A, E1, R> ComputeStage<Entry<K, R>> coGroup(
            DistributedFunction<? super E, ? extends K> thisKeyFn,
            ComputeStage<E1> stage1, DistributedFunction<? super E1, ? extends K> key1Fn,
            AggregateOperation2<? super E, ? super E1, A, R> aggrOp
    ) {
        return attach(new CoGroupTransform<K, A, R>(asList(thisKeyFn, key1Fn), aggrOp), singletonList(stage1));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, A, E1, E2, R> ComputeStage<Entry<K, R>> coGroup(
            DistributedFunction<? super E, ? extends K> thisKeyFn,
            ComputeStage<E1> stage1, DistributedFunction<? super E1, ? extends K> key1Fn,
            ComputeStage<E2> stage2, DistributedFunction<? super E2, ? extends K> key2Fn,
            AggregateOperation3<? super E, ? super E1, ? super E2, A, R> aggrOp
    ) {
        return attach(new CoGroupTransform<K, A, R>(asList(thisKeyFn, key1Fn, key2Fn), aggrOp), asList(stage1, stage2));
    }

    private <R> ComputeStage<R> attach(UnaryTransform<? super E, R> unaryTransform) {
        return pipelineImpl.attach(this, unaryTransform);
    }

    private <R> ComputeStage<R> attach(MultiTransform<R> multiTransform, List<ComputeStage> otherInputs) {
        return pipelineImpl.attach(
                Stream.concat(Stream.of(this), otherInputs.stream()).collect(toList()),
                multiTransform);
    }

    @Override
    public SinkStage drainTo(Sink sink) {
        return pipelineImpl.drainTo(this, sink);
    }

    @Override
    public <R> ComputeStage<R> customTransform(String stageName, DistributedSupplier<Processor> procSupplier) {
        return attach(new ProcessorTransform<E, R>(stageName, procSupplier));
    }
}
