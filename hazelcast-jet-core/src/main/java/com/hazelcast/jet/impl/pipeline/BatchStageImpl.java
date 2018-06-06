/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBiPredicate;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedTriFunction;
import com.hazelcast.jet.impl.pipeline.transform.AbstractTransform;
import com.hazelcast.jet.impl.pipeline.transform.AggregateTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.StageWithGrouping;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.function.DistributedFunctions.constantKey;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class BatchStageImpl<T> extends ComputeStageImplBase<T> implements BatchStage<T> {

    BatchStageImpl(@Nonnull Transform transform, @Nonnull PipelineImpl pipeline) {
        super(transform, DO_NOT_ADAPT, pipeline, true);
    }

    /**
     * This constructor exists just to match the shape of the functional interface
     * {@code GeneralHashJoinBuilder.CreateOutStageFn}
     */
    public BatchStageImpl(@Nonnull Transform transform, FunctionAdapter ignored, @Nonnull PipelineImpl pipeline) {
        this(transform, pipeline);
    }

    @Nonnull @Override
    public <K> StageWithGrouping<T, K> groupingKey(@Nonnull DistributedFunction<? super T, ? extends K> keyFn) {
        return new StageWithGroupingImpl<>(this, keyFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> map(@Nonnull DistributedFunction<? super T, ? extends R> mapFn) {
        return attachMap(mapFn);
    }

    @Nonnull @Override
    public BatchStage<T> filter(@Nonnull DistributedPredicate<T> filterFn) {
        return attachFilter(filterFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> flatMap(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return attachFlatMap(flatMapFn);
    }

    @Nonnull @Override
    public <C, R> BatchStage<R> mapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends R> mapFn
    ) {
        return attachMapUsingContext(contextFactory, mapFn);
    }

    @Nonnull @Override
    public <C> BatchStage<T> filterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiPredicate<? super C, ? super T> filterFn
    ) {
        return attachFilterUsingContext(contextFactory, filterFn);
    }

    @Nonnull @Override
    public <C, R> BatchStage<R> flatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return attachFlatMapUsingContext(contextFactory, flatMapFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> aggregateRolling(@Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp) {
        return groupingKey(constantKey()).aggregateRolling(aggrOp, (k, v) -> v);
    }

    @Nonnull @Override
    public BatchStage<T> merge(@Nonnull BatchStage<? extends T> other) {
        return attachMerge(other);
    }

    @Nonnull @Override
    public <K, T1_IN, T1, R> BatchStage<R> hashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull DistributedBiFunction<T, T1, R> mapToOutputFn
    ) {
        return attachHashJoin(stage1, joinClause1, mapToOutputFn);
    }

    @Nonnull @Override
    public <K1, K2, T1_IN, T2_IN, T1, T2, R> BatchStage<R> hashJoin2(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull DistributedTriFunction<T, T1, T2, R> mapToOutputFn
    ) {
        return attachHashJoin2(stage1, joinClause1, stage2, joinClause2, mapToOutputFn);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <A, R> BatchStage<R> aggregate(@Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp) {
        return attach(new AggregateTransform<>(singletonList(transform), aggrOp), fnAdapter);
    }

    @Nonnull @Override
    public <T1, A, R> BatchStage<R> aggregate2(
            @Nonnull BatchStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, ? extends R> aggrOp
    ) {
        return attach(new AggregateTransform<>(asList(transform, transformOf(stage1)), aggrOp), DO_NOT_ADAPT);
    }

    @Nonnull @Override
    public <T1, T2, A, R> BatchStage<R> aggregate3(
            @Nonnull BatchStage<T1> stage1,
            @Nonnull BatchStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, ? extends R> aggrOp
    ) {
        return attach(new AggregateTransform<>(
                asList(transform, transformOf(stage1), transformOf(stage2)), aggrOp),
                DO_NOT_ADAPT);
    }

    @Nonnull @Override
    public BatchStage<T> peek(
            @Nonnull DistributedPredicate<? super T> shouldLogFn,
            @Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn
    ) {
        return attachPeek(shouldLogFn, toStringFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> customTransform(
            @Nonnull String stageName,
            @Nonnull DistributedSupplier<Processor> procSupplier
    ) {
        return attachCustomTransform(stageName, procSupplier);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    <RET> RET attach(@Nonnull AbstractTransform transform, @Nonnull FunctionAdapter fnAdapter) {
        pipelineImpl.connect(transform.upstream(), transform);
        return (RET) new BatchStageImpl<>(transform, pipelineImpl);
    }

    @Nonnull @Override
    public BatchStage<T> setLocalParallelism(int localParallelism) {
        super.setLocalParallelism(localParallelism);
        return this;
    }

    @Nonnull @Override
    public BatchStage<T> setName(@Nonnull String name) {
        super.setName(name);
        return this;
    }
}
