/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BiPredicateEx;
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.pipeline.transform.AbstractTransform;
import com.hazelcast.jet.impl.pipeline.transform.AggregateTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.BatchStageWithKey;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class BatchStageImpl<T> extends ComputeStageImplBase<T> implements BatchStage<T> {

    BatchStageImpl(@Nonnull Transform transform, @Nonnull PipelineImpl pipeline) {
        super(transform, DO_NOT_ADAPT, pipeline);
    }

    /**
     * This constructor exists just to match the shape of the functional interface
     * {@code GeneralHashJoinBuilder.CreateOutStageFn}
     */
    public BatchStageImpl(@Nonnull Transform transform, FunctionAdapter ignored, @Nonnull PipelineImpl pipeline) {
        this(transform, pipeline);
    }

    BatchStageImpl(BatchStageImpl<T> toCopy, boolean rebalanceOutput) {
        super(toCopy, rebalanceOutput);
    }

    <K> BatchStageImpl(BatchStageImpl<T> toCopy, FunctionEx<? super T, ? extends K> keyFn) {
        super(toCopy, keyFn);
    }

    @Nonnull @Override
    public <K> BatchStageWithKey<T, K> groupingKey(@Nonnull FunctionEx<? super T, ? extends K> keyFn) {
        checkSerializable(keyFn, "keyFn");
        return new BatchStageWithKeyImpl<>(this, keyFn);
    }

    @Nonnull @Override
    public <K> BatchStage<T> rebalance(@Nonnull FunctionEx<? super T, ? extends K> keyFn) {
        checkSerializable(keyFn, "keyFn");
        return new BatchStageImpl<>(this, keyFn);
    }

    @Nonnull @Override
    public BatchStage<T> rebalance() {
        return new BatchStageImpl<>(this, true);
    }

    @Nonnull @Override
    public BatchStage<T> sort() {
        return attachSort(null);
    }

    @Nonnull @Override
    public BatchStage<T> sort(@Nonnull ComparatorEx<? super T> comparator) {
        return attachSort(comparator);
    }

    @Nonnull @Override
    public <R> BatchStage<R> map(@Nonnull FunctionEx<? super T, ? extends R> mapFn) {
        return attachMap(mapFn);
    }

    @Nonnull @Override
    public BatchStage<T> filter(@Nonnull PredicateEx<T> filterFn) {
        return attachFilter(filterFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> flatMap(
            @Nonnull FunctionEx<? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return attachFlatMap(flatMapFn);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> mapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        return attachGlobalMapStateful(createFn, mapFn);
    }

    @Nonnull @Override
    public <S> BatchStage<T> filterStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        return attachGlobalMapStateful(createFn, (s, t) -> filterFn.test(s, t) ? t : null);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> flatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return attachGlobalFlatMapStateful(createFn, flatMapFn);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> mapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        return attachMapUsingService(serviceFactory, mapFn);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> mapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxConcurrentOps,
            boolean preserveOrder,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<R>> mapAsyncFn
    ) {
        return attachMapUsingServiceAsync(serviceFactory, maxConcurrentOps, preserveOrder,
                (s, t) -> mapAsyncFn.apply(s, t).thenApply(Traversers::singleton));
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> mapUsingServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<List<R>>> mapAsyncFn
    ) {
        return attachMapUsingServiceAsyncBatched(serviceFactory, maxBatchSize,
                (s, t) -> mapAsyncFn.apply(s, t).thenApply(list -> toList(list, Traversers::singleton)));
    }

    @Nonnull @Override
    public <S> BatchStage<T> filterUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        return attachFilterUsingService(serviceFactory, filterFn);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> flatMapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return attachFlatMapUsingService(serviceFactory, flatMapFn);
    }

    @Nonnull @Override
    public BatchStage<T> merge(@Nonnull BatchStage<? extends T> other) {
        return attachMerge(other);
    }

    @Nonnull @Override
    public <K, T1_IN, T1, R> BatchStage<R> hashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BiFunctionEx<T, T1, R> mapToOutputFn
    ) {
        return attachHashJoin(stage1, joinClause1, mapToOutputFn);
    }

    @Nonnull @Override
    public <K, T1_IN, T1, R> BatchStage<R> innerHashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BiFunctionEx<T, T1, R> mapToOutputFn
    ) {
        BiFunctionEx<T, T1, R> finalOutputFn = (leftSide, rightSide) -> {
            if (leftSide == null || rightSide == null) {
                return null;
            }
            return mapToOutputFn.apply(leftSide, rightSide);
        };
        return attachHashJoin(stage1, joinClause1, finalOutputFn);
    }

    @Nonnull @Override
    public <K1, K2, T1_IN, T2_IN, T1, T2, R> BatchStage<R> hashJoin2(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull TriFunction<T, T1, T2, R> mapToOutputFn
    ) {
        return attachHashJoin2(stage1, joinClause1, stage2, joinClause2, mapToOutputFn);
    }

    @Nonnull @Override
    public <K1, K2, T1_IN, T2_IN, T1, T2, R> BatchStage<R> innerHashJoin2(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull TriFunction<T, T1, T2, R> mapToOutputFn
    ) {
        TriFunction<T, T1, T2, R> finalOutputFn = (leftSide, middle, rightSide) -> {
            if (leftSide == null || middle == null || rightSide == null) {
                return null;
            }
            return mapToOutputFn.apply(leftSide, middle, rightSide);
        };
        return attachHashJoin2(stage1, joinClause1, stage2, joinClause2, finalOutputFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> aggregate(@Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp) {
        return attach(new AggregateTransform<>(singletonList(transform), aggrOp), fnAdapter);
    }

    @Nonnull @Override
    public <T1, R> BatchStage<R> aggregate2(
            @Nonnull BatchStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, ?, ? extends R> aggrOp
    ) {
        return attach(
                new AggregateTransform<>(asList(transform, transformOf(stage1)), aggrOp),
                singletonList(stage1),
                DO_NOT_ADAPT);
    }

    @Nonnull @Override
    public <T1, T2, R> BatchStage<R> aggregate3(
            @Nonnull BatchStage<T1> stage1,
            @Nonnull BatchStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, ?, ? extends R> aggrOp
    ) {
        return attach(new AggregateTransform<>(asList(transform, transformOf(stage1), transformOf(stage2)), aggrOp),
                asList(stage1, stage2),
                DO_NOT_ADAPT);
    }

    @Nonnull @Override
    public BatchStage<T> peek(
            @Nonnull PredicateEx<? super T> shouldLogFn,
            @Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn
    ) {
        return attachPeek(shouldLogFn, toStringFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> customTransform(
            @Nonnull String stageName,
            @Nonnull ProcessorMetaSupplier procSupplier
    ) {
        return attachCustomTransform(stageName, procSupplier);
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

    @Override
    @SuppressWarnings("unchecked")
    <RET> RET newStage(@Nonnull AbstractTransform transform, @Nonnull FunctionAdapter fnAdapter) {
        return (RET) new BatchStageImpl<>(transform, pipelineImpl);
    }
}
