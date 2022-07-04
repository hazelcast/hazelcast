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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.pipeline.transform.AbstractTransform;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.jet.pipeline.StageWithWindow;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithKey;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.impl.util.Util.toList;

public class StreamStageImpl<T> extends ComputeStageImplBase<T> implements StreamStage<T> {

    public StreamStageImpl(
            @Nonnull Transform transform,
            @Nonnull FunctionAdapter fnAdapter,
            @Nonnull PipelineImpl pipeline
    ) {
        super(transform, fnAdapter, pipeline);
    }

    StreamStageImpl(StreamStageImpl<T> toCopy, boolean rebalanceOutput) {
        super(toCopy, rebalanceOutput);
    }

    <K> StreamStageImpl(StreamStageImpl<T> toCopy, FunctionEx<? super T, ? extends K> keyFn) {
        super(toCopy, keyFn);
    }

    @Nonnull @Override
    public <K> StreamStageWithKey<T, K> groupingKey(@Nonnull FunctionEx<? super T, ? extends K> keyFn) {
        checkSerializable(keyFn, "keyFn");
        return new StreamStageWithKeyImpl<>(this, keyFn);
    }

    @Nonnull @Override
    public <K> StreamStage<T> rebalance(@Nonnull FunctionEx<? super T, ? extends K> keyFn) {
        checkSerializable(keyFn, "keyFn");
        @SuppressWarnings("unchecked")
        FunctionEx<? super T, ? extends K> adaptedKeyFn =
                (FunctionEx<? super T, ? extends K>) fnAdapter.adaptKeyFn(keyFn);
        return new StreamStageImpl<>(this, adaptedKeyFn);
    }

    @Nonnull @Override
    public StreamStage<T> rebalance() {
        return new StreamStageImpl<>(this, true);
    }

    @Nonnull @Override
    public StageWithWindow<T> window(WindowDefinition wDef) {
        return new StageWithWindowImpl<>(this, wDef);
    }

    @Nonnull @Override
    public <R> StreamStage<R> map(@Nonnull FunctionEx<? super T, ? extends R> mapFn) {
        return attachMap(mapFn);
    }

    @Nonnull @Override
    public StreamStage<T> filter(@Nonnull PredicateEx<T> filterFn) {
        return attachFilter(filterFn);
    }

    @Nonnull @Override
    public <R> StreamStage<R> flatMap(
            @Nonnull FunctionEx<? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return attachFlatMap(flatMapFn);
    }

    @Nonnull @Override
    public <S, R> StreamStage<R> mapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        return attachGlobalMapStateful(createFn, mapFn);
    }

    @Nonnull @Override
    public <S> StreamStage<T> filterStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        return attachGlobalMapStateful(createFn, (s, t) -> filterFn.test(s, t) ? t : null);
    }

    @Nonnull @Override
    public <S, R> StreamStage<R> flatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return attachGlobalFlatMapStateful(createFn, flatMapFn);
    }

    @Nonnull @Override
    public <S, R> StreamStage<R> mapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        return attachMapUsingService(serviceFactory, mapFn);
    }

    @Nonnull @Override
    public <S, R> StreamStage<R> mapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxConcurrentOps,
            boolean preserveOrder,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<R>> mapAsyncFn
    ) {
        return attachMapUsingServiceAsync(serviceFactory, maxConcurrentOps, preserveOrder,
                (s, t) -> mapAsyncFn.apply(s, t).thenApply(Traversers::singleton));
    }

    @Nonnull @Override
    public <S, R> StreamStage<R> mapUsingServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<List<R>>> mapAsyncBatchedFn
    ) {
        return attachMapUsingServiceAsyncBatched(serviceFactory, maxBatchSize,
                (s, t) -> mapAsyncBatchedFn.apply(s, t).thenApply(list ->
                        toList(list, Traversers::singleton)));
    }

    @Nonnull @Override
    public <S> StreamStage<T> filterUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        return attachFilterUsingService(serviceFactory, filterFn);
    }

    @Nonnull @Override
    public <S, R> StreamStage<R> flatMapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return attachFlatMapUsingService(serviceFactory, flatMapFn);
    }

    @Nonnull @Override
    public StreamStage<T> merge(@Nonnull StreamStage<? extends T> other) {
        return attachMerge(other);
    }

    @Nonnull @Override
    public <K, T1_IN, T1, R> StreamStage<R> hashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BiFunctionEx<T, T1, R> mapToOutputFn
    ) {
        return attachHashJoin(stage1, joinClause1, mapToOutputFn);
    }

    @Nonnull @Override
    public <K, T1_IN, T1, R> StreamStage<R> innerHashJoin(
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
    public <K1, K2, T1_IN, T2_IN, T1, T2, R> StreamStage<R> hashJoin2(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull TriFunction<T, T1, T2, R> mapToOutputFn
    ) {
        return attachHashJoin2(stage1, joinClause1, stage2, joinClause2, mapToOutputFn);
    }

    @Nonnull @Override
    public <K1, K2, T1_IN, T2_IN, T1, T2, R> StreamStage<R> innerHashJoin2(
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
    public StreamStage<T> peek(
            @Nonnull PredicateEx<? super T> shouldLogFn,
            @Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn
    ) {
        return attachPeek(shouldLogFn, toStringFn);
    }

    @Nonnull @Override
    public <R> StreamStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorMetaSupplier procSupplier) {
        return attachCustomTransform(stageName, procSupplier);
    }

    @Override
    @SuppressWarnings("unchecked")
    <RET> RET newStage(@Nonnull AbstractTransform transform, @Nonnull FunctionAdapter fnAdapter) {
        return (RET) new StreamStageImpl<>(transform, fnAdapter, pipelineImpl);
    }

    @Nonnull @Override
    public StreamStage<T> setLocalParallelism(int localParallelism) {
        super.setLocalParallelism(localParallelism);
        return this;
    }

    @Nonnull @Override
    public StreamStage<T> setName(@Nonnull String name) {
        super.setName(name);
        return this;
    }
}
