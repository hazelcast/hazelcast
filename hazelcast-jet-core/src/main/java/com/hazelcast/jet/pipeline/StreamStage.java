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

package com.hazelcast.jet.pipeline;

import com.hazelcast.core.IMap;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBiPredicate;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedTriFunction;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * A stage in a distributed computation {@link Pipeline pipeline} that will
 * observe an unbounded amount of data (i.e., an event stream). It accepts
 * input from its upstream stages (if any) and passes its output to its
 * downstream stages.
 *
 * @param <T> the type of items coming out of this stage
 */
public interface StreamStage<T> extends GeneralStage<T> {

    /**
     * Adds the given window definition to this stage, as the first step in the
     * construction of a pipeline stage that performs windowed aggregation.
     *
     * @see WindowDefinition factory methods in WindowDefiniton
     */
    @Nonnull
    StageWithWindow<T> window(WindowDefinition wDef);

    /**
     * Attaches a stage that emits all the items from this stage as well as all
     * the items from the supplied stage. The other stage's type parameter must
     * be assignment-compatible with this stage's type parameter.
     *
     * @param other the other stage whose data to merge into this one
     * @return the newly attached stage
     */
    @Nonnull
    StreamStage<T> merge(@Nonnull StreamStage<? extends T> other);

    @Nonnull @Override
    <K> StreamStageWithKey<T, K> groupingKey(@Nonnull DistributedFunction<? super T, ? extends K> keyFn);

    @Nonnull @Override
    <R> StreamStage<R> map(@Nonnull DistributedFunction<? super T, ? extends R> mapFn);

    @Nonnull @Override
    StreamStage<T> filter(@Nonnull DistributedPredicate<T> filterFn);

    @Nonnull @Override
    <R> StreamStage<R> flatMap(@Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn);

    @Nonnull @Override
    <C, R> StreamStage<R> mapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends R> mapFn
    );

    @Nonnull @Override
    <C, R> StreamStage<R> mapUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends CompletableFuture<R>> mapAsyncFn
    );

    @Nonnull @Override
    <C> StreamStage<T> filterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiPredicate<? super C, ? super T> filterFn
    );

    @Nonnull @Override
    <C> StreamStage<T> filterUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends CompletableFuture<Boolean>> filterAsyncFn
    );

    @Nonnull @Override
    <C, R> StreamStage<R> flatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<R>> flatMapFn
    );

    @Nonnull @Override
    <C, R> StreamStage<R> flatMapUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends CompletableFuture<Traverser<R>>> flatMapAsyncFn
    );

    @Override @Nonnull
    default <K, V, R> StreamStage<R> mapUsingReplicatedMap(
            @Nonnull String mapName,
            @Nonnull DistributedBiFunction<? super ReplicatedMap<K, V>, ? super T, ? extends R> mapFn
    ) {
        return (StreamStage<R>) GeneralStage.super.<K, V, R>mapUsingReplicatedMap(mapName, mapFn);
    }

    @Override @Nonnull
    default <K, V, R> StreamStage<R> mapUsingReplicatedMap(
            @Nonnull ReplicatedMap<K, V> replicatedMap,
            @Nonnull DistributedBiFunction<? super ReplicatedMap<K, V>, ? super T, ? extends R> mapFn
    ) {
        return (StreamStage<R>) GeneralStage.super.<K, V, R>mapUsingReplicatedMap(replicatedMap, mapFn);
    }

    @Override @Nonnull
    default <K, V, R> StreamStage<R> mapUsingIMapAsync(
            @Nonnull String mapName,
            @Nonnull DistributedBiFunction<? super IMap<K, V>, ? super T, ? extends CompletableFuture<R>> mapFn
    ) {
        return (StreamStage<R>) GeneralStage.super.<K, V, R>mapUsingIMapAsync(mapName, mapFn);
    }

    @Override @Nonnull
    default <K, V, R> StreamStage<R> mapUsingIMapAsync(
            @Nonnull IMap<K, V> iMap,
            @Nonnull DistributedBiFunction<? super IMap<K, V>, ? super T, ? extends CompletableFuture<R>> mapFn
    ) {
        return (StreamStage<R>) GeneralStage.super.<K, V, R>mapUsingIMapAsync(iMap, mapFn);
    }

    @Nonnull @Override
    <R> StreamStage<R> rollingAggregate(@Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp);

    @Nonnull @Override
    <K, T1_IN, T1, R> StreamStage<R> hashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull DistributedBiFunction<T, T1, R> mapToOutputFn
    );

    @Nonnull @Override
    <K1, K2, T1_IN, T2_IN, T1, T2, R> StreamStage<R> hashJoin2(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull DistributedTriFunction<T, T1, T2, R> mapToOutputFn
    );

    @Nonnull @Override
    default StreamHashJoinBuilder<T> hashJoinBuilder() {
        return new StreamHashJoinBuilder<>(this);
    }

    @Nonnull @Override
    default StreamStage<T> peek() {
        return (StreamStage<T>) GeneralStage.super.peek();
    }

    @Nonnull @Override
    StreamStage<T> peek(
            @Nonnull DistributedPredicate<? super T> shouldLogFn,
            @Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn);

    @Nonnull @Override
    default StreamStage<T> peek(@Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn) {
        return (StreamStage<T>) GeneralStage.super.peek(toStringFn);
    }

    @Nonnull @Override
    default <R> StreamStage<R> customTransform(@Nonnull String stageName,
                                               @Nonnull DistributedSupplier<Processor> procSupplier) {
        return customTransform(stageName, ProcessorMetaSupplier.of(procSupplier));
    }

    @Nonnull @Override
    default <R> StreamStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorSupplier procSupplier) {
        return customTransform(stageName, ProcessorMetaSupplier.of(procSupplier));
    }

    @Nonnull @Override
    <R> StreamStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorMetaSupplier procSupplier);

    @Nonnull @Override
    StreamStage<T> setLocalParallelism(int localParallelism);

    @Nonnull @Override
    StreamStage<T> setName(@Nonnull String name);
}
