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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedTriFunction;
import com.hazelcast.jet.function.DistributedTriPredicate;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

/**
 * An intermediate step while constructing a windowed group-and-aggregate
 * pipeline stage. It captures the grouping key and offers a method to
 * specify the window definition.
 *
 * @param <T> type of the stream items
 * @param <K> type of the key
 */
public interface StreamStageWithKey<T, K> extends GeneralStageWithKey<T, K> {

    /**
     * Adds the definition of the window to use in the group-and-aggregate
     * pipeline stage being constructed.
     */
    @Nonnull
    StageWithKeyAndWindow<T, K> window(@Nonnull WindowDefinition wDef);

    @Nonnull @Override
    default <V, R> StreamStage<R> mapUsingIMapAsync(
            @Nonnull String mapName,
            @Nonnull DistributedBiFunction<? super T, ? super V, ? extends R> mapFn
    ) {
        return (StreamStage<R>) GeneralStageWithKey.super.<V, R>mapUsingIMapAsync(mapName, mapFn);
    }

    @Nonnull @Override
    default <V, R> StreamStage<R> mapUsingIMapAsync(
            @Nonnull IMap<K, V> iMap,
            @Nonnull DistributedBiFunction<? super T, ? super V, ? extends R> mapFn
    ) {
        return (StreamStage<R>) GeneralStageWithKey.super.mapUsingIMapAsync(iMap, mapFn);
    }

    @Nonnull @Override
    <C, R> StreamStage<R> mapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriFunction<? super C, ? super K, ? super T, ? extends R> mapFn
    );

    @Nonnull @Override
    <C, R> StreamStage<R> mapUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriFunction<? super C, ? super K, ? super T, CompletableFuture<R>> mapAsyncFn
    );

    @Nonnull @Override
    <C> StreamStage<T> filterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriPredicate<? super C, ? super K, ? super T> filterFn
    );

    @Nonnull @Override
    <C> StreamStage<T> filterUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriFunction<? super C, ? super K, ? super T, CompletableFuture<Boolean>> filterAsyncFn
    );

    @Nonnull @Override
    <C, R> StreamStage<R> flatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriFunction<? super C, ? super K, ? super T, ? extends Traverser<? extends R>> flatMapFn
    );

    @Nonnull @Override
    <C, R> StreamStage<R> flatMapUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriFunction<? super C, ? super K, ? super T, CompletableFuture<Traverser<R>>>
                    flatMapAsyncFn
    );

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    <R, OUT> StreamStage<OUT> rollingAggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp,
            @Nonnull DistributedBiFunction<? super K, ? super R, ? extends OUT> mapToOutputFn
    );

    @Nonnull @Override
    default <R> StreamStage<Entry<K, R>> rollingAggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        return (StreamStage<Entry<K, R>>) GeneralStageWithKey.super.<R>rollingAggregate(aggrOp);
    }


}
