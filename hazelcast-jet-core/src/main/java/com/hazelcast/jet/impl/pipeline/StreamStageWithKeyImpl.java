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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.function.TriPredicate;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithKey;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.function.BiPredicateEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

public class StreamStageWithKeyImpl<T, K> extends StageWithGroupingBase<T, K> implements StreamStageWithKey<T, K> {

    StreamStageWithKeyImpl(
            @Nonnull StreamStageImpl<T> computeStage,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn
    ) {
        super(computeStage, keyFn);
    }

    @Nonnull @Override
    public StageWithKeyAndWindowImpl<T, K> window(@Nonnull WindowDefinition wDef) {
        return new StageWithKeyAndWindowImpl<>((StreamStageImpl<T>) computeStage, keyFn(), wDef);
    }

    @Nonnull @Override
    public <S, R> StreamStage<R> mapStateful(
            long ttl,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn,
            @Nonnull TriFunction<? super S, ? super K, ? super Long, ? extends R> onEvictFn
    ) {
        return attachMapStateful(ttl, createFn, mapFn, onEvictFn);
    }

    @Nonnull @Override
    public <S, R> StreamStage<R> mapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn
    ) {
        return attachMapStateful(0, createFn, mapFn, null);
    }

    @Nonnull @Override
    public <S> StreamStage<T> filterStateful(
            long ttl,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        return attachMapStateful(ttl, createFn, (s, k, t) -> filterFn.test(s, t) ? t : null, null);
    }

    @Nonnull @Override
    public <S, R> StreamStage<R> flatMapStateful(
            long ttl,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn,
            @Nonnull TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn
    ) {
        return attachFlatMapStateful(ttl, createFn, flatMapFn, onEvictFn);
    }

    @Nonnull @Override
    public <S, R> StreamStage<R> flatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return attachFlatMapStateful(0, createFn, flatMapFn, null);
    }

    @Nonnull @Override
    public <C, R> StreamStage<R> mapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, ? extends R> mapFn
    ) {
        return attachMapUsingContext(contextFactory, mapFn);
    }

    @Nonnull @Override
    public <C, R> StreamStage<R> mapUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, CompletableFuture<R>> mapAsyncFn
    ) {
        return attachTransformUsingContextAsync("map", contextFactory,
                (c, k, t) -> mapAsyncFn.apply(c, k, t).thenApply(Traversers::singleton));
    }

    @Nonnull @Override
    public <C> StreamStage<T> filterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriPredicate<? super C, ? super K, ? super T> filterFn
    ) {
        return attachFilterUsingContext(contextFactory, filterFn);
    }

    @Nonnull @Override
    public <C> StreamStage<T> filterUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, CompletableFuture<Boolean>> filterAsyncFn
    ) {
        return attachTransformUsingContextAsync("filter", contextFactory,
                (c, k, t) -> filterAsyncFn.apply(c, k, t).thenApply(passed -> passed ? Traversers.singleton(t) : null));
    }

    @Nonnull @Override
    public <C, R> StreamStage<R> flatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return attachFlatMapUsingContext(contextFactory, flatMapFn);
    }

    @Nonnull @Override
    public <C, R> StreamStage<R> flatMapUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, CompletableFuture<Traverser<R>>>
                    flatMapAsyncFn
    ) {
        return attachTransformUsingContextAsync("flatMap", contextFactory, flatMapAsyncFn);
    }

    @Nonnull @Override
    public <R> StreamStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorMetaSupplier procSupplier) {
        return computeStage.attachPartitionedCustomTransform(stageName, procSupplier, keyFn());
    }
}
