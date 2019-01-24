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
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedTriFunction;
import com.hazelcast.jet.function.DistributedTriPredicate;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithKey;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

public class StreamStageWithKeyImpl<T, K> extends StageWithGroupingBase<T, K> implements StreamStageWithKey<T, K> {

    StreamStageWithKeyImpl(
            @Nonnull StreamStageImpl<T> computeStage,
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn
    ) {
        super(computeStage, keyFn);
    }

    @Nonnull @Override
    public StageWithKeyAndWindowImpl<T, K> window(@Nonnull WindowDefinition wDef) {
        return new StageWithKeyAndWindowImpl<>((StreamStageImpl<T>) computeStage, keyFn(), wDef);
    }

    @Nonnull @Override
    public <C, R> StreamStage<R> mapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriFunction<? super C, ? super K, ? super T, ? extends R> mapFn
    ) {
        return attachMapUsingContext(contextFactory, mapFn);
    }

    @Nonnull @Override
    public <C, R> StreamStage<R> mapUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriFunction<? super C, ? super K, ? super T, CompletableFuture<R>> mapAsyncFn
    ) {
        return attachTransformUsingContextAsync("map", contextFactory,
                (c, k, t) -> mapAsyncFn.apply(c, k, t).thenApply(Traversers::singleton));
    }

    @Nonnull @Override
    public <C> StreamStage<T> filterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriPredicate<? super C, ? super K, ? super T> filterFn
    ) {
        return attachFilterUsingContext(contextFactory, filterFn);
    }

    @Nonnull @Override
    public <C> StreamStage<T> filterUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriFunction<? super C, ? super K, ? super T, CompletableFuture<Boolean>> filterAsyncFn
    ) {
        return attachTransformUsingContextAsync("filter", contextFactory,
                (c, k, t) -> filterAsyncFn.apply(c, k, t).thenApply(passed -> passed ? Traversers.singleton(t) : null));
    }

    @Nonnull @Override
    public <C, R> StreamStage<R> flatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriFunction<? super C, ? super K, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return attachFlatMapUsingContext(contextFactory, flatMapFn);
    }

    @Nonnull @Override
    public <C, R> StreamStage<R> flatMapUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriFunction<? super C, ? super K, ? super T, CompletableFuture<Traverser<R>>>
                    flatMapAsyncFn
    ) {
        return attachTransformUsingContextAsync("flatMap", contextFactory, flatMapAsyncFn);
    }

    @Nonnull @Override
    public <R, OUT> StreamStage<OUT> rollingAggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp,
            @Nonnull DistributedBiFunction<? super K, ? super R, ? extends OUT> mapToOutputFn
    ) {
        return computeStage.attachRollingAggregate(keyFn(), aggrOp, mapToOutputFn);
    }

    @Nonnull @Override
    public <R> GeneralStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorSupplier procSupplier) {
        return computeStage.attachPartitionedCustomTransform(stageName, procSupplier, keyFn());
    }
}
