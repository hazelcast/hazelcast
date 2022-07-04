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
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.function.TriPredicate;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;

class StageWithGroupingBase<T, K> {

    final ComputeStageImplBase<T> computeStage;
    private final FunctionEx<? super T, ? extends K> keyFn;

    StageWithGroupingBase(
            @Nonnull ComputeStageImplBase<T> computeStage,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn
    ) {
        checkSerializable(keyFn, "keyFn");
        this.computeStage = computeStage;
        this.keyFn = keyFn;
    }

    @Nonnull
    public FunctionEx<? super T, ? extends K> keyFn() {
        return keyFn;
    }

    @Nonnull
    <S, R, RET> RET attachMapStateful(
            long ttl,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends R> onEvictFn
    ) {
        return computeStage.attachMapStateful(ttl, keyFn(), createFn, mapFn, onEvictFn);
    }

    @Nonnull
    <S, R, RET> RET attachFlatMapStateful(
            long ttl,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn
    ) {
        return computeStage.attachFlatMapStateful(ttl, keyFn(), createFn, flatMapFn, onEvictFn);
    }

    @Nonnull
    <S, R, RET> RET attachMapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn
    ) {
        FunctionEx<? super T, ? extends K> keyFn = keyFn();
        return computeStage.attachMapUsingPartitionedService(serviceFactory, keyFn, (s, t) -> {
            K k = keyFn.apply(t);
            return mapFn.apply(s, k, t);
        });
    }

    @Nonnull
    <S, RET> RET attachFilterUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriPredicate<? super S, ? super K, ? super T> filterFn
    ) {
        FunctionEx<? super T, ? extends K> keyFn = keyFn();
        return computeStage.attachFilterUsingPartitionedService(serviceFactory, keyFn, (s, t) -> {
            K k = keyFn.apply(t);
            return filterFn.test(s, k, t);
        });
    }

    @Nonnull
    <S, R, RET> RET attachFlatMapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        FunctionEx<? super T, ? extends K> keyFn = keyFn();
        return computeStage.attachFlatMapUsingPartitionedService(serviceFactory, keyFn, (s, t) -> {
            K k = keyFn.apply(t);
            return flatMapFn.apply(s, k, t);
        });
    }

    @Nonnull
    <S, R, RET> RET attachMapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxConcurrentOps,
            boolean preserveOrder,
            @Nonnull TriFunction<? super S, ? super K, ? super T, CompletableFuture<R>> mapAsyncFn
    ) {
        FunctionEx<? super T, ? extends K> keyFn = keyFn();
        return computeStage.attachMapUsingPartitionedServiceAsync(
                serviceFactory, maxConcurrentOps, preserveOrder, keyFn,
                (s, t) -> {
                    K k = keyFn.apply(t);
                    return mapAsyncFn.apply(s, k, t);
                });
    }

    @Nonnull
    <S, R, RET> RET attachMapUsingServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<List<R>>> mapAsyncFn
    ) {
        return computeStage.attachMapUsingPartitionedServiceAsyncBatched(
                serviceFactory, maxBatchSize, keyFn(), mapAsyncFn);
    }

    @Nonnull
    <S, R, RET> RET attachMapUsingServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull TriFunction<? super S, ? super List<K>, ? super List<T>, ? extends CompletableFuture<List<R>>>
                    mapAsyncFn
    ) {
        FunctionEx<? super T, ? extends K> keyFn = keyFn();
        return computeStage.attachMapUsingPartitionedServiceAsyncBatched(
                serviceFactory, maxBatchSize, keyFn,
                (s, items) -> {
                    List<K> keys = items.stream()
                            .map(t -> (K) keyFn.apply(t))
                            .collect(Collectors.toList());
                    return mapAsyncFn.apply(s, keys, items);
                });
    }
}
