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
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.function.TriPredicate;
import com.hazelcast.jet.impl.pipeline.transform.DistinctTransform;
import com.hazelcast.jet.impl.pipeline.transform.GroupTransform;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.BatchStageWithKey;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.DO_NOT_ADAPT;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class BatchStageWithKeyImpl<T, K> extends StageWithGroupingBase<T, K> implements BatchStageWithKey<T, K> {

    BatchStageWithKeyImpl(
            @Nonnull BatchStageImpl<T> computeStage,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn
    ) {
        super(computeStage, keyFn);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> mapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn
    ) {
        return attachMapStateful(0, createFn, mapFn, null);
    }

    @Nonnull @Override
    public <S> BatchStage<T> filterStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        return attachMapStateful(0, createFn, (s, k, t) -> filterFn.test(s, t) ? t : null, null);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> flatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return attachFlatMapStateful(0, createFn, flatMapFn, null);
    }

    @Nonnull @Override
    public BatchStage<T> distinct() {
        return computeStage.attach(new DistinctTransform<>(computeStage.transform, keyFn()), DO_NOT_ADAPT);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> mapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn
    ) {
        return attachMapUsingService(serviceFactory, mapFn);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> mapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxConcurrentOps,
            boolean preserveOrder,
            @Nonnull TriFunction<? super S, ? super K, ? super T, CompletableFuture<R>> mapAsyncFn
    ) {
        return attachMapUsingServiceAsync(serviceFactory, maxConcurrentOps, preserveOrder, mapAsyncFn);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> mapUsingServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<List<R>>> mapAsyncFn
    ) {
        return attachMapUsingServiceAsyncBatched(serviceFactory, maxBatchSize, mapAsyncFn);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> mapUsingServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull TriFunction<? super S, ? super List<K>, ? super List<T>, ? extends CompletableFuture<List<R>>>
                    mapAsyncFn
    ) {
        return attachMapUsingServiceAsyncBatched(serviceFactory, maxBatchSize, mapAsyncFn);
    }

    @Nonnull @Override
    public <S> BatchStage<T> filterUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriPredicate<? super S, ? super K, ? super T> filterFn
    ) {
        return attachFilterUsingService(serviceFactory, filterFn);
    }

    @Nonnull @Override
    public <S, R> BatchStage<R> flatMapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return attachFlatMapUsingService(serviceFactory, flatMapFn);
    }

    @Nonnull @Override
    public <R> BatchStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorMetaSupplier procSupplier) {
        return computeStage.attachPartitionedCustomTransform(stageName, procSupplier, keyFn());
    }

    @Nonnull @Override
    public <R> BatchStage<Entry<K, R>> aggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        return computeStage.attach(new GroupTransform<>(
                        singletonList(computeStage.transform),
                        singletonList(keyFn()),
                        aggrOp,
                        Util::entry),
                DO_NOT_ADAPT);
    }

    @Nonnull @Override
    @SuppressWarnings("rawtypes")
    public <T1, R> BatchStage<Entry<K, R>> aggregate2(
            @Nonnull BatchStageWithKey<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, ?, R> aggrOp
    ) {
        ComputeStageImplBase computeStage1 = ((StageWithGroupingBase) stage1).computeStage;
        return computeStage.attach(
                new GroupTransform<>(
                        asList(computeStage.transform, computeStage1.transform),
                        asList(keyFn(), stage1.keyFn()),
                        aggrOp,
                        Util::entry
                ),
                singletonList((GeneralStage<?>) computeStage1),
                DO_NOT_ADAPT);
    }

    @Nonnull @Override
    @SuppressWarnings("rawtypes")
    public <T1, T2, R> BatchStage<Entry<K, R>> aggregate3(
            @Nonnull BatchStageWithKey<T1, ? extends K> stage1,
            @Nonnull BatchStageWithKey<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, ?, ? extends R> aggrOp
    ) {
        ComputeStageImplBase computeStage1 = ((StageWithGroupingBase) stage1).computeStage;
        ComputeStageImplBase computeStage2 = ((StageWithGroupingBase) stage2).computeStage;
        return computeStage.attach(
                new GroupTransform<>(
                        asList(computeStage.transform, computeStage1.transform, computeStage2.transform),
                        asList(keyFn(), stage1.keyFn(), stage2.keyFn()),
                        aggrOp,
                        Util::entry),
                asList((GeneralStage<?>) computeStage1, (GeneralStage<?>) computeStage2),
                DO_NOT_ADAPT);
    }
}
