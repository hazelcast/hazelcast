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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BiPredicateEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.impl.pipeline.PipelineImpl.Context;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.core.processor.Processors.filterUsingServiceP;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingServiceP;

public final class PartitionedProcessorTransform<T, K> extends ProcessorTransform {

    private static final long serialVersionUID = 1L;

    private final FunctionEx<? super T, ? extends K> partitionKeyFn;

    private PartitionedProcessorTransform(
            @Nonnull String name,
            @Nonnull Transform upstream,
            @Nonnull ProcessorMetaSupplier processorSupplier,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn
    ) {
        super(name, upstream, processorSupplier);
        this.partitionKeyFn = partitionKeyFn;
    }

    public static <T, K> PartitionedProcessorTransform<T, K> partitionedCustomProcessorTransform(
            @Nonnull String name,
            @Nonnull Transform upstream,
            @Nonnull ProcessorMetaSupplier processorSupplier,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn
    ) {
        return new PartitionedProcessorTransform<>(name, upstream, processorSupplier, partitionKeyFn);
    }

    public static <S, T, K, R> PartitionedProcessorTransform<T, K> mapUsingServicePartitionedTransform(
            @Nonnull Transform upstream,
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn
    ) {
        return new PartitionedProcessorTransform<>("mapUsingPartitionedService",
                upstream, ProcessorMetaSupplier.of(getPreferredLP(serviceFactory), serviceFactory.permission(),
                Processors.mapUsingServiceP(serviceFactory, mapFn)), partitionKeyFn);
    }

    public static <S, T, K> PartitionedProcessorTransform<T, K> filterUsingServicePartitionedTransform(
            @Nonnull Transform upstream,
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn
    ) {
        return new PartitionedProcessorTransform<>("filterUsingPartitionedService",
                upstream, ProcessorMetaSupplier.of(getPreferredLP(serviceFactory), serviceFactory.permission(),
                filterUsingServiceP(serviceFactory, filterFn)), partitionKeyFn);
    }

    public static <S, T, K, R> PartitionedProcessorTransform<T, K> flatMapUsingServicePartitionedTransform(
            @Nonnull Transform upstream,
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn
    ) {
        return new PartitionedProcessorTransform<>("flatMapUsingPartitionedService",
                upstream, ProcessorMetaSupplier.of(getPreferredLP(serviceFactory), serviceFactory.permission(),
                flatMapUsingServiceP(serviceFactory, flatMapFn)), partitionKeyFn);
    }

    public static <S, T, K, R> PartitionedProcessorTransform<T, K> flatMapUsingServiceAsyncPartitionedTransform(
            @Nonnull Transform upstream,
            @Nonnull String operationName,
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxConcurrentOps,
            boolean preserveOrder,
            @Nonnull BiFunctionEx<? super S, ? super T, CompletableFuture<Traverser<R>>> flatMapAsyncFn,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn
    ) {
        String name = operationName + "UsingPartitionedServiceAsync";
        ProcessorSupplier supplier = flatMapUsingServiceAsyncP(
                serviceFactory, maxConcurrentOps, preserveOrder, partitionKeyFn, flatMapAsyncFn);
        ProcessorMetaSupplier metaSupplier = ProcessorMetaSupplier.of(getPreferredLP(serviceFactory),
                serviceFactory.permission(), supplier);
        return new PartitionedProcessorTransform<>(name, upstream, metaSupplier, partitionKeyFn);
    }

    public static <S, T, K, R> PartitionedProcessorTransform<T, K> flatMapUsingServiceAsyncBatchedPartitionedTransform(
            @Nonnull Transform upstream,
            @Nonnull String operationName,
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxConcurrentOps,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<Traverser<R>>> flatMapAsyncFn,
            @Nonnull FunctionEx<? super T, ? extends K> partitionKeyFn
    ) {
        String name = operationName + "UsingPartitionedServiceAsync";
        ProcessorSupplier supplier = flatMapUsingServiceAsyncBatchedP(
                serviceFactory, maxConcurrentOps, maxBatchSize, flatMapAsyncFn);
        ProcessorMetaSupplier metaSupplier = ProcessorMetaSupplier.of(getPreferredLP(serviceFactory),
                serviceFactory.permission(), supplier);
        return new PartitionedProcessorTransform<>(name, upstream, metaSupplier, partitionKeyFn);
    }

    @Override
    public void addToDag(Planner p, Context context) {
        determineLocalParallelism(processorSupplier.preferredLocalParallelism(), context, p.isPreserveOrder());
        PlannerVertex pv = p.addVertex(this, name(), determinedLocalParallelism(), processorSupplier);
        p.addEdges(this, pv.v, e -> e.partitioned(partitionKeyFn).distributed());
    }
}
