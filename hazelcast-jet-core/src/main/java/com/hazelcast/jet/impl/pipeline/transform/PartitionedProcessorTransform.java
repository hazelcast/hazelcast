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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBiPredicate;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.pipeline.ContextFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.core.processor.Processors.filterUsingContextP;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingContextAsyncP;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingContextP;
import static com.hazelcast.jet.core.processor.Processors.mapUsingContextP;

public final class PartitionedProcessorTransform<T, K> extends ProcessorTransform {

    private final DistributedFunction<? super T, ? extends K> partitionKeyFn;

    private PartitionedProcessorTransform(
            @Nonnull String name,
            @Nonnull Transform upstream,
            @Nonnull ProcessorMetaSupplier processorSupplier,
            @Nonnull DistributedFunction<? super T, ? extends K> partitionKeyFn
    ) {
        super(name, upstream, processorSupplier);
        this.partitionKeyFn = partitionKeyFn;
    }

    public static <T, K> PartitionedProcessorTransform<T, K> partitionedCustomProcessorTransform(
            @Nonnull String name,
            @Nonnull Transform upstream,
            @Nonnull ProcessorMetaSupplier processorSupplier,
            @Nonnull DistributedFunction<? super T, ? extends K> partitionKeyFn
    ) {
        return new PartitionedProcessorTransform<>(name, upstream, processorSupplier, partitionKeyFn);
    }

    public static <C, T, K, R> PartitionedProcessorTransform<T, K> mapUsingContextPartitionedTransform(
            @Nonnull Transform upstream,
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends R> mapFn,
            @Nonnull DistributedFunction<? super T, ? extends K> partitionKeyFn
    ) {
        return new PartitionedProcessorTransform<>("mapUsingPartitionedContext",
                upstream, ProcessorMetaSupplier.of(mapUsingContextP(contextFactory, mapFn)), partitionKeyFn);
    }

    public static <C, T, K> PartitionedProcessorTransform<T, K> filterUsingPartitionedContextTransform(
            @Nonnull Transform upstream,
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiPredicate<? super C, ? super T> filterFn,
            @Nonnull DistributedFunction<? super T, ? extends K> partitionKeyFn
    ) {
        return new PartitionedProcessorTransform<>("filterUsingPartitionedContext",
                upstream, ProcessorMetaSupplier.of(filterUsingContextP(contextFactory, filterFn)), partitionKeyFn);
    }

    public static <C, T, K, R> PartitionedProcessorTransform<T, K> flatMapUsingPartitionedContextTransform(
            @Nonnull Transform upstream,
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn,
            @Nonnull DistributedFunction<? super T, ? extends K> partitionKeyFn
    ) {
        return new PartitionedProcessorTransform<>("flatMapUsingPartitionedContext",
                upstream, ProcessorMetaSupplier.of(flatMapUsingContextP(contextFactory, flatMapFn)), partitionKeyFn);
    }

    public static <C, T, K, R> PartitionedProcessorTransform<T, K> flatMapUsingPartitionedContextAsyncTransform(
            @Nonnull Transform upstream,
            @Nonnull String operationName,
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, CompletableFuture<Traverser<R>>> flatMapAsyncFn,
            @Nonnull DistributedFunction<? super T, ? extends K> partitionKeyFn
    ) {
        return new PartitionedProcessorTransform<>(operationName + "UsingPartitionedContextAsync", upstream,
                ProcessorMetaSupplier.of(flatMapUsingContextAsyncP(contextFactory, partitionKeyFn, flatMapAsyncFn)),
                partitionKeyFn);
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, p.uniqueVertexName(name()), localParallelism(), processorSupplier);
        p.addEdges(this, pv.v, e -> e.partitioned(partitionKeyFn).distributed());
    }
}
