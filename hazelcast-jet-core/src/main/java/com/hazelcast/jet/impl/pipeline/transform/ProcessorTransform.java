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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBiPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.pipeline.ContextFactory;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.core.processor.Processors.filterUsingContextP;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingContextAsyncP;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingContextP;
import static com.hazelcast.jet.core.processor.Processors.mapUsingContextP;

public class ProcessorTransform extends AbstractTransform {
    final ProcessorSupplier processorSupplier;

    ProcessorTransform(
            @Nonnull String name,
            @Nonnull Transform upstream,
            @Nonnull ProcessorSupplier processorSupplier
    ) {
        super(name, upstream);
        this.processorSupplier = processorSupplier;
    }

    public static ProcessorTransform customProcessorTransform(
            @Nonnull String name,
            @Nonnull Transform upstream,
            @Nonnull DistributedSupplier<Processor> createProcessorFn
    ) {
        return new ProcessorTransform(name, upstream, ProcessorSupplier.of(createProcessorFn));
    }

    public static <C, T, R> ProcessorTransform mapUsingContextTransform(
            @Nonnull Transform upstream,
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends R> mapFn
    ) {
        return new ProcessorTransform("mapUsingContext", upstream, mapUsingContextP(contextFactory, mapFn));
    }

    public static <C, T> ProcessorTransform filterUsingContextTransform(
            @Nonnull Transform upstream,
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiPredicate<? super C, ? super T> filterFn
    ) {
        return new ProcessorTransform("filterUsingContext", upstream, filterUsingContextP(contextFactory, filterFn));
    }

    public static <C, T, R> ProcessorTransform flatMapUsingContextTransform(
            @Nonnull Transform upstream,
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return new ProcessorTransform("flatMapUsingContext", upstream,
                flatMapUsingContextP(contextFactory, flatMapFn));
    }

    public static <C, T, R> ProcessorTransform flatMapUsingContextAsyncTransform(
            @Nonnull Transform upstream,
            @Nonnull String operationName,
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        // TODO use better key so that snapshots are local. Currently they will
        //      be sent to a random member. We keep it this way for simplicity:
        //      the number of in-flight items is limited (maxAsyncOps)
        return new ProcessorTransform(operationName + "UsingContextAsync", upstream,
                flatMapUsingContextAsyncP(contextFactory, Object::hashCode, flatMapAsyncFn));
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, p.uniqueVertexName(name()), localParallelism(), processorSupplier);
        p.addEdges(this, pv.v);
    }
}
