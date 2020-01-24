/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static com.hazelcast.jet.core.processor.Processors.filterUsingServiceP;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingServiceAsyncP;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingServiceP;
import static com.hazelcast.jet.core.processor.Processors.mapUsingServiceP;

public class ProcessorTransform extends AbstractTransform {
    public static final int NON_COOPERATIVE_DEFAULT_LOCAL_PARALLELISM = 2;

    final ProcessorMetaSupplier processorSupplier;

    ProcessorTransform(
            @Nonnull String name,
            @Nonnull Transform upstream,
            @Nonnull ProcessorMetaSupplier processorSupplier
    ) {
        super(name, upstream);
        this.processorSupplier = processorSupplier;
    }

    public static ProcessorTransform customProcessorTransform(
            @Nonnull String name,
            @Nonnull Transform upstream,
            @Nonnull ProcessorMetaSupplier createProcessorFn
    ) {
        return new ProcessorTransform(name, upstream, createProcessorFn);
    }

    public static <S, T, R> ProcessorTransform mapUsingServiceTransform(
            @Nonnull Transform upstream,
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        return new ProcessorTransform("mapUsingService", upstream,
                ProcessorMetaSupplier.of(getPreferredLP(serviceFactory), mapUsingServiceP(serviceFactory, mapFn)));
    }

    public static <S, T> ProcessorTransform filterUsingServiceTransform(
            @Nonnull Transform upstream,
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        return new ProcessorTransform("filterUsingService", upstream,
                ProcessorMetaSupplier.of(getPreferredLP(serviceFactory), filterUsingServiceP(serviceFactory, filterFn)));
    }

    public static <S, T, R> ProcessorTransform flatMapUsingServiceTransform(
            @Nonnull Transform upstream,
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return new ProcessorTransform("flatMapUsingService", upstream,
                ProcessorMetaSupplier.of(getPreferredLP(serviceFactory), flatMapUsingServiceP(serviceFactory, flatMapFn)));
    }

    public static <S, T, R> ProcessorTransform flatMapUsingServiceAsyncTransform(
            @Nonnull Transform upstream,
            @Nonnull String operationName,
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<Traverser<R>>> flatMapAsyncFn
    ) {
        // TODO use better key so that snapshots are local. Currently they will
        //      be sent to a random member. We keep it this way for simplicity:
        //      the number of in-flight items is limited (maxAsyncOps)
        return new ProcessorTransform(operationName + "UsingServiceAsync", upstream,
                ProcessorMetaSupplier.of(getPreferredLP(serviceFactory),
                        flatMapUsingServiceAsyncP(serviceFactory, Object::hashCode, flatMapAsyncFn)));
    }

    static <S> int getPreferredLP(@Nonnull ServiceFactory<?, S> serviceFactory) {
        return serviceFactory.isCooperative() ? LOCAL_PARALLELISM_USE_DEFAULT : NON_COOPERATIVE_DEFAULT_LOCAL_PARALLELISM;
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, name(), localParallelism(), processorSupplier);
        p.addEdges(this, pv.v);
    }
}
