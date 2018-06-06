/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.pipeline.ContextFactory;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.processor.Processors.flatMapUsingKeyedContextP;

public class FlatMapUsingKeyedContextTransform<C, T, K, R> extends AbstractTransform {
    private final ContextFactory<C> contextFactory;
    private final DistributedFunction<? super T, ? extends K> keyFn;
    private final DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn;

    public FlatMapUsingKeyedContextTransform(
            @Nonnull Transform upstream,
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        super("keyed-map", upstream);
        this.contextFactory = contextFactory;
        this.keyFn = keyFn;
        this.flatMapFn = flatMapFn;
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, p.uniqueVertexName(name(), ""), localParallelism(),
                flatMapUsingKeyedContextP(contextFactory, keyFn, flatMapFn));
        p.addEdges(this, pv.v, edge -> edge.partitioned(keyFn).distributed());
    }
}
