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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBiPredicate;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithGrouping;
import com.hazelcast.jet.pipeline.WindowDefinition;

import javax.annotation.Nonnull;

public class StreamStageWithGroupingImpl<T, K>
        extends StageWithGroupingBase<T, K>
        implements StreamStageWithGrouping<T, K> {
    StreamStageWithGroupingImpl(
            @Nonnull StreamStageImpl<T> computeStage,
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn
    ) {
        super(computeStage, keyFn);
    }

    @Nonnull @Override
    public StageWithGroupingAndWindowImpl<T, K> window(@Nonnull WindowDefinition wDef) {
        return new StageWithGroupingAndWindowImpl<>((StreamStageImpl<T>) computeStage, keyFn(), wDef);
    }

    @Nonnull @Override
    public <C, R> StreamStage<R> mapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends R> mapFn
    ) {
        return computeStage.attachMapUsingKeyedContext(contextFactory, keyFn(), mapFn);
    }

    @Nonnull @Override
    public <C> StreamStage<T> filterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiPredicate<? super C, ? super T> filterFn
    ) {
        return computeStage.attachFilterUsingKeyedContext(contextFactory, keyFn(), filterFn);
    }

    @Nonnull @Override
    public <C, R> StreamStage<R> flatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return computeStage.attachFlatMapUsingKeyedContext(contextFactory, keyFn(), flatMapFn);
    }
}
