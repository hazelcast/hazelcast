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
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedTriFunction;
import com.hazelcast.jet.function.DistributedTriPredicate;
import com.hazelcast.jet.impl.pipeline.transform.Transform;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.GeneralStageWithKey;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;

class StageWithGroupingBase<T, K> {

    final ComputeStageImplBase<T> computeStage;
    private final DistributedFunction<? super T, ? extends K> keyFn;

    StageWithGroupingBase(
            @Nonnull ComputeStageImplBase<T> computeStage,
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn
    ) {
        checkSerializable(keyFn, "keyFn");
        this.computeStage = computeStage;
        this.keyFn = keyFn;
    }

    @Nonnull
    public DistributedFunction<? super T, ? extends K> keyFn() {
        return keyFn;
    }

    @Nonnull
    <C, R, RET> RET attachMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriFunction<? super C, ? super K, ? super T, ? extends R> mapFn
    ) {
        DistributedFunction<? super T, ? extends K> keyFn = keyFn();
        return computeStage.attachMapUsingPartitionedContext(contextFactory, keyFn, (c, t) -> {
            K k = keyFn.apply(t);
            return mapFn.apply(c, k, t);
        });
    }

    @Nonnull
    <C, RET> RET attachFilterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriPredicate<? super C, ? super K, ? super T> filterFn
    ) {
        DistributedFunction<? super T, ? extends K> keyFn = keyFn();
        return computeStage.attachFilterUsingPartitionedContext(contextFactory, keyFn, (c, t) -> {
            K k = keyFn.apply(t);
            return filterFn.test(c, k, t);
        });
    }

    @Nonnull
    public <C, R, RET> RET attachFlatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedTriFunction<? super C, ? super K, ? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        DistributedFunction<? super T, ? extends K> keyFn = keyFn();
        return computeStage.attachFlatMapUsingPartitionedContext(contextFactory, keyFn, (c, t) -> {
            K k = keyFn.apply(t);
            return flatMapFn.apply(c, k, t);
        });
    }

    static Transform transformOf(GeneralStageWithKey stage) {
        return ((StageWithGroupingBase) stage).computeStage.transform;
    }
}
