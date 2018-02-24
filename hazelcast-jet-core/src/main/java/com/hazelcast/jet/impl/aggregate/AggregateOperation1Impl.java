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

package com.hazelcast.jet.impl.aggregate;

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AggregateOperation1Impl<T0, A, R>
        extends AggregateOperationImpl<A, R>
        implements AggregateOperation1<T0, A, R> {

    public AggregateOperation1Impl(@Nonnull DistributedSupplier<A> createFn,
                                   @Nonnull DistributedBiConsumer<? super A, ? super T0> accumulateFn,
                                   @Nullable DistributedBiConsumer<? super A, ? super A> combineFn,
                                   @Nullable DistributedBiConsumer<? super A, ? super A> deductFn,
                                   @Nonnull DistributedFunction<? super A, R> finishFn
    ) {
        super(createFn, accumulateFns(accumulateFn), combineFn, deductFn, finishFn);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public DistributedBiConsumer<? super A, ? super T0> accumulateFn() {
        return (DistributedBiConsumer<? super A, ? super T0>) accumulateFns[0];
    }

    @Nonnull @Override
    public <NEW_T> AggregateOperation1<NEW_T, A, R> withAccumulateFn(
            DistributedBiConsumer<? super A, ? super NEW_T> accumulateFn) {
        return new AggregateOperation1Impl<>(createFn(), accumulateFn, combineFn(), deductFn(), finishFn());
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> DistributedBiConsumer<? super A, ? super T> accumulateFn(@Nonnull Tag<T> tag) {
        if (tag.index() != 0) {
            throw new IllegalArgumentException("AggregateOperation1 recognizes only tag with index 0, but asked for "
                    + tag.index());
        }
        return (DistributedBiConsumer<? super A, ? super T>) accumulateFns[0];
    }

    @Nonnull
    @Override
    public <R1> AggregateOperation1<T0, A, R1> withFinishFn(
            @Nonnull DistributedFunction<? super A, R1> finishFn
    ) {
        return new AggregateOperation1Impl<>(
                createFn(), accumulateFn(),
                combineFn(), deductFn(), finishFn);
    }
}
