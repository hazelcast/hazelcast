/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AggregateOperation2Impl<T0, T1, A, R>
        extends AggregateOperationImpl<A, R>
        implements AggregateOperation2<T0, T1, A, R> {

    public AggregateOperation2Impl(@Nonnull DistributedSupplier<A> createAccumulatorFn,
                                   @Nonnull DistributedBiConsumer<? super A, T0> accumulateItemF0,
                                   @Nonnull DistributedBiConsumer<? super A, T1> accumulateItemF1,
                                   @Nullable DistributedBiConsumer<? super A, ? super A> combineAccumulatorsFn,
                                   @Nullable DistributedBiConsumer<? super A, ? super A> deductAccumulatorFn,
                                   @Nonnull DistributedFunction<? super A, R> finishAccumulationFn
    ) {
        super(createAccumulatorFn, accumulateFs(accumulateItemF0, accumulateItemF1),
                combineAccumulatorsFn, deductAccumulatorFn, finishAccumulationFn);
    }

    private AggregateOperation2Impl(@Nonnull DistributedSupplier<A> createAccumulatorFn,
                                    @Nonnull DistributedBiConsumer<? super A, ?>[] accumulateFs,
                                    @Nullable DistributedBiConsumer<? super A, ? super A> combineAccumulatorsFn,
                                    @Nullable DistributedBiConsumer<? super A, ? super A> deductAccumulatorFn,
                                    @Nonnull DistributedFunction<? super A, R> finishAccumulationFn
    ) {
        super(createAccumulatorFn, accumulateFs, combineAccumulatorsFn, deductAccumulatorFn, finishAccumulationFn);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public DistributedBiConsumer<? super A, ? super T0> accumulateFn0() {
        return (DistributedBiConsumer<? super A, ? super T0>) accumulateFs[0];
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public DistributedBiConsumer<? super A, ? super T1> accumulateFn1() {
        return (DistributedBiConsumer<? super A, ? super T1>) accumulateFs[1];
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> DistributedBiConsumer<? super A, T> accumulateFn(Tag<T> tag) {
        if (tag.index() > 1) {
            throw new IllegalArgumentException(
                    "AggregateOperation2 only recognizes tags with index 0 and 1, but asked for " + tag.index());
        }
        return (DistributedBiConsumer<? super A, T>) accumulateFs[tag.index()];
    }

    @Override
    public <R1> AggregateOperation2<T0, T1, A, R1> withFinishFn(
            @Nonnull DistributedFunction<? super A, R1> finishFn
    ) {
        return new AggregateOperation2Impl<>(createFn(), accumulateFs, combineFn(), deductFn(), finishFn);
    }
}
