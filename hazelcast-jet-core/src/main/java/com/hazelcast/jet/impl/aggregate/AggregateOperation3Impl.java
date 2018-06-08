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

import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;

public class AggregateOperation3Impl<T0, T1, T2, A, R>
        extends AggregateOperationImpl<A, R>
        implements AggregateOperation3<T0, T1, T2, A, R> {

    public AggregateOperation3Impl(@Nonnull DistributedSupplier<A> createAccumulatorFn,
                                   @Nonnull DistributedBiConsumer<? super A, ? super T0> accumulateItemF0,
                                   @Nonnull DistributedBiConsumer<? super A, ? super T1> accumulateItemF1,
                                   @Nonnull DistributedBiConsumer<? super A, ? super T2> accumulateItemF2,
                                   @Nullable DistributedBiConsumer<? super A, ? super A> combineAccumulatorsFn,
                                   @Nullable DistributedBiConsumer<? super A, ? super A> deductAccumulatorFn,
                                   @Nonnull DistributedFunction<? super A, R> finishAccumulationFn
    ) {
        super(createAccumulatorFn, accumulateFns(accumulateItemF0, accumulateItemF1, accumulateItemF2),
                combineAccumulatorsFn, deductAccumulatorFn, finishAccumulationFn);
    }

    private AggregateOperation3Impl(@Nonnull DistributedSupplier<A> createAccumulatorFn,
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
        return (DistributedBiConsumer<? super A, ? super T0>) accumulateFns[0];
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public DistributedBiConsumer<? super A, ? super T1> accumulateFn1() {
        return (DistributedBiConsumer<? super A, ? super T1>) accumulateFns[1];
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public DistributedBiConsumer<? super A, ? super T2> accumulateFn2() {
        return (DistributedBiConsumer<? super A, ? super T2>) accumulateFns[2];
    }

    @Nonnull @Override
    public <T0_NEW> AggregateOperation3<T0_NEW, T1, T2, A, R> withAccumulateFn0(
            @Nonnull DistributedBiConsumer<? super A, ? super T0_NEW> newAccFn0
    ) {
        checkSerializable(newAccFn0, "newAccFn0");
        return new AggregateOperation3Impl<>(
                createFn(), newAccFn0, accumulateFn1(), accumulateFn2(), combineFn(), deductFn(), finishFn());
    }

    @Nonnull @Override
    public <T1_NEW> AggregateOperation3<T0, T1_NEW, T2, A, R> withAccumulateFn1(
            @Nonnull DistributedBiConsumer<? super A, ? super T1_NEW> newAccFn1
    ) {
        checkSerializable(newAccFn1, "newAccFn1");
        return new AggregateOperation3Impl<>(
                createFn(), accumulateFn0(), newAccFn1, accumulateFn2(), combineFn(), deductFn(), finishFn());
    }

    @Nonnull @Override
    public <T2_NEW> AggregateOperation3<T0, T1, T2_NEW, A, R> withAccumulateFn2(
            @Nonnull DistributedBiConsumer<? super A, ? super T2_NEW> newAccFn2
    ) {
        checkSerializable(newAccFn2, "newAccFn2");
        return new AggregateOperation3Impl<>(
                createFn(), accumulateFn0(), accumulateFn1(), newAccFn2, combineFn(), deductFn(), finishFn());
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> DistributedBiConsumer<? super A, T> accumulateFn(Tag<T> tag) {
        if (tag.index() > 2) {
            throw new IllegalArgumentException(
                    "AggregateOperation3 only recognizes tags with index 0, 1 and 2, but asked for " + tag.index());
        }
        return (DistributedBiConsumer<? super A, T>) accumulateFns[tag.index()];
    }

    @Nonnull
    @Override
    public <R1> AggregateOperation3<T0, T1, T2, A, R1> withFinishFn(
            @Nonnull DistributedFunction<? super A, R1> finishFn
    ) {
        checkSerializable(finishFn, "finishFn");
        return new AggregateOperation3Impl<>(createFn(), accumulateFns, combineFn(), deductFn(), finishFn);
    }
}
