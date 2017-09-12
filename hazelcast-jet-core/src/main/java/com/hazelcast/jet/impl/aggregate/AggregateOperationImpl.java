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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class AggregateOperationImpl<A, R> implements AggregateOperation<A, R> {
    final DistributedBiConsumer<? super A, ?>[] accumulateFs;
    private final DistributedSupplier<A> createAccumulatorFn;
    private final DistributedBiConsumer<? super A, ? super A> combineAccumulatorsFn;
    private final DistributedBiConsumer<? super A, ? super A> deductAccumulatorFn;
    private final DistributedFunction<? super A, R> finishAccumulationFn;

    public AggregateOperationImpl(
            @Nonnull DistributedSupplier<A> createAccumulatorFn,
            @Nonnull DistributedBiConsumer<? super A, ?>[] accumulateFs,
            @Nullable DistributedBiConsumer<? super A, ? super A> combineAccumulatorsFn,
            @Nullable DistributedBiConsumer<? super A, ? super A> deductAccumulatorFn,
            @Nonnull DistributedFunction<? super A, R> finishAccumulationFn
    ) {
        for (Object f : accumulateFs) {
            checkNotNull(f, "accumulateFs array contains a null slot");
        }
        this.createAccumulatorFn = createAccumulatorFn;
        this.accumulateFs = accumulateFs.clone();
        this.combineAccumulatorsFn = combineAccumulatorsFn;
        this.deductAccumulatorFn = deductAccumulatorFn;
        this.finishAccumulationFn = finishAccumulationFn;
    }

    @Nonnull
    public DistributedSupplier<A> createFn() {
        return createAccumulatorFn;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> DistributedBiConsumer<? super A, ? super T> accumulateFn(int index) {
        if (index >= accumulateFs.length) {
            throw new IllegalArgumentException("This AggregateOperation has " + accumulateFs.length
                    + " accumulating functions, but was asked for function at index " + index);
        }
        return (DistributedBiConsumer<? super A, T>) accumulateFs[index];
    }

    @Nullable
    public DistributedBiConsumer<? super A, ? super A> combineFn() {
        return combineAccumulatorsFn;
    }

    @Nullable
    public DistributedBiConsumer<? super A, ? super A> deductFn() {
        return deductAccumulatorFn;
    }

    @Nonnull
    public DistributedFunction<? super A, R> finishFn() {
        return finishAccumulationFn;
    }

    @Nonnull @Override
    public AggregateOperation<A, R> withAccumulateFns(
            @Nonnull DistributedBiConsumer<? super A, ?>[] accumulateFns
    ) {
        return new AggregateOperationImpl<>(createFn(), accumulateFns, combineFn(),
                deductFn(), finishFn());
    }

    @Override
    public <R1> AggregateOperation<A, R1> withFinishFn(
            @Nonnull DistributedFunction<? super A, R1> finishFn
    ) {
        return new AggregateOperationImpl<>(createFn(), accumulateFs, combineFn(),
                deductFn(), finishFn);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    static <A> DistributedBiConsumer<? super A, ?>[] accumulateFs(DistributedBiConsumer... accFs) {
        return (DistributedBiConsumer<? super A, ?>[]) accFs;
    }
}
