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
    private final DistributedSupplier<A> createAccumulatorF;
    private final DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;
    private final DistributedBiConsumer<? super A, ? super A> deductAccumulatorF;
    private final DistributedFunction<? super A, R> finishAccumulationF;

    public AggregateOperationImpl(
            @Nonnull DistributedSupplier<A> createAccumulatorF,
            @Nonnull DistributedBiConsumer<? super A, ?>[] accumulateFs,
            @Nullable DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF,
            @Nullable DistributedBiConsumer<? super A, ? super A> deductAccumulatorF,
            @Nonnull DistributedFunction<? super A, R> finishAccumulationF
    ) {
        for (Object f : accumulateFs) {
            checkNotNull(f, "accumulateFs array contains a null slot");
        }
        this.createAccumulatorF = createAccumulatorF;
        this.accumulateFs = accumulateFs.clone();
        this.combineAccumulatorsF = combineAccumulatorsF;
        this.deductAccumulatorF = deductAccumulatorF;
        this.finishAccumulationF = finishAccumulationF;
    }

    @Nonnull
    public DistributedSupplier<A> createAccumulatorF() {
        return createAccumulatorF;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> DistributedBiConsumer<? super A, ? super T> accumulateItemF(int index) {
        if (index >= accumulateFs.length) {
            throw new IllegalArgumentException("This AggregateOperation has " + accumulateFs.length
                    + " accumulating functions, but was asked for function at index " + index);
        }
        return (DistributedBiConsumer<? super A, T>) accumulateFs[index];
    }

    @Nullable
    public DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF() {
        return combineAccumulatorsF;
    }

    @Nullable
    public DistributedBiConsumer<? super A, ? super A> deductAccumulatorF() {
        return deductAccumulatorF;
    }

    @Nonnull
    public DistributedFunction<? super A, R> finishAccumulationF() {
        return finishAccumulationF;
    }

    @Nonnull @Override
    public AggregateOperation<A, R> withAccumulateItemFs(
            @Nonnull DistributedBiConsumer<? super A, ?>[] accumulateFs
    ) {
        return new AggregateOperationImpl<>(createAccumulatorF(), accumulateFs, combineAccumulatorsF(),
                deductAccumulatorF(), finishAccumulationF());
    }

    @Override
    public <R1> AggregateOperation<A, R1> withFinish(
            @Nonnull DistributedFunction<? super A, R1> finishAccumulationF
    ) {
        return new AggregateOperationImpl<>(createAccumulatorF(), accumulateFs, combineAccumulatorsF(),
                deductAccumulatorF(), finishAccumulationF);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    static <A> DistributedBiConsumer<? super A, ?>[] accumulateFs(DistributedBiConsumer... accFs) {
        return (DistributedBiConsumer<? super A, ?>[]) accFs;
    }
}
