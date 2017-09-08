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
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.pipeline.datamodel.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Javadoc pending.
 */
public class AggregateOperation2Impl<T0, T1, A, R>
        extends AggregateOperationImpl<A, R>
        implements AggregateOperation2<T0, T1, A, R> {

    public AggregateOperation2Impl(@Nonnull DistributedSupplier<A> createAccumulatorF,
                                   @Nonnull DistributedBiConsumer<? super A, T0> accumulateItemF0,
                                   @Nonnull DistributedBiConsumer<? super A, T1> accumulateItemF1,
                                   @Nullable DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF,
                                   @Nullable DistributedBiConsumer<? super A, ? super A> deductAccumulatorF,
                                   @Nonnull DistributedFunction<? super A, R> finishAccumulationF
    ) {
        super(createAccumulatorF, accumulateFs(accumulateItemF0, accumulateItemF1),
                combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
    }

    private AggregateOperation2Impl(@Nonnull DistributedSupplier<A> createAccumulatorF,
                                    @Nonnull DistributedBiConsumer<? super A, ?>[] accumulateFs,
                                    @Nullable DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF,
                                    @Nullable DistributedBiConsumer<? super A, ? super A> deductAccumulatorF,
                                    @Nonnull DistributedFunction<? super A, R> finishAccumulationF
    ) {
        super(createAccumulatorF, accumulateFs, combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
        validateCountOfAccumulateFs(accumulateFs);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public DistributedBiConsumer<? super A, ? super T0> accumulateItemF0() {
        return (DistributedBiConsumer<? super A, ? super T0>) accumulateFs[0];
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public DistributedBiConsumer<? super A, ? super T1> accumulateItemF1() {
        return (DistributedBiConsumer<? super A, ? super T1>) accumulateFs[1];
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> DistributedBiConsumer<? super A, T> accumulateItemF(Tag<T> tag) {
        if (tag.index() > 1) {
            throw new IllegalArgumentException(
                    "AggregateOperation2 only recognizes tags with index 0 and 1, but asked for " + tag.index());
        }
        return (DistributedBiConsumer<? super A, T>) accumulateFs[tag.index()];
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public AggregateOperation<A, R> withAccumulateItemFs(
            @Nonnull DistributedBiConsumer<? super A, ?>[] accumulateFs
    ) {
        validateCountOfAccumulateFs(accumulateFs);
        return new AggregateOperation2Impl<>(createAccumulatorF(), accumulateFs, combineAccumulatorsF(),
                deductAccumulatorF(), finishAccumulationF());
    }

    @Override
    public <R1> AggregateOperation2<T0, T1, A, R1> withFinish(
            @Nonnull DistributedFunction<? super A, R1> finishAccumulationF
    ) {
        return new AggregateOperation2Impl<>(createAccumulatorF(), accumulateFs, combineAccumulatorsF(),
                deductAccumulatorF(), finishAccumulationF);
    }

    private static void validateCountOfAccumulateFs(@Nonnull DistributedBiConsumer[] accumulateFs) {
        if (accumulateFs.length != 2) {
            throw new IllegalArgumentException("AggregateOperationImpl2 needs exactly two accumulating functions," +
                    " but got " + accumulateFs.length);
        }
    }
}
