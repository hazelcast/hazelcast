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
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.pipeline.datamodel.Tag;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Javadoc pending.
 */
public class AggregateOperation1Impl<T0, A, R>
        extends AggregateOperationImpl<A, R>
        implements AggregateOperation1<T0, A, R> {

    public AggregateOperation1Impl(@Nonnull DistributedSupplier<A> createAccumulatorF,
                                   @Nonnull DistributedBiConsumer<? super A, ? super T0> accumulateItemF,
                                   @Nullable DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF,
                                   @Nullable DistributedBiConsumer<? super A, ? super A> deductAccumulatorF,
                                   @Nonnull DistributedFunction<? super A, R> finishAccumulationF
    ) {
        super(createAccumulatorF, accumulateFs(accumulateItemF), combineAccumulatorsF,
                deductAccumulatorF, finishAccumulationF);
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public DistributedBiConsumer<? super A, ? super T0> accumulateItemF() {
        return (DistributedBiConsumer<? super A, ? super T0>) accumulateFs[0];
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public <T> DistributedBiConsumer<? super A, ? super T> accumulateItemF(Tag<T> tag) {
        if (tag.index() != 0) {
            throw new IllegalArgumentException("AggregateOperation1 recognizes only tag with index 0, but asked for "
                    + tag.index());
        }
        return (DistributedBiConsumer<? super A, ? super T>) accumulateFs[0];
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public AggregateOperation<A, R> withAccumulateItemFs(
            @Nonnull DistributedBiConsumer<? super A, ?>[] accumulateFs
    ) {
        if (accumulateFs.length != 1) {
            throw new IllegalArgumentException(
                    "AggregateOperationImpl1 needs exactly one accumulating function, but got " + accumulateFs.length);
        }
        return new AggregateOperation1Impl<>(
                createAccumulatorF(), (DistributedBiConsumer<? super A, ? super T0>) accumulateFs[0],
                combineAccumulatorsF(), deductAccumulatorF(), finishAccumulationF());
    }

    @Nonnull @Override
    public <T_NEW> AggregateOperation1<T_NEW, A, R> withAccumulateItemF(
            DistributedBiConsumer<? super A, ? super T_NEW> accumulateItemF
    ) {
        return new AggregateOperation1Impl<>(
                createAccumulatorF(), accumulateItemF,
                combineAccumulatorsF(), deductAccumulatorF(), finishAccumulationF());
    }

    @Override
    public <R1> AggregateOperation1<T0, A, R1> withFinish(
            @Nonnull DistributedFunction<? super A, R1> finishAccumulationF
    ) {
        return new AggregateOperation1Impl<>(
                createAccumulatorF(), accumulateItemF(),
                combineAccumulatorsF(), deductAccumulatorF(), finishAccumulationF);
    }
}
