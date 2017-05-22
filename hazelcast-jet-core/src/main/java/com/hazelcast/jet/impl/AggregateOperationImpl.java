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

package com.hazelcast.jet.impl;

import com.hazelcast.jet.AggregateOperation;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class AggregateOperationImpl<T, A, R> implements AggregateOperation<T, A, R> {
    private final DistributedSupplier<A> createAccumulatorF;
    private final DistributedBiConsumer<? super A, T> accumulateItemF;
    private final DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF;
    private final DistributedBiConsumer<? super A, ? super A> deductAccumulatorF;
    private final DistributedFunction<? super A, R> finishAccumulationF;

    public AggregateOperationImpl(
            @Nonnull DistributedSupplier<A> createAccumulatorF,
            @Nonnull DistributedBiConsumer<? super A, T> accumulateItemF,
            @Nonnull DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF,
            @Nullable DistributedBiConsumer<? super A, ? super A> deductAccumulatorF,
            @Nonnull DistributedFunction<? super A, R> finishAccumulationF
    ) {
        this.createAccumulatorF = createAccumulatorF;
        this.accumulateItemF = accumulateItemF;
        this.combineAccumulatorsF = combineAccumulatorsF;
        this.deductAccumulatorF = deductAccumulatorF;
        this.finishAccumulationF = finishAccumulationF;
    }

    @Override @Nonnull
    public DistributedSupplier<A> createAccumulatorF() {
        return createAccumulatorF;
    }

    @Override @Nonnull
    public DistributedBiConsumer<? super A, T> accumulateItemF() {
        return accumulateItemF;
    }

    @Override @Nonnull
    public DistributedBiConsumer<? super A, ? super A> combineAccumulatorsF() {
        return combineAccumulatorsF;
    }

    @Override @Nullable
    public DistributedBiConsumer<? super A, ? super A> deductAccumulatorF() {
        return deductAccumulatorF;
    }

    @Override @Nonnull
    public DistributedFunction<? super A, R> finishAccumulationF() {
        return finishAccumulationF;
    }
}
