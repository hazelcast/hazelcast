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

package com.hazelcast.jet.aggregate;

import com.hazelcast.jet.function.DistributedBiConsumer;

import javax.annotation.Nonnull;

/**
 * Specialization of {@link AggregateOperation} to the "arity-2" case with
 * two data streams being aggregated over. This example constructs an operation
 * that sums up {@code long} values from two streams:
 *
 * <pre>{@code
 * AggregateOperation2<Long, Long, LongAccumulator, Long> aggrOp = AggregateOperation
 *     .withCreate(LongAccumulator::new)
 *     .<Long>andAccumulate0(LongAccumulator::add)
 *     .<Long>andAccumulate1(LongAccumulator::add)
 *     .andFinish(LongAccumulator::get);
 * }</pre>
 *
 * @param <T0> the type of item in stream-0
 * @param <T1> the type of item in stream-1
 * @param <A> the type of the accumulator
 * @param <R> the type of the aggregation result
 */
public interface AggregateOperation2<T0, T1, A, R> extends AggregateOperation<A, R> {

    /**
     * A primitive that updates the accumulator state to account for a new
     * item coming from stream-0.
     */
    @Nonnull
    DistributedBiConsumer<? super A, ? super T0> accumulateFn0();

    /**
     * A primitive that updates the accumulator state to account for a new
     * item coming from stream-1.
     */
    @Nonnull
    DistributedBiConsumer<? super A, ? super T1> accumulateFn1();

    /**
     * Returns a copy of this aggregate operation, but with the {@code
     * accumulate} primitive at index 0 replaced with the one supplied here.
     */
    @Nonnull
    <T0_NEW> AggregateOperation2<T0_NEW, T1, A, R> withAccumulateFn0(
            @Nonnull DistributedBiConsumer<? super A, ? super T0_NEW> newAccFn0
    );

    /**
     * Returns a copy of this aggregate operation, but with the {@code
     * accumulate} primitive at index 1 replaced with the one supplied here.
     */
    @Nonnull
    <T1_NEW> AggregateOperation2<T0, T1_NEW, A, R> withAccumulateFn1(
            @Nonnull DistributedBiConsumer<? super A, ? super T1_NEW> newAccFn1
    );

    // Narrows the return type
    @Nonnull @Override
    AggregateOperation2<T0, T1, A, A> withIdentityFinish();
}
