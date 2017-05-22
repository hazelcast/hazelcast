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

package com.hazelcast.jet;

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.AggregateOperationImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;

/**
 * Contains primitives needed to compute an aggregated result of
 * stream processing. The result is computed by maintaining a mutable
 * result container, called the <em>accumulator</em>, which is transformed
 * to the final result at the end of accumulation. These are the
 * primitives:
 * <ol><li>
 *     {@link #createAccumulatorF() create} a new accumulator
 * </li><li>
 *     {@link #accumulateItemF() accumulate} the data of an item
 * </li><li>
 *     {@link #combineAccumulatorsF() combine} the contents of one accumulator
 *     into another one
 * </li><li>
 *     {@link #deductAccumulatorF() deduct} the contents of one accumulator
 *     from another (undo the effects of {@code combine})
 * </li><li>
 *     {@link #finishAccumulationF() finish} accumulation by transforming the
 *     accumulator's intermediate result into the final result
 * </li></ol>
 * The <em>deduct</em> primitive is optional. It is used in sliding window
 * aggregation, where it can significantly improve the performance.
 *
 * @param <T> the type of the stream item &mdash; contravariant
 * @param <A> the type of the accumulator &mdash; invariant
 * @param <R> the type of the final result &mdash; covariant
 */
public interface AggregateOperation<T, A, R> extends Serializable {

    /**
     * A primitive that returns a new accumulator. If the {@code deduct}
     * operation is defined, the accumulator object must properly implement
     * {@code equals()}, which will be used to detect when an accumulator is
     * "empty" (i.e., equal to a fresh instance returned from this method) and
     * can be evicted from a processor's storage.
     */
    @Nonnull
    DistributedSupplier<A> createAccumulatorF();

    /**
     * A primitive that updates the accumulator state to account for a new item.
     */
    @Nonnull
    DistributedBiConsumer<A, T> accumulateItemF();

    /**
     * A primitive that accepts two accumulators and updates the state of the
     * left-hand one by combining it with the state of the right-hand one.
     * The right-hand accumulator remains unchanged.
     */
    @Nonnull
    DistributedBiConsumer<A, A> combineAccumulatorsF();

    /**
     * A primitive that accepts two accumulators and updates the state of the
     * left-hand one by deducting the state of the right-hand one from it. The
     * right-hand accumulator remains unchanged.
     * <p>
     * The effect of this primitive must be the opposite of {@link
     * #combineAccumulatorsF() combine} so that
     * <pre>
     *     combine(acc, x);
     *     deduct(acc, x);
     * </pre>
     * leaves {@code acc} in the same state as it was before the two
     * operations.
     * <p>
     * <strong>Note:</strong> this method may return {@code null} because the
     * <em>deduct</em> primitive is optional. However, when this aggregate
     * operation is used to compute a sliding window, its presence may
     * significantly reduce the computational cost. With it, the current
     * sliding window can be obtained from the previous one by deducting the
     * trailing frame and combining the leading frame; without it, each window
     * must be recomputed from all its constituent frames. The finer the sliding
     * step, the more pronounced the difference in computation effort will be.
     */
    @Nullable
    DistributedBiConsumer<A, A> deductAccumulatorF();

    /**
     * A primitive that finishes the accumulation process by transforming
     * the accumulator object into the final result.
     */
    @Nonnull
    DistributedFunction<A, R> finishAccumulationF();

    /**
     * Returns a new {@code AggregateOperation} object composed from the provided
     * primitives.
     *
     * @param <T> the type of the stream item
     * @param <A> the type of the accumulator object
     * @param <R> the type of the final result
     *
     * @param createAccumulatorF see {@link #createAccumulatorF()}()
     * @param accumulateItemF see {@link #accumulateItemF()}
     * @param combineAccumulatorsF see {@link #combineAccumulatorsF()}
     * @param deductAccumulatorF see {@link #deductAccumulatorF()}
     * @param finishAccumulationF see {@link #finishAccumulationF()}
     */
    @Nonnull
    static <T, A, R> AggregateOperation<T, A, R> of(
            @Nonnull DistributedSupplier<A> createAccumulatorF,
            @Nonnull DistributedBiConsumer<A, T> accumulateItemF,
            @Nonnull DistributedBiConsumer<A, A> combineAccumulatorsF,
            @Nullable DistributedBiConsumer<A, A> deductAccumulatorF,
            @Nonnull DistributedFunction<A, R> finishAccumulationF
    ) {
        Objects.requireNonNull(createAccumulatorF);
        Objects.requireNonNull(accumulateItemF);
        Objects.requireNonNull(combineAccumulatorsF);
        Objects.requireNonNull(finishAccumulationF);
        return new AggregateOperationImpl<>(
                createAccumulatorF, accumulateItemF, combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
    }
}
