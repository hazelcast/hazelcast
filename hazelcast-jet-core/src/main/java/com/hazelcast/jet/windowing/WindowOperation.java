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

package com.hazelcast.jet.windowing;

import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;

/**
 * Contains primitives needed to compute a windowed result of infinite
 * stream processing. The result is computed by maintaining a mutable
 * result container, called the <em>accumulator</em>, which is transformed
 * to the final result at the end of accumulation. These are the
 * primitives:
 * <ol><li>
 *     {@link #createAccumulatorF() create} a new accumulator
 * </li><li>
 *     {@link #accumulateItemF() accumulate} the data of an item
 * </li><li>
 *     {@link #combineAccumulatorsF() combine} the contents of two accumulator
 *     objects
 * </li><li>
 *     {@link #deductAccumulatorF() deduct} the contents of one accumulator
 *     from another (undo the effects of {@code combine})
 * </li><li>
 *     {@link #finishAccumulationF() finish} accumulation by transforming the
 *     accumulator's intermediate result into the final result
 * </li></ol>
 *
 * @param <T> the type of the stream item &mdash; contravariant
 * @param <A> the type of the accumulator &mdash; invariant
 * @param <R> the type of the final result &mdash; covariant
 */
public interface WindowOperation<T, A, R> extends Serializable {

    /**
     * A function that creates a new accumulator and returns it. If the {@code
     * deduct} operation is defined, the accumulator object must properly
     * implement {@code equals()}, which will be used to detect when an
     * accumulator is "empty" (i.e., equal to a fresh instance returned from
     * this method) and can be evicted from a processor's storage.
     */
    @Nonnull
    DistributedSupplier<A> createAccumulatorF();

    /**
     * A function that updates the accumulated value to account for a new item.
     */
    @Nonnull
    DistributedBiFunction<A, T, A> accumulateItemF();

    /**
     * A function that accepts two accumulators, merges their contents, and
     * returns an accumulator with the resulting state. It is allowed to mutate
     * the left-hand operator (presumably to return it as the new result), but
     * not the right-hand one.
     */
    @Nonnull
    DistributedBinaryOperator<A> combineAccumulatorsF();

    /**
     * A function that accepts two accumulators, deducts the contents of the
     * right-hand one from the contents of the left-hand one, and returns an
     * accumulator with the resulting state. It is allowed to mutate the
     * left-hand accumulator (presumably to return it as the new result), but
     * not the right-hand one.
     * <p>
     * The effect of this function must be the opposite of {@link
     * #combineAccumulatorsF() combine} so that {@code deduct(combine(acc, x),
     * x)} returns an accumulator in the same state as {@code acc} was before
     * the operation.
     * <p>
     * <b>Note:</b> it's allowed to return {@code null} here, however it
     * impacts performance. This function allows us to <i>combine </i> new
     * frames into sliding window and <i>deduct</i> old frames, as the window
     * slides. Without it, we always have to combine all frames for each window
     * slide, which gets worse, when the window is sled by small increments
     * (with regard to window length). For tumbling windows, it is never used.
     */
    @Nullable
    DistributedBinaryOperator<A> deductAccumulatorF();

    /**
     * A function that finishes the accumulation process by transforming
     * the accumulator object into the final result.
     */
    @Nonnull
    DistributedFunction<A, R> finishAccumulationF();

    /**
     * Returns a new {@code WindowOperation} object composed from the provided
     * primitives.
     *
     * @param <T> the type of the stream item
     * @param <A> the type of the accumulator
     * @param <R> the type of the final result
     *
     * @param createAccumulatorF see {@link #createAccumulatorF()}()
     * @param accumulateItemF see {@link #accumulateItemF()}
     * @param combineAccumulatorsF see {@link #combineAccumulatorsF()}
     * @param deductAccumulatorF see {@link #deductAccumulatorF()}
     * @param finishAccumulationF see {@link #finishAccumulationF()}
     */
    @Nonnull
    static <T, A, R> WindowOperation<T, A, R> of(@Nonnull DistributedSupplier<A> createAccumulatorF,
                                                 @Nonnull DistributedBiFunction<A, T, A> accumulateItemF,
                                                 @Nonnull DistributedBinaryOperator<A> combineAccumulatorsF,
                                                 @Nullable DistributedBinaryOperator<A> deductAccumulatorF,
                                                 @Nonnull DistributedFunction<A, R> finishAccumulationF
    ) {
        Objects.requireNonNull(createAccumulatorF);
        Objects.requireNonNull(accumulateItemF);
        Objects.requireNonNull(combineAccumulatorsF);
        Objects.requireNonNull(finishAccumulationF);
        return new WindowOperationImpl<>(
                createAccumulatorF, accumulateItemF, combineAccumulatorsF, deductAccumulatorF, finishAccumulationF);
    }

    /**
     * Returns a new {@code WindowOperation} object based on a {@code
     * DistributedCollector}. <strong>Note:</strong> the resulting operation
     * will lack the {@code deduct} primitive, which can cause poor performance
     * of a sliding window computation, see {@link #deductAccumulatorF()}
     */
    @Nonnull
    static <T, A, R> WindowOperation<T, A, R> fromCollector(@Nonnull DistributedCollector<T, A, R> c) {
        return of(c.supplier(),
                (a, v) -> {
                    c.accumulator().accept(a, v);
                    return a;
                },
                c.combiner(), null, c.finisher());
    }
}
