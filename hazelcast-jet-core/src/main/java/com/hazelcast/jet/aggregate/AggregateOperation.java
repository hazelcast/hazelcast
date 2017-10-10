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

package com.hazelcast.jet.aggregate;

import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.aggregate.AggregateOperation1Impl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Objects;

/**
 * Contains primitives needed to compute an aggregated result of stream
 * processing. The result is computed by updating a mutable result
 * container, called the <em>accumulator</em>, with data from each stream
 * item and, after all items are processed, transforming the accumulator
 * into the final result. The data items may come from one or more inbound
 * streams; there is a separate {@code accumulate} function for each of
 * them.
 * <p>
 * This is a summary of all the primitives involved:
 * <ol><li>
 *     {@link #createFn() create} a new accumulator object
 * </li><li>
 *     {@link #accumulateFn(Tag) accumulate} the data of an item by mutating
 *     the accumulator
 * </li><li>
 *     {@link #combineFn() combine} the contents of the right-hand
 *     accumulator into the left-hand one
 * </li><li>
 *     {@link #deductFn() deduct} the contents of the right-hand
 *     accumulator from the left-hand one (undo the effects of {@code combine})
 * </li><li>
 *     {@link #finishFn() finish} accumulation by transforming the
 *     accumulator object into the final result
 * </li></ol>
 * The <em>deduct</em> primitive is optional. It is used in sliding window
 * aggregation, where it can significantly improve the performance.
 *
 * <h3>Static type design</h3>
 * This interface covers the fully general case with an arbitrary number of
 * contributing streams, each identified by its <em>tag</em>. The static
 * type system cannot capture a variable number of type parameters,
 * therefore this interface has no type parameter describing the stream
 * item. The tags themselves carry the type information, but there must be a
 * runtime check whether a given tag is registered with this aggregate
 * operation.
 * <p>
 * There are specializations of this interface for up to three contributing
 * streams whose type they statically capture. They are {@link
 * AggregateOperation1}, {@link AggregateOperation2} and {@link
 * AggregateOperation3}. If you use the provided {@link
 * #withCreate(DistributedSupplier) builder object}, it will automatically
 * return the appropriate static type, depending on which {@code accumulate}
 * primitives you have provided.
 *
 * @param <A> the type of the accumulator
 * @param <R> the type of the final result
 */
public interface AggregateOperation<A, R> extends Serializable {

    /**
     * A primitive that returns a new accumulator. If the {@code deduct}
     * primitive is defined, the accumulator object must properly implement
     * {@code equals()}, which will be used to detect when an accumulator is
     * "empty" (i.e., equal to a fresh instance returned from this method) and
     * can be evicted from a processor's storage.
     */
    @Nonnull
    DistributedSupplier<A> createFn();

    /**
     * A primitive that updates the accumulator state to account for a new
     * item. The tag argument identifies which of the contributing streams
     * the returned function will handle. If asked for a tag that isn't
     * registered with it, it will throw an exception.
     */
    @Nonnull
    default <T> DistributedBiConsumer<? super A, ? super T> accumulateFn(@Nonnull Tag<T> tag) {
        return accumulateFn(tag.index());
    }

    /**
     * A primitive that updates the accumulator state to account for a new
     * item. The argument identifies the index of the contributing stream
     * the returned function will handle. If asked for an index that isn't
     * registered with it, it will throw an exception.
     */
    @Nonnull
    <T> DistributedBiConsumer<? super A, ? super T> accumulateFn(int index);

    /**
     * A primitive that accepts two accumulators and updates the state of the
     * left-hand one by combining it with the state of the right-hand one.
     * The right-hand accumulator remains unchanged. In some cases, such as
     * single-stage batch aggregation it is not needed and may be {@code null}.
     */
    @Nullable
    DistributedBiConsumer<? super A, ? super A> combineFn();

    /**
     * A primitive that accepts two accumulators and updates the state of the
     * left-hand one by deducting the state of the right-hand one from it. The
     * right-hand accumulator remains unchanged.
     * <p>
     * The effect of this primitive must be the opposite of {@link
     * #combineFn() combine} so that
     * <pre>
     *     combine(acc, x);
     *     deduct(acc, x);
     * </pre>
     * leaves {@code acc} in the same state as it was before the two
     * operations.
     * <p>
     * <strong>Note:</strong> this primitive is only used in sliding window
     * aggregation and even in that case it is optional, but its presence
     * may significantly reduce the computational cost. With it, the current
     * sliding window can be obtained from the previous one by deducting the
     * trailing frame and combining the leading frame; without it, each window
     * must be recomputed from all its constituent frames. The finer the
     * sliding step, the more pronounced the difference in computation effort
     * will be.
     */
    @Nullable
    DistributedBiConsumer<? super A, ? super A> deductFn();

    /**
     * A primitive that finishes the accumulation process by transforming
     * the accumulator object into the final result.
     */
    @Nonnull
    DistributedFunction<? super A, R> finishFn();

    /**
     * Returns a copy of this aggregate operation, but with the {@code finish}
     * primitive replaced with the supplied one.
     *
     * @param finishFn the new {@code finish} primitive
     * @param <R_NEW> the new aggregation result type
     */
    @Nonnull
    <R_NEW> AggregateOperation<A, R_NEW> withFinishFn(
            @Nonnull DistributedFunction<? super A, R_NEW> finishFn
    );

    /**
     * Returns a copy of this aggregate operation, but with the {@code
     * accumulate} primitive replaced with one that expects to find
     * accumulator objects in the input and will combine them all into
     * a single accumulator of the same type.
     *
     * @param getAccFn the function that extracts the accumulator from the stream item
     * @param <T> the type of stream item
     */
    @Nonnull
    default <T> AggregateOperation1<T, A, R> withCombiningAccumulateFn(
            @Nonnull DistributedFunction<T, A> getAccFn
    ) {
        DistributedBiConsumer<? super A, ? super A> combineFn = combineFn();
        Objects.requireNonNull(combineFn, "The 'combine' primitive is missing");
        return new AggregateOperation1Impl<>(
                createFn(),
                (A acc, T item) -> combineFn.accept(acc, getAccFn.apply(item)),
                combineFn,
                deductFn(),
                finishFn());
    }

    /**
     * Returns a builder object, initialized with the supplied {@code create}
     * primitive, that can be used to construct the definition of an aggregate
     * operation in a step-by-step manner.
     * <p>
     * The same builder is used to construct both fixed- and variable-arity
     * aggregate operations:
     * <ul><li>
     *     For fixed arity use {@link
     *     AggregateOperationBuilder#andAccumulate0(DistributedBiConsumer)
     *     andAccumulate0()}, optionally followed by {@code .andAccumulate1()},
     *     {@code .andAccumulate2()}. The return type of these methods changes as the
     *     static types of the contributing streams are captured.
     * </li><li>
     *     For variable arity use {@link AggregateOperationBuilder#andAccumulate(Tag,
     *     DistributedBiConsumer) andAccumulate(tag)}.
     * </li></ul>
     * The {@link AggregateOperationBuilder.Arity1#andFinish(DistributedFunction)
     * andFinish()} method returns the constructed aggregate operation. Its
     * static type receives all the type parameters captured in the above
     * method calls. If your aggregate operation doesn't need a finishing
     * transformation (the accumulator itself is the result value), you
     * can call the shorthand {@link AggregateOperationBuilder.Arity1#andIdentityFinish()
     * andIdentityFinish()}.
     *
     * @param createFn the {@code create} primitive
     * @param <A> the type of the accumulator
     * @return the builder object whose static type represents the fact that it
     *         has just the {@code create} primitive defined
     */
    @Nonnull
    static <A> AggregateOperationBuilder<A> withCreate(@Nonnull DistributedSupplier<A> createFn) {
        return new AggregateOperationBuilder<>(createFn);
    }
}
