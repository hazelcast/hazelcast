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
 * Contains primitives needed to compute an aggregated result of data
 * processing. Check out {@link AggregateOperations} to find the one
 * you need and, if you don't find it there, construct one by using the
 * {@link #withCreate aggregate operation builder} and reading the
 * description below.
 * <p>
 * Jet aggregates the data by updating a mutable container,
 * called the <em>accumulator</em>, with the data from each stream item.
 * It does this by applying the {@link #accumulateFn accumulate} primitive
 * to the the accumulator and a given item. Jet provides some accumulator
 * objects in the {@link com.hazelcast.jet.accumulator accumulator} package
 * that you can reuse, and you can also write your own if needed. The
 * accumulator must be serializable because Jet may need to send it to
 * another member to be combined with other accumulators or store it in state
 * snapshot.
 * <p>
 * After it processes all the items in a batch/window, Jet transforms the
 * accumulator into the final result by applying the {@link #finishFn()
 * finish} primitive.
 * <p>
 * Since it is a distributed/parallel computation engine, Jet will create
 * several independent processing units to perform the same aggregation,
 * and it must combine their partial results before applying the {@code
 * finish} primitive and emitting the final result. This is the role of the
 * {@link #combineFn combine} primitive.
 * <p>
 * Finally, {@code AggregateOperation} also defines the {@link #deductFn()
 * deduct} primitive, which allows Jet to efficiently aggregate infinite
 * stream data into a <em>sliding window</em> by evicting old data from the
 * existing accumulator instead of building a new one from scratch each time
 * the window slides forward. Providing a {@code deduct} primitive that makes
 * the computation more efficient than rebuilding the accumulator from scratch
 * isn't always possible. Therefore it is optional.
 * <p>
 * Depending on usage, the data items may come from one or more inbound
 * streams, and the {@code AggregateOperation} must provide a separate
 * {@code accumulate} primitive for each of them. If you are creating the
 * aggregating pipeline stage using the {@link
 * com.hazelcast.jet.pipeline.StageWithGroupingAndWindow#aggregateBuilder
 * builder object}, then you'll identify each contributing stream to the
 * {@code AggregateOperation} using the <em>tags</em> you got from the
 * builder.
 * <p>
 * If, on the other hand, you are calling one of the direct methods such
 * as {@link com.hazelcast.jet.pipeline.StageWithGroupingAndWindow#aggregate2
 * stage.aggregate2()}, then you'll deal with specializations of this interface
 * such as {@link AggregateOperation2} and you'll identify the input stages by
 * their index; zero index corresponds to the stage you're calling the
 * method on and the higher indices correspond to the stages you pass in as
 * arguments.
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
 *
 * @param <A> the type of the accumulator
 * @param <R> the type of the final result
 */
public interface AggregateOperation<A, R> extends Serializable {

    /**
     * Returns the number of contributing streams this operation is set up to
     * handle. The index passed to {@link #accumulateFn(int)} must be less than
     * this number.
     */
    int arity();

    /**
     * A primitive that returns a new accumulator. If the {@code deduct}
     * primitive is defined, the accumulator object <strong>must</strong>
     * properly implement {@code equals()}. See {@link #deductFn()} for an
     * explanation.
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
     * single-step batch aggregation it is not needed and may be {@code null}.
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
     * This primitive is only used in sliding window aggregation and even in
     * that case it is optional, but its presence may significantly reduce the
     * computational cost. With it, the current sliding window can be obtained
     * from the previous one by deducting the trailing frame and combining the
     * leading frame; without it, each window must be recomputed from all its
     * constituent frames. The finer the sliding step, the more pronounced the
     * difference in computation effort will be.
     * <p>
     * If this method returns non-null, then {@link #createFn()} <strong>must
     * </strong> return an accumulator which properly implements {@code
     * equals()}. After calling {@code deductFn}, Jet will use {@code equals()}
     * to determine whether the accumulator is now "empty" (i.e., equal to a
     * fresh instance), which signals that the current window contains no more
     * items with the associated grouping key and the entry must be removed
     * from the resuts.
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
     * Returns a copy of this aggregate operation, but with all the {@code
     * accumulate} primitives replaced with the ones supplied here. The
     * argument at position {@code i} replaces the primitive at index {@code
     * i}, as returned by {@link #accumulateFn(int)}.
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    AggregateOperation<A, R> withAccumulateFns(DistributedBiConsumer... accumulateFns);

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
        DistributedBiConsumer<? super A, ? super A> combineFn =
                Objects.requireNonNull(combineFn(), "The 'combine' primitive is missing");
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
