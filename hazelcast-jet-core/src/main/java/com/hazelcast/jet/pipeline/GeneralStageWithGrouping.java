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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBiPredicate;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * Represents an intermediate step when constructing a group-and-aggregate
 * pipeline stage. This is the base type for the batch and stream variants.
 *
 * @param <T> type of the stream item
 * @param <K> type of the grouping key
 */
public interface GeneralStageWithGrouping<T, K> {

    /**
     * Returns the function that extracts the grouping key from stream items.
     * This function will be used in the aggregating stage you are about to
     * construct using this object.
     */
    @Nonnull
    DistributedFunction<? super T, ? extends K> keyFn();

    /**
     * Attaches to this stage a mapping stage, one which applies the supplied
     * function to each input item independently and emits the function's result
     * as the output item. The mapping function receives another parameter, the
     * context object which Jet will create using the supplied {@code
     * contextFactory}, separately for each grouping key.
     * <p>
     * If the mapping result is {@code null}, it emits nothing. Therefore this
     * stage can be used to implement filtering semantics as well.
     * <p>
     * Context objects are saved to state snapshot. Therefore they have to be
     * serializable if snapshotting is enabled.
     *
     * <h3>Note on context object retention</h3>
     * A context object, once created, is only released at the end of the job.
     * If new keys appear and disappear on the stream, you'll run out of memory
     * over time.
     *
     * <h3>Note on item retention in {@linkplain GeneralStage#addTimestamps
     * jobs with timestamps}</h3>
     *
     * The context should not be used to accumulate stream items and emit the
     * result later, such as aggregating N items into one. The emitted item
     * will always have the timestamp of the item it was mapped from.
     *
     * @param contextFactory the context factory
     * @param mapFn a stateless mapping function
     * @return the newly attached stage
     *
     * @param <C> type of context object
     * @param <R> the result type of the mapping function
     */
    @Nonnull
    <C, R> GeneralStage<R> mapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends R> mapFn
    );

    /**
     * Attaches to this stage a filtering stage, one which applies the provided
     * predicate function to each input item to decide whether to pass the item
     * to the output or to discard it. The predicate function receives another
     * parameter, the context object which Jet will create using the supplied
     * {@code contextFactory}, separately for each grouping key.
     * <p>
     * Context objects are saved to state snapshot. Therefore they have to be
     * serializable if snapshotting is enabled.
     *
     * <h3>Note on context object retention</h3>
     * A context object, once created, is only released at the end of the job.
     * If new keys appear and disappear on the stream, you'll run out of memory
     * over time.
     *
     * @param contextFactory the context factory
     * @param filterFn a stateless filter predicate function
     * @return the newly attached stage
     *
     * @param <C> type of context object
     */
    @Nonnull
    <C> GeneralStage<T> filterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiPredicate<? super C, ? super T> filterFn
    );

    /**
     * Attaches to this stage a flat-mapping stage, one which applies the
     * supplied function to each input item independently and emits all items
     * from the {@link Traverser} it returns as the output items. The traverser
     * must be <em>null-terminated</em>. The mapping function receives another
     * parameter, the context object which Jet will create using the supplied
     * {@code contextFactory}, separately for each grouping key.
     *
     * <h3>Note on context object retention</h3>
     * A context object, once created, is only released at the end of the job.
     * If new keys appear and disappear on the stream, you'll run out of memory
     * over time.
     *
     * <h3>Note on item retention in {@linkplain GeneralStage#addTimestamps
     * jobs with timestamps}</h3>
     *
     * The context should not be used to accumulate stream items and emit the
     * result later, such as aggregating N items into one. The emitted item
     * will always have the timestamp of the item it was mapped from.
     *
     * @param contextFactory the context factory
     * @param flatMapFn a stateless flatmapping function, whose result type is
     *                  Jet's {@link Traverser}
     * @return the newly attached stage
     *
     * @param <C> type of context object
     * @param <R> the type of items in the result's traversers
     */
    @Nonnull
    <C, R> GeneralStage<R> flatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    );

    /**
     * A shortcut for:
     * <blockquote>
     *     {@link #aggregateRolling(AggregateOperation1,
     *     DistributedBiFunction) aggregateRolling(aggrOp, Util::entry)}.
     * </blockquote>
     */
    @Nonnull
    default <R> GeneralStage<Entry<K, R>> aggregateRolling(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        return aggregateRolling(aggrOp, Util::entry);
    }

    /**
     * Attaches a rolling aggregation stage. Every input item will be
     * accumulated using the given {@code aggrOp} and a finished result will be
     * emitted.
     * <p>
     * For example, if the input is {@code {2, 7, 8, -5}} and the aggregate
     * operation is <em>summing</em>, the output will be {@code {2, 9, 17,
     * 12}}. The number of input and output items is equal.
     * <p>
     * The accumulator for each key is saved to state snapshot; after a
     * restart, the computation is resumed.
     * <p>
     * <strong>NOTE:</strong> An accumulator for a key, once created, is only
     * released at the end of the job. If new keys appear and disappear on the
     * stream, you'll run out of memory over time. Also take caution when using
     * ever-growing accumulators in streaming jobs such as {@code toList()}:
     * they will grow for the lifetime of the job, even after restart.
     *
     * <h3>Limitation on aggregate operations</h3>
     * The used aggregate operation must not return the accumulator in its
     * {@linkplain AggregateOperation#finishFn() finish function}. An aggregate
     * operation is normally allowed to do so, but unlike other stages, this
     * stage continues to accumulate to the accumulator after finishing
     * function was called. This stage throws an exception if the finish
     * function returns the same instance for any accumulator at runtime.
     * <p>
     * For example, {@link AggregateOperations#summingLong summingLong()} is OK
     * because its result is an immutable {@code Long}. {@link
     * AggregateOperations#toSet() toSet()} is not because its result is the
     * same {@code Set} instance to which it accumulates.
     *
     * @param aggrOp the aggregate operation to do the aggregation
     * @param mapToOutputFn a function to construct emitted object from key and
     *                      a result
     * @param <R> result type of the aggregate operation
     * @param <OUT> emitted type from this stage
     *
     * @return the newly attached stage
     */
    @Nonnull
    default <R, OUT> GeneralStage<OUT> aggregateRolling(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp,
            @Nonnull DistributedBiFunction<K, R, OUT> mapToOutputFn
    ) {
        checkSerializable(mapToOutputFn, "mapToOutputFn");
        // Early check for identity finish: tries to finish an empty accumulator, different instance
        // must be returned.
        Object emptyAcc = aggrOp.createFn().get();
        if (emptyAcc == ((Function) aggrOp.finishFn()).apply(emptyAcc)) {
            throw new IllegalArgumentException("Aggregate operation must not use identity finishing function");
        }
        AggregateOperation1<? super T, Object, R> aggrOp1 = (AggregateOperation1<? super T, Object, R>) aggrOp;
        DistributedFunction<? super T, ? extends K> keyFnLocal = keyFn();

        return mapUsingContext(ContextFactory.withCreateFn(jet -> aggrOp1.createFn().get()),
                (Object acc, T item) -> {
                    aggrOp1.accumulateFn().accept(acc, item);
                    R r = aggrOp1.finishFn().apply(acc);
                    if (r == acc) {
                        throw new IllegalArgumentException("Aggregate operation must not use identity finishing function");
                    }
                    return mapToOutputFn.apply(keyFnLocal.apply(item), r);
                });
    }
}
