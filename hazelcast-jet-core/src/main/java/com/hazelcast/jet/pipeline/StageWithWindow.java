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

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.WindowResultFunction;

import javax.annotation.Nonnull;

/**
 * Represents an intermediate step in the construction of a pipeline stage
 * that performs a windowed aggregate operation. You can perform a global
 * aggregation or add a grouping key to perform a group-and-aggregate
 * operation.
 *
 * @param <T> type of the input item
 */
public interface StageWithWindow<T> {

    /**
     * Returns the pipeline stage associated with this object. It is the stage
     * to which you are about to attach an aggregating stage.
     */
    @Nonnull
    StreamStage<T> streamStage();

    /**
     * Returns the definition of the window for the windowed aggregation
     * operation that you are about to construct using this object.
     */
    @Nonnull
    WindowDefinition windowDefinition();

    /**
     * Specifes the function that will extract the grouping key from the
     * items in the associated pipeline stage and moves on to the step in
     * which you'll complete the construction of a windowed group-and-aggregate
     * stage.
     *
     * @param keyFn function that extracts the grouping key
     * @param <K> type of the key
     */
    @Nonnull
    <K> StageWithGroupingAndWindow<T, K> groupingKey(
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn
    );

    /**
     * Attaches to this stage a stage that performs the given aggregate operation
     * over all the items that belong to a given window. Once the window is
     * complete, it invokes {@code mapToOutputFn} with the result of the aggregate
     * operation and emits its return value as the window result.
     *
     * @param aggrOp the aggregate operation to perform
     * @param mapToOutputFn the function that creates the output item
     * @param <A> the type of the accumulator used by the aggregate operation
     * @param <R> the type of the result
     */
    @Nonnull
    <A, R, OUT> StreamStage<OUT> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp,
            @Nonnull WindowResultFunction<? super R, ? extends OUT> mapToOutputFn
    );

    /**
     * Attaches to this stage a stage that performs the given aggregate
     * operation over all the items that belong to a given window. Once the
     * window is complete, it emits a {@code TimestampedItem} with the result
     * of the aggregate operation and the timestamp denoting the window's
     * ending time.
     *
     * @param aggrOp the aggregate operation to perform
     * @param <A> the type of the accumulator used by the aggregate operation
     * @param <R> the type of the result
     */
    @Nonnull
    default <A, R> StreamStage<TimestampedItem<R>> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp
    ) {
        return aggregate(aggrOp, TimestampedItem::new);
    }

    /**
     * Attaches to this stage a stage that performs the given aggregate
     * operation over all the items that belong to the same window. It receives
     * the items from both this stage and {@code stage1}. Once a given window
     * is complete, it invokes {@code mapToOutputFn} with the result of the
     * aggregate operation and emits its return value as the window result.
     * <p>
     * The aggregate operation must specify a separate accumulator function for
     * each of the two streams (refer to its {@link AggregateOperation2
     * Javadoc} for a simple example).
     * <p>
     * The aggregating stage emits a single item for each completed window.
     *
     * @param aggrOp the aggregate operation to perform
     * @param mapToOutputFn the function that creates the output item
     * @param <T1> type of items in {@code stage1}
     * @param <A> type of the accumulator used by the aggregate operation
     * @param <R> type of the result
     */
    @Nonnull
    <T1, A, R, OUT> StreamStage<OUT> aggregate2(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp,
            @Nonnull WindowResultFunction<? super R, ? extends OUT> mapToOutputFn);

    /**
     * Attaches to this stage a stage that performs the given aggregate
     * operation over all the items that belong to the same window. It receives
     * the items from both this stage and {@code stage1}. Once a given window
     * is complete, it emits a {@link TimestampedItem} with the result
     * of the aggregate operation and the timestamp denoting the window's
     * ending time.
     * <p>
     * The aggregate operation must specify a separate
     * accumulator function for each of the two streams (refer to its {@link
     * AggregateOperation2 Javadoc} for a simple example).
     *
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <A> type of the accumulator used by the aggregate operation
     * @param <R> type of the result
     */
    @Nonnull
    default <T1, A, R> StreamStage<TimestampedItem<R>> aggregate2(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp
    ) {
        return aggregate2(stage1, aggrOp, TimestampedItem::new);
    }

    /**
     * Attaches to this stage a stage that performs the given aggregate
     * operation over the items it receives from this stage as well as
     * {@code stage1} and {@code stage2} you supply. Once a given window
     * is complete, it invokes {@code mapToOutputFn} with the result of the
     * aggregate operation and emits its return value as the window result.
     * <p>
     * The aggregate operation must specify a separate accumulator function
     * for each of the three streams (refer to its {@link AggregateOperation3
     * Javadoc} for a simple example).
     *
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <T2> type of items in {@code stage2}
     * @param <A> type of the accumulator used by the aggregate operation
     * @param <R> type of the result
     */
    @Nonnull
    <T1, T2, A, R, OUT> StreamStage<OUT> aggregate3(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull StreamStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp,
            @Nonnull WindowResultFunction<? super R, ? extends OUT> mapToOutputFn);

    /**
     * Attaches to this stage a stage that performs the given aggregate
     * operation over the items it receives from this stage as well as
     * {@code stage1} and {@code stage2} you supply. Once a given window
     * is complete, it emits a {@link TimestampedItem} with the result
     * of the aggregate operation and the timestamp denoting the window's
     * ending time.
     * <p>
     * The aggregate operation must specify a separate accumulator function
     * for each of the three streams (refer to its {@link AggregateOperation3
     * Javadoc} for a simple example).
     *
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <T2> type of items in {@code stage2}
     * @param <A> type of the accumulator used by the aggregate operation
     * @param <R> type of the result
     */
    @Nonnull
    default <T1, T2, A, R> StreamStage<TimestampedItem<R>> aggregate3(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull StreamStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp
    ) {
        return aggregate3(stage1, stage2, aggrOp, TimestampedItem::new);
    }

    /**
     * Returns a fluent API builder object to construct an windowed aggregating
     * stage with any number of contributing stages. It is mainly intended to
     * co-aggregate four or more stages. For up to three stages prefer the
     * direct {@code stage.aggregateN(...)} calls because they offer more
     * static type safety.
     */
    @Nonnull
    default WindowAggregateBuilder<T> aggregateBuilder() {
        return new WindowAggregateBuilder<>(streamStage(), windowDefinition());
    }
}
