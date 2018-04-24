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
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.KeyedWindowResultFunction;

import javax.annotation.Nonnull;

/**
 * Represents an intermediate step in the construction of a pipeline stage
 * that performs a windowed group-and-aggregate operation. It captures the
 * grouping key and the window definition, and offers the methods to finalize
 * the construction by specifying the aggregate operation and any additional
 * pipeline stages contributing their data to a co-group-and-aggregate stage.
 *
 * @param <T> type of the input item
 * @param <K> type of the key
 */
public interface StageWithGroupingAndWindow<T, K> {

    /**
     * Returns the function that extracts the grouping key from stream items.
     * This function will be used in the aggregating stage you are about to
     * construct using this object.
     */
    @Nonnull
    DistributedFunction<? super T, ? extends K> keyFn();

    /**
     * Returns the definition of the window for the windowed aggregation
     * operation that you are about to construct using this object.
     */
    @Nonnull
    WindowDefinition windowDefinition();

    /**
     * Attaches to this stage a stage that performs the given
     * group-and-aggregate operation over the window described by the window
     * definition captured by this object. For each distinct grouping key it
     * observes in the input belonging to a given window, it performs the
     * supplied aggregate operation across all the items sharing that key. Once
     * it has received all the items, it calls the supplied {@code
     * mapToOutputFn} with each key and the associated aggregation result to
     * create the items to emit.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param mapToOutputFn the function that creates the output item
     * @param <A> type of the accumulator used by the aggregate operation
     * @param <R> type of the aggregation result
     * @param <OUT> type of the output item
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    <A, R, OUT> StreamStage<OUT> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn
    );

    /**
     * Attaches to this stage a stage that performs the given
     * group-and-aggregate operation. It emits one key-value pair (in a {@link
     * TimestampedEntry}) for each distinct key it observes in its input
     * belonging to a given window. The value is the result of the aggregate
     * operation across all the items with the given grouping key.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param <A> type of the accumulator used by the aggregate operation
     * @param <R> type of the aggregation result
     */
    @Nonnull
    <A, R> StreamStage<TimestampedEntry<K, R>> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp);

    /**
     * Attaches to this stage a stage that performs the given
     * cogroup-and-aggregate operation over the items from both this stage
     * and {@code stage1} you supply. For each distinct grouping key it
     * observes in the input belonging to a given window, it performs the
     * supplied aggregate operation across all the items sharing that key. Once
     * it has received all the items, it calls the supplied {@code
     * mapToOutputFn} with each key and the associated aggregation result to
     * create the items to emit.
     * <p>
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * The aggregate operation must specify a separate accumulator function for
     * each of the two streams (refer to its {@link AggregateOperation2 Javadoc}
     * for a simple example).
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param mapToOutputFn the function that creates the output item
     * @param <T1> type of items in {@code stage1}
     * @param <A> type of the accumulator used by the aggregate operation
     * @param <R> type of the aggregation result
     * @param <OUT> type of the output item
     */
    @Nonnull
    <T1, A, R, OUT> StreamStage<OUT> aggregate2(
            @Nonnull StreamStageWithGrouping<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn
    );

    /**
     * Attaches to this stage a stage that performs the given
     * cogroup-and-aggregate operation over the items from both this stage
     * and {@code stage1} you supply. It emits one key-value pair (in a {@link
     * TimestampedEntry}) for each distinct key it observes in the input
     * belonging to a given window. The value is the result of the aggregate
     * operation across all the items with the given grouping key.
     * <p>
     * The aggregate operation must specify a separate accumulator function for
     * each of the two streams (refer to its {@link AggregateOperation2 Javadoc}
     * for a simple example).
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <A> type of the accumulator used by the aggregate operation
     * @param <R> type of the aggregation result
     */
    @Nonnull
    <T1, A, R> StreamStage<TimestampedEntry<K, R>> aggregate2(
            @Nonnull StreamStageWithGrouping<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp);

    /**
     * Attaches to this stage a stage that performs the given
     * cogroup-and-aggregate operation over the items from this stage as well
     * as {@code stage1} and {@code stage2} you supply. For each distinct
     * grouping key it observes in the input belonging to a given window, it
     * performs the supplied aggregate operation across all the items sharing
     * that key. Once it has received all the items, it calls the supplied
     * {@code mapToOutputFn} with each key and the associated aggregation result
     * to create the items to emit.
     * <p>
     * The aggregate operation must specify a separate accumulator function for
     * each of the three streams (refer to its {@link AggregateOperation3
     * Javadoc} for a simple example).
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param mapToOutputFn the function that creates the output item
     * @param <T1> type of items in {@code stage1}
     * @param <T2> type of items in {@code stage2}
     * @param <A> type of the accumulator used by the aggregate operation
     * @param <R> type of the aggregation result
     * @param <OUT> type of the output item
     */
    @Nonnull
    <T1, T2, A, R, OUT> StreamStage<OUT> aggregate3(
            @Nonnull StreamStageWithGrouping<T1, ? extends K> stage1,
            @Nonnull StreamStageWithGrouping<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn);

    /**
     * Attaches to this stage a stage that performs the given
     * cogroup-and-aggregate operation over the items from this stage as well
     * as {@code stage1} and {@code stage2} you supply. It emits one key-value
     * pair (in a {@link TimestampedEntry}) for each distinct key it observes
     * in the input belonging to a given window. The value is the result of the
     * aggregate operation across all the items with the given grouping key.
     * <p>
     * The aggregate operation must specify a separate accumulator function for
     * each of the three streams (refer to its {@link AggregateOperation3
     * Javadoc} for a simple example).
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <T2> type of items in {@code stage2}
     * @param <A> type of the accumulator used by the aggregate operation
     * @param <R> type of the aggregation result
     */
    @Nonnull
    <T1, T2, A, R> StreamStage<TimestampedEntry<K, R>> aggregate3(
            @Nonnull StreamStageWithGrouping<T1, ? extends K> stage1,
            @Nonnull StreamStageWithGrouping<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp);

    /**
     * Returns a fluent API builder object to construct a windowed
     * cogroup-and-aggregate stage with any number of contributing stages.
     * It is mainly intended to co-group four or more stages. For up to three
     * stages prefer the direct {@code stage.aggregateN(...)} calls because
     * they offer more static type safety.
     */
    @Nonnull
    default WindowGroupAggregateBuilder<T, K> aggregateBuilder() {
        return new WindowGroupAggregateBuilder<>(this);
    }
}
