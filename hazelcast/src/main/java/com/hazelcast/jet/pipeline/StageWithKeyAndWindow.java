/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;
import static com.hazelcast.jet.aggregate.AggregateOperations.pickAny;

/**
 * Represents an intermediate step in the construction of a pipeline stage
 * that performs a windowed group-and-aggregate operation. It captures the
 * grouping key and the window definition, and offers the methods to finalize
 * the construction by specifying the aggregate operation and any additional
 * pipeline stages contributing their data to a co-group-and-aggregate stage.
 *
 * @param <T> type of the input item
 * @param <K> type of the key
 *
 * @since Jet 3.0
 */
public interface StageWithKeyAndWindow<T, K> {

    /**
     * Returns the function that extracts the grouping key from stream items.
     * This function will be used in the aggregating stage you are about to
     * construct using this object.
     */
    @Nonnull
    FunctionEx<? super T, ? extends K> keyFn();

    /**
     * Returns the definition of the window for the windowed aggregation
     * operation that you are about to construct using this object.
     */
    @Nonnull
    WindowDefinition windowDefinition();

    /**
     * Attaches a stage that passes through just the items that are distinct
     * within their window according to the grouping key (no two items emitted
     * for a window map to the same key). There is no guarantee which one of
     * the items with the same key will pass through.
     *
     * @return the newly attached stage
     */
    @Nonnull
    default StreamStage<KeyedWindowResult<K, T>> distinct() {
        return aggregate(pickAny());
    }

    /**
     * Attaches a stage that performs the given group-and-aggregate operation.
     * It emits one key-value pair (in a {@link KeyedWindowResult}) for each
     * distinct key it observes in its input belonging to a given window. The
     * value is the result of the aggregate operation across all the items with
     * the given grouping key.
     * <p>
     * Sample usage:
     * <pre>{@code
     * StreamStage<KeyedWindowResult<Long, Long>> aggregated = pageVisits
     *     .window(SlidingWindowDefinition.sliding(MINUTES.toMillis(1), SECONDS.toMillis(1)))
     *     .groupingKey(PageVisit::getUserId)
     *     .aggregate(AggregateOperations.counting());
     * }</pre>
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param <R> type of the aggregation result
     */
    @Nonnull
    <R> StreamStage<KeyedWindowResult<K, R>> aggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    );

    /**
     * Attaches a stage that performs the given cogroup-and-aggregate operation
     * over the items from both this stage and {@code stage1} you supply. It
     * emits one key-value pair (in a {@link KeyedWindowResult}) for each
     * distinct key it observes in the input belonging to a given window. The
     * value is the result of the aggregate operation across all the items with
     * the given grouping key.
     * <p>
     * Sample usage:
     * <pre>{@code
     * StreamStage<KeyedWindowResult<Long, Tuple2<Long, Long>>> aggregated = pageVisits
     *     .window(SlidingWindowDefinition.sliding(MINUTES.toMillis(1), SECONDS.toMillis(1)))
     *     .groupingKey(PageVisit::getUserId)
     *     .aggregate2(
     *         addToCarts.groupingKey(AddToCart::getUserId),
     *         AggregateOperations.aggregateOperation2(
     *                 AggregateOperations.counting(),
     *                 AggregateOperations.counting())
     *     );
     * }</pre>
     * This variant requires you to provide a two-input aggregate operation
     * (refer to its {@linkplain AggregateOperation2 Javadoc} for a simple
     * example). If you can express your logic in terms of two single-input
     * aggregate operations, one for each input stream, then you should use
     * {@link #aggregate2(AggregateOperation1, StreamStageWithKey, AggregateOperation1)
     * stage0.aggregate2(aggrOp0, stage1, aggrOp1)} because it offers a simpler
     * API and you can use the already defined single-input operations. Use
     * this variant only when you have the need to implement an aggregate
     * operation that combines the input streams into the same accumulator.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param stage1 the other stage
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <R> type of the aggregation result
     */
    @Nonnull
    <T1, R> StreamStage<KeyedWindowResult<K, R>> aggregate2(
            @Nonnull StreamStageWithKey<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, ?, ? extends R> aggrOp
    );

    /**
     * Attaches a stage that performs the given cogroup-and-aggregate operation
     * over the items from both this stage and {@code stage1} you supply. For
     * each distinct grouping key it observes in the input belonging to a given
     * window, it performs the supplied aggregate operation across all the
     * items sharing that key. It performs the aggregation separately for each
     * input stage: {@code aggrOp0} on this stage and {@code aggrOp1} on {@code
     * stage1}. Once it has received all the items belonging to a window, it
     * emits for each distinct key a {@code KeyedWindowResult(key, Tuple2(result0,
     * result1))}.
     * <p>
     * Sample usage:
     * <pre>{@code
     * StreamStage<KeyedWindowResult<Long, Tuple2<Long, Long>>> aggregated = pageVisits
     *     .window(SlidingWindowDefinition.sliding(MINUTES.toMillis(1), SECONDS.toMillis(1)))
     *     .groupingKey(PageVisit::getUserId)
     *     .aggregate2(
     *             AggregateOperations.counting(),
     *             addToCarts.groupingKey(AddToCart::getUserId),
     *             AggregateOperations.counting()
     *     );
     * }</pre>
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp0 aggregate operation to perform on this stage
     * @param stage1 the other stage
     * @param aggrOp1 aggregate operation to perform on the other stage
     * @param <T1> type of the items in the other stage
     * @param <R0> type of the aggregated result for this stage
     * @param <R1> type of the aggregated result for the other stage
     */
    @Nonnull
    default <T1, R0, R1> StreamStage<KeyedWindowResult<K, Tuple2<R0, R1>>> aggregate2(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> aggrOp0,
            @Nonnull StreamStageWithKey<T1, ? extends K> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> aggrOp1
    ) {
        return aggregate2(stage1, aggregateOperation2(aggrOp0, aggrOp1, Tuple2::tuple2));
    }

    /**
     * Attaches a stage that performs the given cogroup-and-aggregate operation
     * over the items from this stage as well as {@code stage1} and {@code
     * stage2} you supply. For each distinct grouping key it observes in the
     * input belonging to a given window, it performs the supplied aggregate
     * operation across all the items sharing that key. Once it has received
     * all the items belonging to a window, it emits for each distinct key a
     * {@code KeyedWindowResult(key, Tuple3(result0, result1, result2))}.
     * <p>
     * Sample usage:
     * StreamStage<KeyedWindowResult<Long, Tuple3<Long, Long, Long>>> aggregated = pageVisits
     *     .window(SlidingWindowDefinition.sliding(MINUTES.toMillis(1), SECONDS.toMillis(1)))
     *     .groupingKey(PageVisit::getUserId)
     *     .aggregate3(
     *         addToCarts.groupingKey(AddToCart::getUserId),
     *         payments.groupingKey(Payment::getUserId),
     *         AggregateOperations.aggregateOperation3(
     *             AggregateOperations.counting(),
     *             AggregateOperations.counting(),
     *             AggregateOperations.counting())
     *     );
     * <pre>{@code
     *
     * }</pre>
     * This variant requires you to provide a three-input aggregate operation
     * (refer to its {@linkplain AggregateOperation3 Javadoc} for a simple
     * example). If you can express your logic in terms of three single-input
     * aggregate operations, one for each input stream, then you should use
     * {@link #aggregate3(AggregateOperation1, StreamStageWithKey,
     *      AggregateOperation1, StreamStageWithKey, AggregateOperation1)
     * stage0.aggregate2(aggrOp0, stage1, aggrOp1, stage2, aggrOp2)} because it
     * offers a simpler API and you can use the already defined single-input
     * operations. Use this variant only when you have the need to implement an
     * aggregate operation that combines the input streams into the same
     * accumulator.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param stage1 the first additional stage
     * @param stage2 the second additional stage
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <T2> type of items in {@code stage2}
     * @param <R> type of the aggregation result
     */
    @Nonnull
    <T1, T2, R> StreamStage<KeyedWindowResult<K, R>> aggregate3(
            @Nonnull StreamStageWithKey<T1, ? extends K> stage1,
            @Nonnull StreamStageWithKey<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, ?, ? extends R> aggrOp
    );

    /**
     * Attaches a stage that performs the given cogroup-and-aggregate operation
     * over the items from both this stage and {@code stage1} you supply. For
     * each distinct grouping key it observes in the input belonging to a given
     * window, it performs the supplied aggregate operation across all the
     * items sharing that key. It performs the aggregation separately for each
     * input stage: {@code aggrOp0} on this stage, {@code aggrOp1} on {@code
     * stage1} and {@code aggrOp2} on {@code stage2}. Once it has received all
     * the items, it calls the supplied {@code mapToOutputFn} with each key and
     * the associated aggregation result to create the items to emit.
     * <p>
     * Sample usage:
     * <pre>{@code
     * StreamStage<KeyedWindowResult<Long, Tuple3<Long, Long, Long>>> aggregated = pageVisits
     *     .window(SlidingWindowDefinition.sliding(MINUTES.toMillis(1), SECONDS.toMillis(1)))
     *     .groupingKey(PageVisit::getUserId)
     *     .aggregate3(
     *         AggregateOperations.counting(),
     *         addToCarts.groupingKey(AddToCart::getUserId),
     *         AggregateOperations.counting(),
     *         payments.groupingKey(Payment::getUserId),
     *         AggregateOperations.counting()
     *     );
     * }</pre>
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp0 aggregate operation to perform on this stage
     * @param stage1 the first additional stage
     * @param aggrOp1 aggregate operation to perform on {@code stage1}
     * @param stage2 the second additional stage
     * @param aggrOp2 aggregate operation to perform on {@code stage2}
     * @param <T1> type of the items in {@code stage1}
     * @param <T2> type of the items in {@code stage2}
     * @param <R0> type of the aggregated result for this stage
     * @param <R1> type of the aggregated result for {@code stage1}
     * @param <R2> type of the aggregated result for {@code stage2}
     */
    @Nonnull
    default <T1, T2, R0, R1, R2> StreamStage<KeyedWindowResult<K, Tuple3<R0, R1, R2>>> aggregate3(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> aggrOp0,
            @Nonnull StreamStageWithKey<T1, ? extends K> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> aggrOp1,
            @Nonnull StreamStageWithKey<T2, ? extends K> stage2,
            @Nonnull AggregateOperation1<? super T2, ?, ? extends R2> aggrOp2
    ) {
        return aggregate3(stage1, stage2, aggregateOperation3(aggrOp0, aggrOp1, aggrOp2, Tuple3::tuple3));
    }

    /**
     * Offers a step-by-step API to build a pipeline stage that co-aggregates
     * the data from several input stages. The current stage will be already
     * registered with the builder you get. You supply an aggregate operation
     * for each input stage and in the output you get the individual
     * aggregation results as {@link KeyedWindowResult KeyedWindowResult(key,
     * itemsByTag)}. Use the tag you get from {@link AggregateBuilder#add
     * builder.add(stageN, aggrOpN)} to retrieve the aggregated result for that
     * stage. Use {@link AggregateBuilder#tag0() builder.tag0()} as the tag of
     * this stage.
     * <p>
     * This builder is mainly intended to build a co-aggregation of four or
     * more contributing stages. For up to three stages, prefer the direct
     * {@code stage.aggregateN(...)} calls because they offer more static type
     * safety.
     * <p>
     * This example reads from three stream sources that produce {@code
     * Map.Entry<String, Long>}. It groups by entry key, defines a 1-second
     * sliding window and then counts the items in stage-0, sums those in
     * stage-1 and takes the average of those in stage-2:
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     * StreamStageWithKey<Entry<String, Long>, String> stage0 =
     *         p.readFrom(source0).withNativeTimestamps(0L)
     *          .groupingKey(Entry::getKey);
     * StreamStageWithKey<Entry<String, Long>, String> stage1 =
     *         p.readFrom(source1).withNativeTimestamps(0L)
     *          .groupingKey(Entry::getKey);
     * StreamStageWithKey<Entry<String, Long>, String> stage2 =
     *         p.readFrom(source2).withNativeTimestamps(0L)
     *          .groupingKey(Entry::getKey);
     * WindowGroupAggregateBuilder<String, Long> b = stage0
     *         .window(sliding(1000, 10))
     *         .aggregateBuilder(AggregateOperations.counting());
     * Tag<Long> tag0 = b.tag0();
     * Tag<Long> tag1 = b.add(stage1,
     *         AggregateOperations.summingLong(Entry::getValue));
     * Tag<Double> tag2 = b.add(stage2,
     *         AggregateOperations.averagingLong(Entry::getValue));
     * StreamStage<KeyedWindowResult<String, ItemsByTag>> aggregated = b.build();
     * aggregated.map(e -> String.format(
     *         "Key %s, count of stage0: %d, sum of stage1: %d, average of stage2: %f",
     *         e.getKey(),
     *         e.getValue().get(tag0), e.getValue().get(tag1), e.getValue().get(tag2))
     * );
     *}</pre>
     */
    @Nonnull
    default <R0> WindowGroupAggregateBuilder<K, R0> aggregateBuilder(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> aggrOp0
    ) {
        return new WindowGroupAggregateBuilder<>(this, aggrOp0);
    }

    /**
     * Offers a step-by-step API to build a pipeline stage that co-aggregates
     * the data from several input stages. This stage will be already
     * registered with the builder you get.
     * <p>
     * This builder requires you to provide a multi-input aggregate operation.
     * If you can express your logic in terms of single-input aggregate
     * operations, one for each input stream, then you should use {@link
     * #aggregateBuilder(AggregateOperation1) stage0.aggregateBuilder(aggrOp0)}
     * because it offers a simpler API. Use this builder only when you have the
     * need to implement an aggregate operation that combines all the input
     * streams into the same accumulator.
     * <p>
     * This builder is mainly intended to build a co-aggregation of four or
     * more contributing stages. For up to three stages, prefer the direct
     * {@code stage.aggregateN(...)} calls because they offer more static type
     * safety.
     * <p>
     * To add the other stages, call {@link WindowGroupAggregateBuilder1#add
     * builder.add(stage)}. Collect all the tags returned from {@code add()}
     * and use them when building the aggregate operation. Retrieve the tag of
     * the first stage (from which you obtained this builder) by calling {@link
     * WindowGroupAggregateBuilder1#tag0() builder.tag0()}.
     * <p>
     * This example takes three streams of {@code Map.Entry<String, Long>},
     * specifies a 1-second sliding window and, for each string key, counts
     * the distinct {@code Long} values across all input streams:
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     *
     * StreamStageWithGrouping<Entry<String, Long>, String> stage0 =
     *         p.readFrom(source0).groupingKey(Entry::getKey);
     * StreamStageWithGrouping<Entry<String, Long>, String> stage1 =
     *         p.readFrom(source1).groupingKey(Entry::getKey);
     * StreamStageWithGrouping<Entry<String, Long>, String> stage2 =
     *         p.readFrom(source2).groupingKey(Entry::getKey);
     *
     * WindowGroupAggregateBuilder1<Entry<String, Long>, String> b = stage0
     *         .window(sliding(1000, 10))
     *         .aggregateBuilder();
     * Tag<Entry<String, Long>> tag0 = b.tag0();
     * Tag<Entry<String, Long>> tag1 = b.add(stage1);
     * Tag<Entry<String, Long>> tag2 = b.add(stage2);
     * StreamStage<KeyedWindowResult<String, Integer>> aggregated = b.build(AggregateOperation
     *         .withCreate(HashSet<Long>::new)
     *         .andAccumulate(tag0, (acc, item) -> acc.add(item.getValue()))
     *         .andAccumulate(tag1, (acc, item) -> acc.add(item.getValue()))
     *         .andAccumulate(tag2, (acc, item) -> acc.add(item.getValue()))
     *         .andCombine(HashSet::addAll)
     *         .andFinish(HashSet::size));
     * }</pre>
     */
    @Nonnull
    default WindowGroupAggregateBuilder1<T, K> aggregateBuilder() {
        return new WindowGroupAggregateBuilder1<>(this);
    }
}
