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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.datamodel.WindowResult;

import javax.annotation.Nonnull;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;

/**
 * Represents an intermediate step in the construction of a pipeline stage
 * that performs a windowed aggregate operation. You can perform a global
 * aggregation or add a grouping key to perform a group-and-aggregate
 * operation.
 *
 * @param <T> type of the input item
 *
 * @since Jet 3.0
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
     * Specifies the function that will extract the grouping key from the
     * items in the associated pipeline stage and moves on to the step in
     * which you'll complete the construction of a windowed group-and-aggregate
     * stage.
     * <p>
     * <b>Note:</b> make sure the extracted key is not-null, it would fail the
     * job otherwise. Also make sure that it implements {@code equals()} and
     * {@code hashCode()}.
     *
     * @param keyFn function that extracts the grouping key. It must be
     *     stateless and {@linkplain Processor#isCooperative() cooperative}.
     * @param <K> type of the key
     */
    @Nonnull
    <K> StageWithKeyAndWindow<T, K> groupingKey(
            @Nonnull FunctionEx<? super T, ? extends K> keyFn
    );

    /**
     * Attaches a stage that passes through just the items that are distinct
     * within their window (no two items emitted for a window are equal). There
     * is no guarantee which one of the items with the same key will pass
     * through. The stage emits results in the form of {@link WindowResult
     * WindowResult(windowEnd, distinctItem)}.
     *
     * @return the newly attached stage
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    default StreamStage<WindowResult<T>> distinct() {
        return (StreamStage<WindowResult<T>>) (StreamStage) groupingKey(wholeItem()).distinct();
    }

    /**
     * Attaches a stage that performs the given aggregate operation over all
     * the items that belong to a given window. Once the window is complete, it
     * emits a {@code WindowResult} with the result of the aggregate
     * operation and the timestamp denoting the window's ending time.
     * <p>
     * Sample usage:
     * <pre>{@code
     * StreamStage<WindowResult<Long>> aggregated = pageVisits
     *     .window(SlidingWindowDefinition.sliding(MINUTES.toMillis(1), SECONDS.toMillis(1)))
     *     .aggregate(AggregateOperations.counting());
     * }</pre>
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param <R> the type of the result
     */
    @Nonnull
    <R> StreamStage<WindowResult<R>> aggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    );

    /**
     * Attaches a stage that performs the given aggregate operation over all
     * the items that belong to the same window. It receives the items from
     * both this stage and {@code stage1}. Once a given window is complete, it
     * invokes {@code mapToOutputFn} with the result of the aggregate
     * operation and emits its return value as the window result.
     * <p>
     * Sample usage:
     * <pre>{@code
     * StreamStage<WindowResult<Tuple2<Long, Long>>> aggregated = pageVisits
     *     .window(SlidingWindowDefinition.sliding(MINUTES.toMillis(1), SECONDS.toMillis(1)))
     *     .aggregate2(
     *         addToCarts,
     *         AggregateOperations.aggregateOperation2(
     *             AggregateOperations.counting(),
     *             AggregateOperations.counting())
     *     );
     * }</pre>
     * This variant requires you to provide a two-input aggregate operation
     * (refer to its {@linkplain AggregateOperation2 Javadoc} for a simple
     * example). If you can express your logic in terms of two single-input
     * aggregate operations, one for each input stream, then you should use
     * {@link #aggregate2(AggregateOperation1, StreamStage, AggregateOperation1)
     * stage0.aggregate2(aggrOp0, stage1, aggrOp1)} because it offers a simpler
     * API and you can use the already defined single-input operations. Use
     * this variant only when you have the need to implement an aggregate
     * operation that combines the input streams into the same accumulator.
     * <p>
     * The aggregating stage emits a single item for each completed window.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <R> type of the aggregation result
     */
    @Nonnull
    <T1, R> StreamStage<WindowResult<R>> aggregate2(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, ?, ? extends R> aggrOp
    );

    /**
     * Attaches a stage that performs the given co-aggregate operations over
     * the items from this stage and {@code stage1} you supply. It performs
     * the aggregation separately for each input stage: {@code aggrOp0} on this
     * stage and {@code aggrOp1} on {@code stage1}. Once it has received all
     * the items belonging to a window, it emits a {@code
     * WindowResult(Tuple2(result0, result1))}.
     * <p>
     * The aggregating stage emits a single item for each completed window.
     * <p>
     * Sample usage:
     * <pre>{@code
     * StreamStage<WindowResult<Tuple2<Long, Long>>> aggregated = pageVisits
     *     .window(SlidingWindowDefinition.sliding(MINUTES.toMillis(1), SECONDS.toMillis(1)))
     *     .aggregate2(
     *         AggregateOperations.counting(),
     *         addToCarts,
     *         AggregateOperations.counting()
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
    default <T1, R0, R1> StreamStage<WindowResult<Tuple2<R0, R1>>> aggregate2(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> aggrOp0,
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> aggrOp1
    ) {
        return aggregate2(stage1, aggregateOperation2(aggrOp0, aggrOp1, Tuple2::tuple2));
    }

    /**
     * Attaches a stage that performs the given aggregate operation over the
     * items it receives from this stage as well as {@code stage1} and {@code
     * stage2} you supply. Once a given window is complete, it emits a {@link
     * WindowResult} with the result of the aggregate operation and the
     * timestamp denoting the window's ending time.
     * <p>
     * Sample usage:
     * <pre>{@code
     * StreamStage<WindowResult<Tuple3<Long, Long, Long>>> aggregated = pageVisits
     *     .window(SlidingWindowDefinition.sliding(MINUTES.toMillis(1), SECONDS.toMillis(1)))
     *     .aggregate3(
     *         addToCarts,
     *         payments,
     *         AggregateOperations.aggregateOperation3(
     *             AggregateOperations.counting(),
     *             AggregateOperations.counting(),
     *             AggregateOperations.counting())
     *     );
     * }</pre>
     * This variant requires you to provide a three-input aggregate operation
     * (refer to its {@linkplain AggregateOperation3 Javadoc} for a simple
     * example). If you can express your logic in terms of three single-input
     * aggregate operations, one for each input stream, then you should use
     * {@link #aggregate3(AggregateOperation1, StreamStage,
     *      AggregateOperation1, StreamStage, AggregateOperation1)
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
     * @param <R> type of the result
     */
    @Nonnull
    <T1, T2, R> StreamStage<WindowResult<R>> aggregate3(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull StreamStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, ?, ? extends R> aggrOp
    );

    /**
     * Attaches a stage that performs the given aggregate operation over all
     * the items that belong to the same window. It receives the items from
     * both this stage and {@code stage1}. It performs the aggregation
     * separately for each input stage: {@code aggrOp0} on this stage, {@code
     * aggrOp1} on {@code stage1} and {@code aggrOp2} on {@code stage2}. Once
     * it has received all the items belonging to a window, it emits a {@code
     * WindowResult(Tuple3(result0, result1, result2))}.
     * <p>
     * The aggregating stage emits a single item for each completed window.
     * <p>
     * Sample usage:
     * <pre>{@code
     * StreamStage<WindowResult<Tuple3<Long, Long, Long>>> aggregated = pageVisits
     *     .window(SlidingWindowDefinition.sliding(MINUTES.toMillis(1), SECONDS.toMillis(1)))
     *     .aggregate3(
     *         AggregateOperations.counting(),
     *         addToCarts,
     *         AggregateOperations.counting(),
     *         payments,
     *         AggregateOperations.counting()
     *     );
     * }</pre>
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     *
     * @param aggrOp0 aggregate operation to perform on this stage
     * @param stage1 the first additional stage
     * @param aggrOp1 aggregate operation to perform on {@code stage1}
     * @param stage2 the second additional stage
     * @param aggrOp2 aggregate operation to perform on {@code stage2}
     * @param <T1> type of items in {@code stage1}
     * @param <T2> type of items in {@code stage2}
     * @param <R0> type of the result from stream-0
     * @param <R1> type of the result from stream-1
     * @param <R2> type of the result from stream-2
     */
    @Nonnull
    default <T1, T2, R0, R1, R2> StreamStage<WindowResult<Tuple3<R0, R1, R2>>> aggregate3(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> aggrOp0,
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> aggrOp1,
            @Nonnull StreamStage<T2> stage2,
            @Nonnull AggregateOperation1<? super T2, ?, ? extends R2> aggrOp2
    ) {
        return aggregate3(stage1, stage2, aggregateOperation3(aggrOp0, aggrOp1, aggrOp2, Tuple3::tuple3));
    }

    /**
     * Offers a step-by-step API to build a pipeline stage that co-aggregates
     * the data from several input stages. The current stage will be already
     * registered with the builder you get. You supply an aggregate operation
     * for each input stage and in the output you get the individual
     * aggregation results in a {@code WindowResult(windowEnd, itemsByTag)}.
     * Use the tag you get from {@link AggregateBuilder#add builder.add(stageN,
     * aggrOpN)} to retrieve the aggregated result for that stage. Use {@link
     * AggregateBuilder#tag0() builder.tag0()} as the tag of this stage. You
     * will also be able to supply a function to the builder that immediately
     * transforms the results to the desired output type.
     * <p>
     * This builder is mainly intended to build a co-aggregation of four or
     * more contributing stages. For up to three stages, prefer the direct
     * {@code stage.aggregateN(...)} calls because they offer more static type
     * safety.
     * <p>
     * This example defines a 1-second sliding window and counts the items in
     * stage-0, sums those in stage-1 and takes the average of those in
     * stage-2:
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     * StreamStage<Long> stage0 = p.readFrom(source0).withNativeTimestamps(0L);;
     * StreamStage<Long> stage1 = p.readFrom(source1).withNativeTimestamps(0L);;
     * StreamStage<Long> stage2 = p.readFrom(source2).withNativeTimestamps(0L);;
     * WindowAggregateBuilder<Long> b = stage0
     *         .window(sliding(1000, 10))
     *         .aggregateBuilder(AggregateOperations.counting());
     * Tag<Long> tag0 = b.tag0();
     * Tag<Long> tag1 = b.add(stage1,
     *         AggregateOperations.summingLong(Long::longValue));
     * Tag<Double> tag2 = b.add(stage2,
     *         AggregateOperations.averagingLong(Long::longValue));
     * StreamStage<WindowResult<ItemsByTag>> aggregated = b.build();
     * aggregated.map(e -> String.format(
     *         "Timestamp %d, count of stage0: %d, sum of stage1: %d, average of stage2: %f",
     *         e.timestamp(), e.item().get(tag0), e.item().get(tag1), e.item().get(tag2))
     * );
     *}</pre>
     */
    @Nonnull
    default <R0> WindowAggregateBuilder<R0> aggregateBuilder(
            AggregateOperation1<? super T, ?, ? extends R0> aggrOp
    ) {
        return new WindowAggregateBuilder<>(streamStage(), aggrOp, windowDefinition());
    }

    /**
     * Offers a step-by-step API to build a pipeline stage that co-aggregates
     * the data from several input stages. The current stage will be already
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
     * To add the other stages, call {@link WindowAggregateBuilder1#add
     * add(stage)}. Collect all the tags returned from {@code add()} and use
     * them when building the aggregate operation. Retrieve the tag of the
     * first stage (from which you obtained this builder) by calling {@link
     * WindowAggregateBuilder1#tag0()}.
     * <p>
     * This example takes three streams of strings, specifies a 1-second
     * sliding window and counts the distinct strings across all streams:
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     * StreamStage<String> stage0 = p.readFrom(source0).withNativeTimestamps(0L);;
     * StreamStage<String> stage1 = p.readFrom(source1).withNativeTimestamps(0L);;
     * StreamStage<String> stage2 = p.readFrom(source2).withNativeTimestamps(0L);;
     * WindowAggregateBuilder1<String> b = stage0
     *         .window(sliding(1000, 10))
     *         .aggregateBuilder();
     * Tag<String> tag0 = b.tag0();
     * Tag<String> tag1 = b.add(stage1);
     * Tag<String> tag2 = b.add(stage2);
     * StreamStage<WindowResult<Integer>> aggregated = b.build(AggregateOperation
     *         .withCreate(HashSet<String>::new)
     *         .andAccumulate(tag0, (acc, item) -> acc.add(item))
     *         .andAccumulate(tag1, (acc, item) -> acc.add(item))
     *         .andAccumulate(tag2, (acc, item) -> acc.add(item))
     *         .andCombine(HashSet::addAll)
     *         .andExportFinish(HashSet::size));
     * }</pre>
     */
    @Nonnull
    default WindowAggregateBuilder1<T> aggregateBuilder() {
        return new WindowAggregateBuilder1<>(streamStage(), windowDefinition());
    }
}
