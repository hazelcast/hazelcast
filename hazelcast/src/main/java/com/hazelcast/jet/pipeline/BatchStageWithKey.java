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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BiPredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.function.TriPredicate;
import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;

/**
 * An intermediate step while constructing a group-and-aggregate batch
 * pipeline stage. It captures the grouping key and offers the methods to
 * finalize the construction by specifying the aggregate operation and any
 * additional pipeline stages contributing their data to a
 * co-group-and-aggregate stage.
 *
 * @param <T> type of the input item
 * @param <K> type of the key
 *
 * @since Jet 3.0
 */
public interface BatchStageWithKey<T, K> extends GeneralStageWithKey<T, K> {

    /**
     * Attaches a stage that emits just the items that are distinct according
     * to the grouping key (no two items which map to the same key will be on
     * the output). There is no guarantee which one of the items with the same
     * key it will emit.
     * <p>
     * This operation is subject to memory limits. See {@link
     * InstanceConfig#setMaxProcessorAccumulatedRecords(long)} for more
     * information.
     *
     * @return the newly attached stage
     */
    @Nonnull
    BatchStage<T> distinct();

    @Nonnull @Override
    <S, R> BatchStage<R> mapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn
    );

    @Nonnull @Override
    <S> BatchStage<T> filterStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    );

    @Nonnull @Override
    <S, R> BatchStage<R> flatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn
    );

    @Nonnull @Override
    default <A, R> BatchStage<Entry<K, R>> rollingAggregate(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        return (BatchStage<Entry<K, R>>) GeneralStageWithKey.super.<A, R>rollingAggregate(aggrOp);
    }

    @Nonnull @Override
    default <V, R> BatchStage<R> mapUsingIMap(
            @Nonnull String mapName,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return (BatchStage<R>) GeneralStageWithKey.super.<V, R>mapUsingIMap(mapName, mapFn);
    }

    @Nonnull @Override
    default <V, R> BatchStage<R> mapUsingIMap(
            @Nonnull IMap<K, V> iMap,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return (BatchStage<R>) GeneralStageWithKey.super.<V, R>mapUsingIMap(iMap, mapFn);
    }

    @Nonnull @Override
    <S, R> BatchStage<R> mapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn
    );

    @Nonnull @Override
    default <S, R> BatchStage<R> mapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, CompletableFuture<R>> mapAsyncFn
    ) {
        return (BatchStage<R>) GeneralStageWithKey.super.mapUsingServiceAsync(serviceFactory, mapAsyncFn);
    }

    @Nonnull @Override
    <S, R> BatchStage<R> mapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxConcurrentOps,
            boolean preserveOrder,
            @Nonnull TriFunction<? super S, ? super K, ? super T, CompletableFuture<R>> mapAsyncFn
    );

    @Nonnull @Override
    <S, R> BatchStage<R> mapUsingServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<List<R>>> mapAsyncFn
    );

    @Nonnull @Override
    <S, R> BatchStage<R> mapUsingServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull TriFunction<? super S, ? super List<K>, ? super List<T>,
                    ? extends CompletableFuture<List<R>>> mapAsyncFn
    );

    @Nonnull @Override
    <S> BatchStage<T> filterUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriPredicate<? super S, ? super K, ? super T> filterFn
    );

    @Nonnull @Override
    <S, R> BatchStage<R> flatMapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn
    );

    /**
     * Attaches a stage that performs the given group-and-aggregate operation.
     * It emits one key-value pair (in a {@code Map.Entry}) for each distinct
     * key it observes in its input. The value is the result of the aggregate
     * operation across all the items with the given grouping key.
     * <p>
     * Sample usage:
     * <pre>{@code
     * BatchStage<Entry<String, Long>> aggregated = people.
     *         .groupingKey(Person::getLastName)
     *         .aggregate(AggregateOperations.counting());
     * }</pre>
     * <p>
     * This operation is subject to memory limits. See {@link
     * InstanceConfig#setMaxProcessorAccumulatedRecords(long)} for more
     * information.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param <R> type of the aggregation result
     */
    @Nonnull
    <R> BatchStage<Entry<K, R>> aggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    );

    /**
     * Attaches a stage that performs the given cogroup-and-aggregate operation
     * over the items from both this stage and {@code stage1} you supply. It
     * emits one key-value pair (in a {@code Map.Entry}) for each distinct key
     * it observes in its input. The value is the result of the aggregate
     * operation across all the items with the given grouping key.
     * <p>
     * Sample usage:
     * <pre>{@code
     * BatchStage<Entry<Long, Tuple2<Long, Long>>> aggregated = pageVisits
     *         .groupingKey(PageVisit::getUserId)
     *         .aggregate2(addToCarts.groupingKey(AddToCart::getUserId),
     *                 aggregateOperation2(
     *                         AggregateOperations.counting(),
     *                         AggregateOperations.counting())
     *         );
     * }</pre>
     * This variant requires you to provide a two-input aggregate operation. If
     * you can express your logic in terms of two single-input aggregate
     * operations, one for each input stream, then you should use {@link
     * #aggregate2(AggregateOperation1, BatchStageWithKey, AggregateOperation1)
     * stage0.aggregate2(aggrOp0, stage1, aggrOp1)} because it offers a simpler
     * API. Use this variant only when your aggregate operation must combine
     * the input streams into the same accumulator.
     * <p>
     * This operation is subject to memory limits. See {@link
     * InstanceConfig#setMaxProcessorAccumulatedRecords(long)} for more
     * information.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <R> type of the aggregation result
     */
    @Nonnull
    <T1, R> BatchStage<Entry<K, R>> aggregate2(
            @Nonnull BatchStageWithKey<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, ?, R> aggrOp
    );

    /**
     * Attaches a stage that performs the given cogroup-and-aggregate
     * transformation of the items from both this stage and {@code stage1}
     * you supply. For each distinct grouping key it performs the supplied
     * aggregate operation across all the items sharing that key. It
     * performs the aggregation separately for each input stage: {@code
     * aggrOp0} on this stage and {@code aggrOp1} on {@code stage1}. Once it
     * has received all the items, it emits for each distinct key a {@code
     * Map.Entry(key, Tuple2(result0, result1))}.
     * <p>
     * Sample usage:
     * <pre>{@code
     * BatchStage<Entry<Long, Tuple2<Long, Long>>> aggregated = pageVisits
     *         .groupingKey(PageVisit::getUserId)
     *         .aggregate2(
     *                 AggregateOperations.counting(),
     *                 addToCarts.groupingKey(AddToCart::getUserId),
     *                 AggregateOperations.counting()
     *         );
     * }</pre>
     * <p>
     * This operation is subject to memory limits. See {@link
     * InstanceConfig#setMaxProcessorAccumulatedRecords(long)} for more
     * information.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     *
     * @param aggrOp0 aggregate operation to perform on this stage
     * @param stage1 the other stage
     * @param aggrOp1 aggregate operation to perform on the other stage
     * @param <R0> type of the aggregation result for stream-0
     * @param <T1> type of items in {@code stage1}
     * @param <R1> type of the aggregation result for stream-1
     */
    @Nonnull
    default <T1, R0, R1> BatchStage<Entry<K, Tuple2<R0, R1>>> aggregate2(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> aggrOp0,
            @Nonnull BatchStageWithKey<? extends T1, ? extends K> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> aggrOp1
    ) {
        AggregateOperation2<? super T, ? super T1, ?, Tuple2<R0, R1>> aggrOp =
                aggregateOperation2(aggrOp0, aggrOp1, Tuple2::tuple2);
        return aggregate2(stage1, aggrOp);
    }

    /**
     * Attaches a stage that performs the given cogroup-and-aggregate operation
     * over the items from this stage as well as {@code stage1} and {@code
     * stage2} you supply. It emits one key-value pair (in a {@code Map.Entry})
     * for each distinct key it observes in its input. The value is the result
     * of the aggregate operation across all the items with the given grouping
     * key.
     * <p>
     * Sample usage:
     * <pre>{@code
     * BatchStage<Entry<Long, Tuple3<Long, Long, Long>>> aggregated = pageVisits
     *         .groupingKey(PageVisit::getUserId)
     *         .aggregate3(
     *                 addToCarts.groupingKey(AddToCart::getUserId),
     *                 payments.groupingKey(Payment::getUserId),
     *                 aggregateOperation3(
     *                         AggregateOperations.counting(),
     *                         AggregateOperations.counting(),
     *                         AggregateOperations.counting())
     *         );
     * }</pre>
     * This variant requires you to provide a three-input aggregate operation.
     * If you can express your logic in terms of three single-input aggregate
     * operations, one for each input stream, then you should use {@link
     * #aggregate3(AggregateOperation1, BatchStageWithKey, AggregateOperation1,
     *             BatchStageWithKey, AggregateOperation1)
     * stage0.aggregate2(aggrOp0, stage1, aggrOp1, stage2, aggrOp2)} because it
     * offers a simpler API. Use this variant only when your aggregate
     * operation must combine the input streams into the same accumulator.
     * <p>
     * This operation is subject to memory limits. See {@link
     * InstanceConfig#setMaxProcessorAccumulatedRecords(long)} for more
     * information.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     *
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <T2> type of items in {@code stage2}
     * @param <R> type of the aggregation result
     */
    @Nonnull
    <T1, T2, R> BatchStage<Entry<K, R>> aggregate3(
            @Nonnull BatchStageWithKey<T1, ? extends K> stage1,
            @Nonnull BatchStageWithKey<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, ?, ? extends R> aggrOp
    );

    /**
     * Attaches a stage that performs the given cogroup-and-aggregate
     * transformation of the items from this stage as well as {@code stage1}
     * and {@code stage2} you supply. For each distinct grouping key it
     * observes in the input, it performs the supplied aggregate operation
     * across all the items sharing that key. It performs the aggregation
     * separately for each input stage: {@code aggrOp0} on this stage, {@code
     * aggrOp1} on {@code stage1} and {@code aggrOp2} on {@code stage2}. Once
     * it has received all the items, it emits for each distinct key a {@code
     * Map.Entry(key, Tuple3(result0, result1, result2))}.
     * <p>
     * Sample usage:
     * <pre>{@code
     * BatchStage<Entry<Long, Tuple3<Long, Long, Long>>> aggregated = pageVisits
     *         .groupingKey(PageVisit::getUserId)
     *         .aggregate3(
     *                 AggregateOperations.counting(),
     *                 addToCarts.groupingKey(AddToCart::getUserId),
     *                 AggregateOperations.counting(),
     *                 payments.groupingKey(Payment::getUserId),
     *                 AggregateOperations.counting()
     *         );
     * }</pre>
     * <p>
     * This operation is subject to memory limits. See {@link
     * InstanceConfig#setMaxProcessorAccumulatedRecords(long)} for more
     * information.
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
     * @param <R0> type of the aggregation result for stream-0
     * @param <R1> type of the aggregation result for stream-1
     * @param <R2> type of the aggregation result for stream-2
     */
    @Nonnull
    default <T1, T2, R0, R1, R2> BatchStage<Entry<K, Tuple3<R0, R1, R2>>> aggregate3(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> aggrOp0,
            @Nonnull BatchStageWithKey<T1, ? extends K> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> aggrOp1,
            @Nonnull BatchStageWithKey<T2, ? extends K> stage2,
            @Nonnull AggregateOperation1<? super T2, ?, ? extends R2> aggrOp2
    ) {
        AggregateOperation3<T, T1, T2, ?, Tuple3<R0, R1, R2>> aggrOp =
                aggregateOperation3(aggrOp0, aggrOp1, aggrOp2, Tuple3::tuple3);
        return aggregate3(stage1, stage2, aggrOp);
    }

    /**
     * Offers a step-by-step API to build a pipeline stage that co-aggregates
     * the data from several input stages. The current stage will be already
     * registered with the builder you get. You supply an aggregate operation
     * for each input stage and in the output you get the individual
     * aggregation results in a {@code Map.Entry(key, itemsByTag)}. Use the tag
     * you get from {@link AggregateBuilder#add builder.add(stageN)} to
     * retrieve the aggregated result for that stage. Use {@link
     * AggregateBuilder#tag0() builder.tag0()} as the tag of this stage. You
     * will also be able to supply a function to the builder that immediately
     * transforms the {@code ItemsByTag} to the desired output type.
     * <p>
     * This example reads from three sources that produce {@code
     * Map.Entry<String, Long>}. It groups by entry key and then counts the
     * items in stage-0, sums those in stage-1 and takes the average of those
     * in stage-2:
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     * StageWithGrouping<Entry<String, Long>, String> stage0 =
     *         p.readFrom(source0).groupingKey(Entry::getKey);
     * StageWithGrouping<Entry<String, Long>, String> stage1 =
     *         p.readFrom(source1).groupingKey(Entry::getKey);
     * StageWithGrouping<Entry<String, Long>, String> stage2 =
     *         p.readFrom(source2).groupingKey(Entry::getKey);
     *
     * GroupAggregateBuilder<String, Long> b = stage0.aggregateBuilder(
     *         AggregateOperations.counting());
     * Tag<Long> tag0 = b.tag0();
     * Tag<Long> tag1 = b.add(stage1,
     *         AggregateOperations.summingLong(Entry::getValue));
     * Tag<Double> tag2 = b.add(stage2,
     *         AggregateOperations.averagingLong(Entry::getValue));
     * BatchStage<Entry<String, ItemsByTag>> aggregated = b.build();
     * aggregated.map(e -> String.format(
     *         "Key %s, count of stage0: %d, sum of stage1: %d, average of stage2: %f",
     *         e.getKey(),
     *         e.getValue().get(tag0), e.getValue().get(tag1), e.getValue().get(tag2))
     * );
     *}</pre>
     * <p>
     * This operation is subject to memory limits. See {@link
     * InstanceConfig#setMaxProcessorAccumulatedRecords(long)} for more
     * information.
     */
    @Nonnull
    default <R0> GroupAggregateBuilder<K, R0> aggregateBuilder(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> aggrOp0
    ) {
        return new GroupAggregateBuilder<>(this, aggrOp0);
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
     * To add the other stages, call {@link GroupAggregateBuilder1#add
     * add(stage)}. Collect all the tags returned from {@code add()} and use
     * them when building the aggregate operation. Retrieve the tag of the
     * first stage (from which you obtained this builder) by calling {@link
     * GroupAggregateBuilder1#tag0()}.
     * <p>
     * This example takes three streams of {@code Map.Entry<String, Long>} and,
     * for each string key, counts the distinct {@code Long} values across all
     * input streams:
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     *
     * StageWithGrouping<Entry<String, Long>, String> stage0 =
     *         p.readFrom(source0).groupingKey(Entry::getKey);
     * StageWithGrouping<Entry<String, Long>, String> stage1 =
     *         p.readFrom(source1).groupingKey(Entry::getKey);
     * StageWithGrouping<Entry<String, Long>, String> stage2 =
     *         p.readFrom(source2).groupingKey(Entry::getKey);
     *
     * GroupAggregateBuilder1<Entry<String, Long>, String> b = stage0.aggregateBuilder();
     * Tag<Entry<String, Long>> tag0 = b.tag0();
     * Tag<Entry<String, Long>> tag1 = b.add(stage1);
     * Tag<Entry<String, Long>> tag2 = b.add(stage2);
     * BatchStage<Entry<String, Integer>> aggregated = b.build(AggregateOperation
     *         .withCreate(HashSet<Long>::new)
     *         .andAccumulate(tag0, (acc, item) -> acc.add(item.getValue()))
     *         .andAccumulate(tag1, (acc, item) -> acc.add(item.getValue()))
     *         .andAccumulate(tag2, (acc, item) -> acc.add(item.getValue()))
     *         .andCombine(HashSet::addAll)
     *         .andFinish(HashSet::size));
     * }</pre>
     * <p>
     * This operation is subject to memory limits. See {@link
     * InstanceConfig#setMaxProcessorAccumulatedRecords(long)} for more
     * information.
     */
    @Nonnull
    default GroupAggregateBuilder1<T, K> aggregateBuilder() {
        return new GroupAggregateBuilder1<>(this);
    }

    @Nonnull @Override
    default <R> BatchStage<R> customTransform(
            @Nonnull String stageName,
            @Nonnull SupplierEx<Processor> procSupplier
    ) {
        return customTransform(stageName, ProcessorMetaSupplier.of(procSupplier));
    }

    @Nonnull @Override
    default <R> BatchStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorSupplier procSupplier) {
        return customTransform(stageName, ProcessorMetaSupplier.of(procSupplier));
    }

    @Nonnull @Override
    <R> BatchStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorMetaSupplier procSupplier);
}
