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
import com.hazelcast.function.ComparatorEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.map.IMap;
import com.hazelcast.replicatedmap.ReplicatedMap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.function.Functions.wholeItem;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;

/**
 * A stage in a distributed computation {@link Pipeline pipeline} that will
 * observe a finite amount of data (a batch). It accepts input from its
 * upstream stages (if any) and passes its output to its downstream stages.
 *
 * @param <T> the type of items coming out of this stage
 *
 * @since Jet 3.0
 */
public interface BatchStage<T> extends GeneralStage<T> {

    /**
     * {@inheritDoc}
     */
    @Nonnull @Override
    <K> BatchStageWithKey<T, K> groupingKey(@Nonnull FunctionEx<? super T, ? extends K> keyFn);

    @Nonnull @Override
    <K> BatchStage<T> rebalance(@Nonnull FunctionEx<? super T, ? extends K> keyFn);

    @Nonnull @Override
    BatchStage<T> rebalance();

    /**
     * Attaches a stage that sorts the input items according to their natural order.
     * <p>
     * Sample usage:
     * <pre>{@code
     * items.sort()
     * }</pre>
     * <p>
     * This operation is subject to memory limits. See {@link
     * InstanceConfig#setMaxProcessorAccumulatedRecords(long)} for more
     * information.
     *
     * @return the newly attached stage
     * @see ComparatorEx#naturalOrder
     * @since Jet 4.3
     */
    @Nonnull
    BatchStage<T> sort();

    /**
     * Attaches a stage that sorts the input items according to the supplied
     * comparator.
     * <p>
     * For example, you can sort a stream of trade events based on their
     * stock ticker:
     * <pre>{@code
     * BatchStage<Trade> trades = pipeline.readFrom(tradeSource);
     * BatchStage<Trade> sortedTrades = trades.sort(ComparatorEx.comparing(Trade::ticker));
     * }</pre>
     * <p>
     * This operation is subject to memory limits. See {@link
     * InstanceConfig#setMaxProcessorAccumulatedRecords(long)} for more
     * information.
     *
     * @param comparator the user-provided comparator that will be used for
     *     sorting. It must be stateless and {@linkplain Processor#isCooperative()
     *     cooperative}.
     * @return the newly attached stage
     * @since Jet 4.3
     */
    @Nonnull
    BatchStage<T> sort(@Nonnull ComparatorEx<? super T> comparator);

    @Nonnull @Override
    <R> BatchStage<R> map(@Nonnull FunctionEx<? super T, ? extends R> mapFn);

    @Nonnull @Override
    BatchStage<T> filter(@Nonnull PredicateEx<T> filterFn);

    @Nonnull @Override
    <R> BatchStage<R> flatMap(@Nonnull FunctionEx<? super T, ? extends Traverser<R>> flatMapFn);

    @Nonnull @Override
    <S, R> BatchStage<R> mapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    );

    @Nonnull @Override
    <S> BatchStage<T> filterStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn);

    @Nonnull @Override
    <S, R> BatchStage<R> flatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn);

    @Nonnull @Override
    default <A, R> BatchStage<R> rollingAggregate(@Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp) {
        return (BatchStage<R>) GeneralStage.super.<A, R>rollingAggregate(aggrOp);
    }

    @Nonnull @Override
    <S, R> BatchStage<R> mapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    );

    @Nonnull @Override
    default <S, R> BatchStage<R> mapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<R>> mapAsyncFn
    ) {
        return (BatchStage<R>) GeneralStage.super.mapUsingServiceAsync(serviceFactory, mapAsyncFn);
    }

    @Nonnull @Override
    <S, R> BatchStage<R> mapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxConcurrentOps,
            boolean preserveOrder,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<R>> mapAsyncFn
    );

    @Nonnull @Override
    <S> BatchStage<T> filterUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    );

    @Nonnull @Override
    <S, R> BatchStage<R> flatMapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    );

    @Nonnull @Override
    <S, R> BatchStage<R> mapUsingServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<List<R>>> mapAsyncFn
    );

    @Nonnull @Override
    default <K, V, R> BatchStage<R> mapUsingReplicatedMap(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> lookupKeyFn,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return (BatchStage<R>) GeneralStage.super.<K, V, R>mapUsingReplicatedMap(mapName, lookupKeyFn, mapFn);
    }

    @Nonnull @Override
    default <K, V, R> BatchStage<R> mapUsingReplicatedMap(
            @Nonnull ReplicatedMap<K, V> replicatedMap,
            @Nonnull FunctionEx<? super T, ? extends K> lookupKeyFn,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return (BatchStage<R>) GeneralStage.super.<K, V, R>mapUsingReplicatedMap(replicatedMap, lookupKeyFn, mapFn);
    }

    @Nonnull @Override
    default <K, V, R> BatchStage<R> mapUsingIMap(
            @Nonnull String mapName,
            @Nonnull FunctionEx<? super T, ? extends K> lookupKeyFn,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return (BatchStage<R>) GeneralStage.super.<K, V, R>mapUsingIMap(mapName, lookupKeyFn, mapFn);
    }

    @Nonnull @Override
    default <K, V, R> BatchStage<R> mapUsingIMap(
            @Nonnull IMap<K, V> iMap,
            @Nonnull FunctionEx<? super T, ? extends K> lookupKeyFn,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return (BatchStage<R>) GeneralStage.super.<K, V, R>mapUsingIMap(iMap, lookupKeyFn, mapFn);
    }

    /**
     * Attaches a stage that emits just the items that are distinct according
     * to their definition of equality ({@code equals} and {@code hashCode}).
     * There is no guarantee which one of equal items it will emit.
     * <p>
     * This operation is subject to memory limits. See {@link
     * InstanceConfig#setMaxProcessorAccumulatedRecords(long)} for more
     * information.
     *
     * @return the newly attached stage
     */
    @Nonnull
    default BatchStage<T> distinct() {
        return groupingKey(wholeItem()).distinct();
    }

    /**
     * Attaches a stage that emits all the items from this stage as well as all
     * the items from the supplied stage. The other stage's type parameter must
     * be assignment-compatible with this stage's type parameter.
     *
     * @param other the other stage whose data to merge into this one
     * @return the newly attached stage
     */
    @Nonnull
    BatchStage<T> merge(@Nonnull BatchStage<? extends T> other);

    @Nonnull @Override
    <K, T1_IN, T1, R> BatchStage<R> hashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BiFunctionEx<T, T1, R> mapToOutputFn
    );

    @Nonnull @Override
    <K, T1_IN, T1, R> BatchStage<R> innerHashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BiFunctionEx<T, T1, R> mapToOutputFn
    );

    @Nonnull @Override
    <K1, K2, T1_IN, T2_IN, T1, T2, R> BatchStage<R> hashJoin2(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull TriFunction<T, T1, T2, R> mapToOutputFn
    );

    @Nonnull @Override
    <K1, K2, T1_IN, T2_IN, T1, T2, R> BatchStage<R> innerHashJoin2(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull TriFunction<T, T1, T2, R> mapToOutputFn
    );

    @Nonnull @Override
    default HashJoinBuilder<T> hashJoinBuilder() {
        return new HashJoinBuilder<>(this);
    }

    /**
     * Attaches a stage that performs the given aggregate operation over all
     * the items it receives. It emits a single item, the result of the
     * aggregate operation's {@link AggregateOperation#finishFn() finish}
     * primitive. The result may be {@code null} (e.g., {@link
     * AggregateOperations#maxBy} with no input), in that case the stage
     * does not produce any output.
     * <p>
     * Sample usage:
     * <pre>{@code
     * BatchStage<Integer> stage = pipeline.readFrom(TestSources.items(1, 2));
     * // Emits a single item, the number 2:
     * BatchStage<Long> count = stage.aggregate(AggregateOperations.counting());
     * }</pre>
     *
     * @see AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param <R> the type of the result
     */
    @Nonnull
    <R> BatchStage<R> aggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    );

    /**
     * Attaches a stage that performs the given aggregate operation over all
     * the items it receives from both this stage and {@code stage1} you supply.
     * This variant requires you to provide a two-input aggregate operation
     * (refer to its {@linkplain AggregateOperation2 Javadoc} for a simple
     * example). If you can express your logic in terms of two single-input
     * aggregate operations, one for each input stream, then you should use
     * {@link #aggregate2(AggregateOperation1, BatchStage, AggregateOperation1)
     * stage0.aggregate2(aggrOp0, stage1, aggrOp1)} because it offers a simpler
     * API and you can use the already defined single-input operations. Use
     * this variant only when you have the need to implement an aggregate
     * operation that combines the input streams into the same accumulator.
     * <p>
     * The stage emits a single item, the result of the aggregate operation's
     * {@link AggregateOperation#finishFn() finish} primitive. The result may
     * be {@code null}, in that case the stage does not produce any output.
     * <p>
     * Sample usage:
     * <pre>{@code
     * BatchStage<Tuple2<Long, Long>> counts = pageVisits.aggregate2(addToCarts,
     *         AggregateOperations.aggregateOperation2(
     *                 AggregateOperations.counting(),
     *                 AggregateOperations.counting())
     *         );
     * }</pre>
     *
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <R> type of the result
     *
     * @see AggregateOperations AggregateOperations
     */
    @Nonnull
    <T1, R> BatchStage<R> aggregate2(
            @Nonnull BatchStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, ?, ? extends R> aggrOp);

    /**
     * Attaches a stage that co-aggregates the data from this and the supplied
     * stage by performing a separate aggregate operation on each and emits a
     * single {@link Tuple2} with their results.
     * <p>
     * Sample usage:
     * <pre>{@code
     * BatchStage<Tuple2<Long, Long>> counts = pageVisits.aggregate2(
     *         AggregateOperations.counting(),
     *         addToCarts, AggregateOperations.counting()
     * );
     * }</pre>
     *
     * @param aggrOp0 aggregate operation to perform on this stage
     * @param stage1 the other stage
     * @param aggrOp1 aggregate operation to perform on the other stage
     * @param <T1> type of the items in the other stage
     * @param <R0> type of the aggregated result for this stage
     * @param <R1> type of the aggregated result for the other stage
     */
    @Nonnull
    default <T1, R0, R1> BatchStage<Tuple2<R0, R1>> aggregate2(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> aggrOp0,
            @Nonnull BatchStage<T1> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> aggrOp1
    ) {
        return aggregate2(stage1, aggregateOperation2(aggrOp0, aggrOp1));
    }

    /**
     * Attaches a stage that performs the given aggregate operation over all
     * the items it receives from this stage as well as {@code stage1} and
     * {@code stage2} you supply. This variant requires you to provide a
     * three-input aggregate operation (refer to its {@linkplain
     * AggregateOperation3 Javadoc} for a simple example). If you can express
     * your logic in terms of two single-input aggregate operations, one for
     * each input stream, then you should use {@link #aggregate3(
     * AggregateOperation1, BatchStage, AggregateOperation1, BatchStage, AggregateOperation1)
     * stage0.aggregate3(aggrOp0, stage1, aggrOp1, stage2, aggrOp2)} because
     * it offers a simpler API and you can use the already defined single-input
     * operations. Use this variant only when you have the need to implement an
     * aggregate operation that combines the input streams into the same
     * accumulator.
     * <p>
     * The stage emits a single item, the result of the aggregate operation's
     * {@link AggregateOperation#finishFn() finish} primitive. The result may
     * be {@code null}, in that case the stage does not produce any output.
     * <p>
     * Sample usage:
     * <pre>{@code
     * BatchStage<Tuple3<Long, Long, Long>> counts = pageVisits.aggregate3(
     *         addToCarts,
     *         payments,
     *         AggregateOperations.aggregateOperation3(
     *                 AggregateOperations.counting(),
     *                 AggregateOperations.counting(),
     *                 AggregateOperations.counting()));
     * }</pre>
     *
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <T2> type of items in {@code stage2}
     * @param <R> type of the result
     *
     * @see AggregateOperations AggregateOperations
     */
    @Nonnull
    <T1, T2, R> BatchStage<R> aggregate3(
            @Nonnull BatchStage<T1> stage1,
            @Nonnull BatchStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, ?, ? extends R> aggrOp);


    /**
     * Attaches a stage that co-aggregates the data from this and the two
     * supplied stages by performing a separate aggregate operation on each and
     * emits a single {@link Tuple3} with their results.
     * <p>
     * Sample usage:
     * <pre>{@code
     * BatchStage<Tuple3<Long, Long, Long>> counts = pageVisits.aggregate3(
     *         AggregateOperations.counting(),
     *         addToCarts, AggregateOperations.counting(),
     *         payments, AggregateOperations.counting()
     * );
     * }</pre>
     *
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
    default <T1, T2, R0, R1, R2> BatchStage<Tuple3<R0, R1, R2>> aggregate3(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> aggrOp0,
            @Nonnull BatchStage<T1> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> aggrOp1,
            @Nonnull BatchStage<T2> stage2,
            @Nonnull AggregateOperation1<? super T2, ?, ? extends R2> aggrOp2
    ) {
        return aggregate3(stage1, stage2, aggregateOperation3(aggrOp0, aggrOp1, aggrOp2, Tuple3::tuple3));
    }

    /**
     * Offers a step-by-step API to build a pipeline stage that co-aggregates
     * the data from several input stages. The current stage will be already
     * registered with the builder you get. You supply an aggregate operation
     * for each input stage and in the output you get the individual
     * aggregation results in a {@code Map.Entry(key, itemsByTag)}. Use the tag
     * you get from {@link AggregateBuilder#add builder.add(stageN, aggrOpN)}
     * to retrieve the aggregated result for that stage. Use {@link
     * AggregateBuilder#tag0() builder.tag0()} as the tag of this stage. You
     * will also be able to supply a function to the builder that immediately
     * transforms the {@code ItemsByTag} to the desired output type. Your
     * function may return {@code null} and in that case the stage will not
     * emit anything.
     * <p>
     * This example counts the items in stage-0, sums those in stage-1 and takes
     * the average of those in stage-2:
     * <pre>{@code
     * BatchStage<Long> stage0 = p.readFrom(source0);
     * BatchStage<Long> stage1 = p.readFrom(source1);
     * BatchStage<Long> stage2 = p.readFrom(source2);
     *
     * AggregateBuilder<Long> b = stage0.aggregateBuilder(
     *         AggregateOperations.counting());
     * Tag<Long> tag0 = b.tag0();
     * Tag<Long> tag1 = b.add(stage1,
     *         AggregateOperations.summingLong(Number::longValue));
     * Tag<Double> tag2 = b.add(stage2,
     *         AggregateOperations.averagingLong(Number::longValue));
     *
     * BatchStage<ItemsByTag> aggregated = b.build();
     * aggregated.map(ibt -> String.format(
     *         "Count of stage0: %d, sum of stage1: %d, average of stage2: %f",
     *         ibt.get(tag0), ibt.get(tag1), ibt.get(tag2))
     * );
     *}</pre>
     */
    @Nonnull
    default <R0> AggregateBuilder<R0> aggregateBuilder(AggregateOperation1<? super T, ?, ? extends R0> aggrOp0) {
        return new AggregateBuilder<>(this, aggrOp0);
    }

    /**
     * Offers a step-by-step API to build a pipeline stage that co-aggregates
     * the data from several input stages. The current stage will be already
     * registered with the builder you get. The stage it builds will emit a
     * single item, the one that the aggregate operation's {@link
     * AggregateOperation#finishFn() finish} primitive returns. If it returns
     * {@code null}, the stage will not emit any output.
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
     * To add the other stages, call {@link AggregateBuilder1#add add(stage)}.
     * Collect all the tags returned from {@code add()} and use them when
     * building the aggregate operation. Retrieve the tag of the first stage
     * (from which you obtained this builder) by calling {@link
     * AggregateBuilder1#tag0()}.
     * <p>
     * This example takes three streams of strings and counts the distinct
     * strings across all of them:
     * <pre>{@code
     * Pipeline p = Pipeline.create();
     * BatchStage<String> stage0 = p.readFrom(source0);
     * BatchStage<String> stage1 = p.readFrom(source1);
     * BatchStage<String> stage2 = p.readFrom(source2);
     * AggregateBuilder1<String> b = stage0.aggregateBuilder();
     * Tag<String> tag0 = b.tag0();
     * Tag<String> tag1 = b.add(stage1);
     * Tag<String> tag2 = b.add(stage2);
     * BatchStage<Integer> aggregated = b.build(AggregateOperation
     *         .withCreate(HashSet<String>::new)
     *         .andAccumulate(tag0, (acc, item) -> acc.add(item))
     *         .andAccumulate(tag1, (acc, item) -> acc.add(item))
     *         .andAccumulate(tag2, (acc, item) -> acc.add(item))
     *         .andCombine(HashSet::addAll)
     *         .andFinish(HashSet::size));
     * }</pre>
     */
    @Nonnull
    default AggregateBuilder1<T> aggregateBuilder() {
        return new AggregateBuilder1<>(this);
    }

    @Nonnull @Override
    default BatchStage<T> peek() {
        return (BatchStage<T>) GeneralStage.super.peek();
    }

    @Nonnull @Override
    BatchStage<T> peek(
            @Nonnull PredicateEx<? super T> shouldLogFn,
            @Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn
    );

    @Nonnull @Override
    default BatchStage<T> peek(@Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn) {
        return (BatchStage<T>) GeneralStage.super.peek(toStringFn);
    }

    @Nonnull @Override
    default <R> BatchStage<R> customTransform(@Nonnull String stageName,
                                              @Nonnull SupplierEx<Processor> procSupplier) {
        return customTransform(stageName, ProcessorMetaSupplier.of(procSupplier));
    }

    @Nonnull @Override
    default <R> BatchStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorSupplier procSupplier) {
        return customTransform(stageName, ProcessorMetaSupplier.of(procSupplier));
    }

    @Nonnull @Override
    <R> BatchStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorMetaSupplier procSupplier);

    /**
     * Transforms {@code this} stage using the provided {@code transformFn} and
     * returns the transformed stage. It allows you to extract common pipeline
     * transformations into a method and then call that method without
     * interrupting the chained pipeline expression.
     * <p>
     * For example, say you have this code:
     *
     * <pre>{@code
     * BatchStage<String> input = pipeline.readFrom(textSource);
     * BatchStage<String> cleanedUp = input
     *         .map(String::toLowerCase)
     *         .filter(s -> s.startsWith("success"));
     * }</pre>
     *
     * You can capture the {@code map} and {@code filter} steps into a common
     * "cleanup" transformation:
     *
     * <pre>{@code
     * BatchStage<String> cleanUp(BatchStage<String> input) {
     *      return input.map(String::toLowerCase)
     *                  .filter(s -> s.startsWith("success"));
     * }
     * }</pre>
     *
     * Now you can insert this transformation as just another step in your
     * pipeline:
     *
     * <pre>{@code
     * BatchStage<String> tokens = pipeline
     *     .readFrom(textSource)
     *     .apply(this::cleanUp)
     *     .flatMap(line -> traverseArray(line.split("\\W+")));
     * }</pre>
     *
     * @param transformFn function to transform this stage into another stage
     * @param <R> type of the returned stage
     *
     * @since Jet 3.1
     */
    @Nonnull
    default <R> BatchStage<R> apply(@Nonnull FunctionEx<? super BatchStage<T>, ? extends BatchStage<R>> transformFn) {
        return transformFn.apply(this);
    }

    @Nonnull @Override
    BatchStage<T> setLocalParallelism(int localParallelism);

    @Nonnull @Override
    BatchStage<T> setName(@Nonnull String name);
}
