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
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBiPredicate;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedTriFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation2;
import static com.hazelcast.jet.aggregate.AggregateOperations.aggregateOperation3;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;

/**
 * Represents a stage in a distributed computation {@link Pipeline
 * pipeline} that will observe a finite amount of data (a batch). It
 * accepts input from its upstream stages (if any) and passes its output
 * to its downstream stages.
 *
 * @param <T> the type of items coming out of this stage
 */
public interface BatchStage<T> extends GeneralStage<T> {

    /**
     * {@inheritDoc}
     */
    @Nonnull
    <K> StageWithGrouping<T, K> groupingKey(@Nonnull DistributedFunction<? super T, ? extends K> keyFn);

    @Nonnull @Override
    <R> BatchStage<R> map(@Nonnull DistributedFunction<? super T, ? extends R> mapFn);

    @Nonnull @Override
    BatchStage<T> filter(@Nonnull DistributedPredicate<T> filterFn);

    @Nonnull @Override
    <R> BatchStage<R> flatMap(@Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn);

    @Nonnull @Override
    <C, R> BatchStage<R> mapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends R> mapFn
    );

    @Nonnull @Override
    <C> BatchStage<T> filterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiPredicate<? super C, ? super T> filterFn
    );

    @Nonnull @Override
    <C, R> BatchStage<R> flatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    );

    /**
     * Attaches a stage that emits just the items that are distinct according
     * to their definition of equality ({@code equals} and {@code hashCode}).
     * There is no guarantee which one of equal items it will emit.
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
            @Nonnull DistributedBiFunction<T, T1, R> mapToOutputFn
    );

    @Nonnull @Override
    <K1, K2, T1_IN, T2_IN, T1, T2, R> BatchStage<R> hashJoin2(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull DistributedTriFunction<T, T1, T2, R> mapToOutputFn
    );

    @Nonnull @Override
    default HashJoinBuilder<T> hashJoinBuilder() {
        return new HashJoinBuilder<>(this);
    }

    /**
     * Attaches a stage that performs the given aggregate operation over all
     * the items it receives. The aggregating stage emits a single item.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param <A> the type of the accumulator used by the aggregate operation
     * @param <R> the type of the result
     */
    @Nonnull
    <A, R> BatchStage<R> aggregate(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
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
     * operation that combines the input streams into the same
     * accumulator.
     * <p>
     * The returned stage emits a single item.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <A> type of the accumulator used by the aggregate operation
     * @param <R> type of the result
     */
    @Nonnull
    <T1, A, R> BatchStage<R> aggregate2(
            @Nonnull BatchStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, ? extends R> aggrOp);

    /**
     * Attaches a stage that co-aggregates the data from this and the supplied
     * stage by performing a separate aggregate operation on each and then
     * passing both results to {@code mapToOutputFn}, which transforms them
     * into the final output.
     * <p>
     * The returned stage emits a single item.
     *
     * @param aggrOp0 aggregate operation to perform on this stage
     * @param stage1 the other stage
     * @param aggrOp1 aggregate operation to perform on the other stage
     * @param mapToOutputFn function to apply to the aggregated results
     * @param <T1> type of the items in the other stage
     * @param <R0> type of the aggregated result for this stage
     * @param <R1> type of the aggregated result for the other stage
     * @param <OUT> the output item type
     */
    @Nonnull
    default <T1, R0, R1, OUT> BatchStage<OUT> aggregate2(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> aggrOp0,
            @Nonnull BatchStage<T1> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> aggrOp1,
            @Nonnull DistributedBiFunction<? super R0, ? super R1, ? extends OUT> mapToOutputFn
    ) {
        return aggregate2(stage1, aggregateOperation2(aggrOp0, aggrOp1, mapToOutputFn));
    }

    /**
     * Attaches a stage that co-aggregates the data from this and the supplied
     * stage by performing a separate aggregate operation on each and emitting
     * a {@link Tuple2} with their results.
     * <p>
     * The returned stage emits a single item.
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
        return aggregate2(stage1, aggregateOperation2(aggrOp0, aggrOp1, Tuple2::tuple2));
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
     * The aggregating stage emits a single item.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp the aggregate operation to perform
     * @param <T1> type of items in {@code stage1}
     * @param <T2> type of items in {@code stage2}
     * @param <A> type of the accumulator used by the aggregate operation
     * @param <R> type of the result
     */
    @Nonnull
    <T1, T2, A, R> BatchStage<R> aggregate3(
            @Nonnull BatchStage<T1> stage1,
            @Nonnull BatchStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, ? extends R> aggrOp);

    /**
     * Attaches a stage that co-aggregates the data from this and the two
     * supplied stages by performing a separate aggregate operation on each and
     * then passing all three results to {@code mapToOutputFn}, which
     * transforms them into the final output.
     * <p>
     * The returned stage emits a single item.
     *
     * @param aggrOp0 aggregate operation to perform on this stage
     * @param stage1 the first additional stage
     * @param aggrOp1 aggregate operation to perform on {@code stage1}
     * @param stage2 the second additional stage
     * @param aggrOp2 aggregate operation to perform on {@code stage2}
     * @param mapToOutputFn function to apply to the aggregated results
     * @param <T1> type of the items in {@code stage1}
     * @param <T2> type of the items in {@code stage2}
     * @param <R0> type of the aggregated result for this stage
     * @param <R1> type of the aggregated result for {@code stage1}
     * @param <R2> type of the aggregated result for {@code stage2}
     * @param <OUT> the output item type
     */
    @Nonnull
    default <T1, T2, R0, R1, R2, OUT> BatchStage<OUT> aggregate3(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R0> aggrOp0,
            @Nonnull BatchStage<T1> stage1,
            @Nonnull AggregateOperation1<? super T1, ?, ? extends R1> aggrOp1,
            @Nonnull BatchStage<T2> stage2,
            @Nonnull AggregateOperation1<? super T2, ?, ? extends R2> aggrOp2,
            @Nonnull DistributedTriFunction<? super R0, ? super R1, ? super R2, ? extends OUT> mapToOutputFn
    ) {
        return aggregate3(stage1, stage2, aggregateOperation3(aggrOp0, aggrOp1, aggrOp2, mapToOutputFn));
    }

    /**
     * Attaches a stage that co-aggregates the data from this and the two
     * supplied stages by performing a separate aggregate operation on each and
     * emitting a {@link Tuple3} with their results.
     * <p>
     * The returned stage emits a single item.
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
     * transforms the {@code ItemsByTag} to the desired output type.
     * <p>
     * This example counts the items in stage-0, sums those in stage-1 and takes
     * the average of those in stage-2:
     * <pre>{@code
     * BatchStage<Long> stage0 = p.drawFrom(source0);
     * BatchStage<Long> stage1 = p.drawFrom(source1);
     * BatchStage<Long> stage2 = p.drawFrom(source2);
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
     * BatchStage<String> stage0 = p.drawFrom(source0);
     * BatchStage<String> stage1 = p.drawFrom(source1);
     * BatchStage<String> stage2 = p.drawFrom(source2);
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
            @Nonnull DistributedPredicate<? super T> shouldLogFn,
            @Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn
    );

    @Nonnull @Override
    default BatchStage<T> peek(@Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn) {
        return (BatchStage<T>) GeneralStage.super.peek(toStringFn);
    }

    @Nonnull @Override
    <R> BatchStage<R> customTransform(
            @Nonnull String stageName, @Nonnull DistributedSupplier<Processor> procSupplier);

    @Nonnull @Override
    BatchStage<T> setLocalParallelism(int localParallelism);

    @Nonnull @Override
    BatchStage<T> setName(@Nullable String name);
}
