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

package com.hazelcast.jet;

import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.processor.InsertPunctuationP;
import com.hazelcast.jet.impl.processor.SessionWindowP;
import com.hazelcast.jet.impl.processor.SlidingWindowP;

import javax.annotation.Nonnull;

import java.util.Map.Entry;

import static com.hazelcast.jet.TimestampKind.EVENT;
import static com.hazelcast.jet.function.DistributedFunction.identity;

/**
 * Contains factory methods for processors that perform an aggregating
 * operation over stream items grouped by an arbitrary key and an event
 * timestamp-based window. There are two main aggregation setups:
 * single-stage and two-stage.
 *
 * <h1>Single-stage aggregation</h1>
 *
 * This is the basic setup where all the aggregation steps happen in one
 * vertex. The input must be properly partitioned and distributed. If the
 * window is non-aligned (e.g., session-based, trigger-based, etc.) this is
 * the only choice. In the case of aligned windows it is the best choice if
 * the source is already partitioned by the grouping key because the
 * inbound edge will not have to be distributed. If the input stream needs
 * repartitioning, this setup will incur heavier network traffic than the
 * two-stage setup due to the need for a distributed-partitioned edge.
 * However, it will use less memory because each member keeps track only of
 * the keys belonging to its own partitions. This is the expected DAG:
 *
 * <pre>
 *               -------------------------
 *              | source with punctuation |
 *               -------------------------
 *                         |
 *                         | (partitioned edge, distributed as needed)
 *                         V
 *                 --------------------
 *                |    single-stage    |
 *                |  window aggregator |
 *                 --------------------
 *                         |
 *                         | (local edge)
 *                         V
 *             ----------------------------
 *            | sink or further processing |
 *             ----------------------------
 * </pre>
 *
 * <h1>Two-stage aggregation</h1>
 *
 * This setup can be used only for aligned window aggregation
 * (sliding/tumbling windows). The first stage applies just the
 * {@link AggregateOperation#accumulateItemF() accumulate} aggregation
 * primitive and the second stage does {@link
 * AggregateOperation#combineAccumulatorsF() combine} and {@link
 * AggregateOperation#finishAccumulationF() finish}.
 * <p>
 * The essential property of this setup is that the edge leading to the
 * first stage is local-partitioned, incurring no network traffic, and
 * only the edge from the first to the second stage is distributed. There
 * is only one item per key per frame traveling on this edge. Compared
 * to the single-stage setup this can dramatically reduce network traffic,
 * but it needs more memory to keep track of all keys on each cluster
 * member. The DAG should look like the following:
 *
 * <pre>
 *             -------------------------
 *            | source with punctuation |
 *             -------------------------
 *                        |
 *                        | (local partitioned edge)
 *                        V
 *            ---------------------------
 *           | groupByFrameAndAccumulate |
 *            ---------------------------
 *                        |
 *                        | (distributed partitioned edge)
 *                        V
 *             ------------------------
 *            | combineToSlidingWindow |
 *             ------------------------
 *                        |
 *                        | (local edge)
 *                        V
 *           ----------------------------
 *          | sink or further processing |
 *           ----------------------------
 * </pre>
 *
 * To get consistent results the same {@link WindowDefinition} and {@link
 * AggregateOperation} must be used for both stages.
 *
 */
public final class WindowingProcessors {

    private WindowingProcessors() {
    }

    /**
     * Returns a supplier of processor that inserts
     * {@link com.hazelcast.jet.Punctuation punctuation} into a data
     * (sub)stream. The value of the punctuation is determined by a separate
     * policy object of type {@link PunctuationPolicy}.
     *
     * @param <T> the type of stream item
     */
    @Nonnull
    public static <T> DistributedSupplier<Processor> insertPunctuation(
            @Nonnull DistributedToLongFunction<T> getTimestampF,
            @Nonnull DistributedSupplier<PunctuationPolicy> newPuncPolicyF
    ) {
        return () -> new InsertPunctuationP<>(getTimestampF, newPuncPolicyF.get());
    }

    /**
     * Returns a supplier of processor that aggregates events into a sliding
     * window in a single stage (see the {@link WindowingProcessors class
     * Javadoc} for an explanation of aggregation stages). The processor groups
     * items by the grouping key (as obtained from the given key-extracting
     * function) and by <em>frame</em>, which is a range of timestamps equal to
     * the sliding step. It emits sliding window results labeled with the
     * timestamp denoting the window's end time. This timestamp is equal to the
     * exclusive upper bound of timestamps belonging to the window.
     * <p>
     * When the processor receives a punctuation with a given {@code puncVal},
     * it emits the result of aggregation for all positions of the sliding
     * window with {@code windowTimestamp <= puncVal}. It computes the window
     * result by combining the partial results of the frames belonging to it
     * and finally applying the {@code finish} aggregation primitive. After this
     * it deletes from storage all the frames that trail behind the emitted
     * windows. The type of emitted items is {@link TimestampedEntry
     * TimestampedEntry&lt;K, A>} so there is one item per key per window position.
     */
    @Nonnull
    public static <T, K, A, R> DistributedSupplier<Processor> aggregateToSlidingWindow(
            @Nonnull DistributedFunction<? super T, K> getKeyF,
            @Nonnull DistributedToLongFunction<? super T> getTimestampF,
            @Nonnull TimestampKind timestampKind,
            @Nonnull WindowDefinition windowDef,
            @Nonnull AggregateOperation<? super T, A, R> aggregateOperation
    ) {
        return WindowingProcessors.<T, K, A, R>aggregateByKeyAndWindow(
                getKeyF,
                getTimestampF,
                timestampKind,
                windowDef,
                aggregateOperation
        );
    }

    /**
     * Returns a supplier of the first-stage processor in a two-stage sliding
     * window aggregation setup (see the {@link WindowingProcessors class
     * Javadoc} for an explanation of aggregation stages). The processor groups
     * items by the grouping key (as obtained from the given key-extracting
     * function) and by <em>frame</em>, which is a range of timestamps equal to
     * the sliding step. It applies the {@link
     * AggregateOperation#accumulateItemF() accumulate} aggregation primitive to
     * each key-frame group.
     * <p>
     * The frame is identified by the timestamp denoting its end time, which is
     * the exclusive upper bound of its timestamp range. {@link
     * WindowDefinition#higherFrameTs(long)} maps the event timestamp to the
     * timestamp of the frame it belongs to.
     * <p>
     * When the processor receives a punctuation with a given {@code puncVal},
     * it emits the current accumulated state of all frames with {@code
     * timestamp <= puncVal} and deletes these frames from its storage.
     * The type of emitted items is {@link TimestampedEntry
     * TimestampedEntry&lt;K, A>} so there is one item per key per frame.
     *
     * @param <T> input item type
     * @param <K> type of key returned from {@code getKeyF}
     * @param <A> type of accumulator returned from {@code aggregateOperation.
     *            createAccumulatorF()}
     */
    @Nonnull
    public static <T, K, A> DistributedSupplier<Processor> groupByFrameAndAccumulate(
            @Nonnull DistributedFunction<? super T, K> getKeyF,
            @Nonnull DistributedToLongFunction<? super T> getTimestampF,
            @Nonnull TimestampKind timestampKind,
            @Nonnull WindowDefinition windowDef,
            @Nonnull AggregateOperation<? super T, A, ?> aggregateOperation
    ) {
        WindowDefinition tumblingByFrame = windowDef.toTumblingByFrame();
        return WindowingProcessors.<T, K, A, A>aggregateByKeyAndWindow(
                getKeyF,
                getTimestampF,
                timestampKind,
                tumblingByFrame,
                aggregateOperation.withFinish(identity())
        );
    }

    /**
     * Returns a supplier of the second-stage processor in a two-stage sliding
     * window aggregation setup (see the {@link WindowingProcessors class
     * Javadoc} for an explanation of aggregation stages). It applies the
     * {@link AggregateOperation#combineAccumulatorsF() combine} aggregation
     * primitive to frames received from several upstream instances of {@link
     * #groupByFrameAndAccumulate(DistributedFunction, DistributedToLongFunction,
     * TimestampKind, WindowDefinition, AggregateOperation)
     * groupByFrameAndAccumulate()}. It emits sliding window results labeled with
     * the timestamp denoting the window's end time. This timestamp is equal to
     * the exclusive upper bound of timestamps belonging to the window.
     * <p>
     * When the processor receives a punctuation with a given {@code puncVal},
     * it emits the result of aggregation for all positions of the sliding
     * window with {@code windowTimestamp <= puncVal}. It computes the window
     * result by combining the partial results of the frames belonging to it
     * and finally applying the {@code finish} aggregation primitive. After this
     * it deletes from storage all the frames that trail behind the emitted
     * windows. The type of emitted items is {@link TimestampedEntry
     * TimestampedEntry&lt;K, A>} so there is one item per key per window position.
     *
     * @param <A> type of the accumulator
     * @param <R> type of the finished result returned from {@code aggregateOperation.
     *            finishAccumulationF()}
     */
    @Nonnull
    public static <K, A, R> DistributedSupplier<Processor> combineToSlidingWindow(
            @Nonnull WindowDefinition windowDef,
            @Nonnull AggregateOperation<?, A, R> aggregateOperation
    ) {
        return aggregateByKeyAndWindow(
                TimestampedEntry<K, A>::getKey,
                TimestampedEntry::getTimestamp,
                TimestampKind.FRAME,
                windowDef,
                withCombiningAccumulate(aggregateOperation)
        );
    }

    /**
     * Returns a supplier of processor that performs a general
     * group-by-key-and-window operation and applies the provided aggregate
     * operation on groups.
     *
     * @param getKeyF function that extracts the grouping key from the input item
     * @param getTimestampF function that extracts the timestamp from the input item
     * @param timestampKind the kind of timestamp extracted by {@code getTimestampF}: either the
     *                      event timestamp or the frame timestamp
     * @param windowDef definition of the window to compute
     * @param aggregateOperation aggregate operation to perform on each group in a window
     * @param <T> type of stream item
     * @param <K> type of grouping key
     * @param <A> type of the aggregate operation's accumulator
     * @param <R> type of the aggregated result
     */
    @Nonnull
    private static <T, K, A, R> DistributedSupplier<Processor> aggregateByKeyAndWindow(
            @Nonnull DistributedFunction<? super T, K> getKeyF,
            @Nonnull DistributedToLongFunction<? super T> getTimestampF,
            @Nonnull TimestampKind timestampKind,
            @Nonnull WindowDefinition windowDef,
            @Nonnull AggregateOperation<? super T, A, R> aggregateOperation
    ) {
        return () -> new SlidingWindowP<T, A, R>(
                getKeyF,
                timestampKind == EVENT
                        ? item -> windowDef.higherFrameTs(getTimestampF.applyAsLong(item))
                        : getTimestampF,
                windowDef,
                aggregateOperation);
    }

    /**
     * Returns a supplier of processor that aggregates events into session
     * windows. Events and windows under different grouping keys are treated
     * independently.
     * <p>
     * The functioning of this processor is easiest to explain in terms of
     * the <em>event interval</em>: the range {@code [timestamp, timestamp +
     * sessionTimeout]}. Initially an event causes a new session window to be
     * created, covering exactly the event interval. A following event under
     * the same key belongs to this window iff its interval overlaps it. The
     * window is extended to cover the entire interval of the new event. The
     * event may happen to belong to two existing windows if its interval
     * bridges the gap between them; in that case they are combined into one.
     *
     * @param sessionTimeout     maximum gap between consecutive events in the same session window
     * @param getTimestampF      function to extract the timestamp from the item
     * @param getKeyF            function to extract the grouping key from the item
     * @param aggregateOperation contains aggregation logic
     *
     * @param <T> type of the stream event
     * @param <K> type of the item's grouping key
     * @param <A> type of the container of the accumulated value
     * @param <R> type of the session window's result value
     */
    @Nonnull
    public static <T, K, A, R> DistributedSupplier<Processor> sessionWindow(
            long sessionTimeout,
            @Nonnull DistributedToLongFunction<? super T> getTimestampF,
            @Nonnull DistributedFunction<? super T, K> getKeyF,
            @Nonnull AggregateOperation<? super T, A, R> aggregateOperation
    ) {
        return () -> new SessionWindowP<>(sessionTimeout, getTimestampF, getKeyF, aggregateOperation);
    }

    static <A, R> AggregateOperation<Entry<?, A>, A, R> withCombiningAccumulate(
            @Nonnull AggregateOperation<?, A, R> aggregateOperation
    ) {
        return aggregateOperation.withAccumulate(
                (A acc, Entry<?, A> e) ->
                        aggregateOperation.combineAccumulatorsF().accept(acc, e.getValue()));
    }
}
