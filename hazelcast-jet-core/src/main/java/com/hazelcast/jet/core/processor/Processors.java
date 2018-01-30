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

package com.hazelcast.jet.core.processor;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.core.WindowDefinition;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.processor.AggregateP;
import com.hazelcast.jet.impl.processor.CoGroupP;
import com.hazelcast.jet.impl.processor.InsertWatermarksP;
import com.hazelcast.jet.impl.processor.SessionWindowP;
import com.hazelcast.jet.impl.processor.SlidingWindowP;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.jet.impl.util.WrappingProcessorMetaSupplier;
import com.hazelcast.jet.impl.util.WrappingProcessorSupplier;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map.Entry;

import static com.hazelcast.jet.core.TimestampKind.EVENT;
import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;

/**
 * Static utility class with factory methods for Jet processors. These
 * are meant to implement the internal vertices of the DAG; for other
 * kinds of processors refer to the {@link com.hazelcast.jet.core.processor
 * package-level documentation}.
 * <p>
 * Many of the processors deal with an aggregating operation over stream
 * items. Prior to aggregation items may be grouped by an arbitrary key
 * and/or an event timestamp-based window. There are two main aggregation
 * setups: single-stage and two-stage.
 * <p>
 * Unless specified otherwise, all functions passed to member methods must
 * be stateless.
 *
 * <h1>Single-stage aggregation</h1>
 *
 * This is the basic setup where all the aggregation steps happen in one
 * vertex. The input must be properly partitioned and distributed. For
 * non-aligned window aggregation (e.g., session-based, trigger-based,
 * etc.) this is the only choice. In the case of aligned windows it is the
 * best choice if the source is already partitioned by the grouping key
 * because the inbound edge will not have to be distributed. If the input
 * stream needs repartitioning, this setup will incur heavier network
 * traffic than the two-stage setup due to the need for a
 * distributed-partitioned edge. On the other hand, it will use less memory
 * because each member keeps track only of the keys belonging to its own
 * partitions. This is the DAG outline for the case where upstream data
 * is not localized by grouping key:
 * <pre>
 *                 -----------------
 *                | upstream vertex |
 *                 -----------------
 *                         |
 *                         | partitioned-distributed
 *                         V
 *                    -----------
 *                   | aggregate |
 *                    -----------
 * </pre>
 *
 * <h1>Two-stage aggregation</h1>
 *
 * In two-stage aggregation, the first stage applies just the
 * {@link AggregateOperation1#accumulateFn() accumulate} aggregation
 * primitive and the second stage does {@link
 * AggregateOperation1#combineFn() combine} and {@link
 * AggregateOperation1#finishFn() finish}. The essential property
 * of this setup is that the edge leading to the first stage is local,
 * incurring no network traffic, and only the edge from the first to the
 * second stage is distributed. There is only one item per group traveling on
 * the distributed edge. Compared to the single-stage setup this can
 * dramatically reduce network traffic, but it needs more memory to keep
 * track of all keys on each cluster member. This is the outline of the DAG:
 * <pre>
 *                -----------------
 *               | upstream vertex |
 *                -----------------
 *                        |
 *                        | partitioned-local
 *                        V
 *                  ------------
 *                 | accumulate |
 *                  ------------
 *                        |
 *                        | partitioned-distributed
 *                        V
 *                 ----------------
 *                | combine/finish |
 *                 ----------------
 * </pre>
 * The variants without a grouping key are equivalent to grouping by a
 * single, global key. In that case the edge towards the final-stage
 * vertex must be all-to-one and the local parallelism of the vertex must
 * be one. Unless the volume of the aggregated data is small (e.g., some
 * side branch off the main flow in the DAG), the best choice is this
 * two-stage setup:
 * <pre>
 *                -----------------
 *               | upstream vertex |
 *                -----------------
 *                        |
 *                        | local, non-partitioned
 *                        V
 *                  ------------
 *                 | accumulate |
 *                  ------------
 *                        |
 *                        | distributed, all-to-one
 *                        V
 *                 ----------------
 *                | combine/finish | localParallelism = 1
 *                 ----------------
 * </pre>
 * This will parallelize and distributed most of the processing and
 * the second-stage processor will receive just a single item from
 * each upstream processor, doing very little work.
 *
 * <h1>Overview of factory methods for aggregate operations</h1>
 * <table border="1">
 * <tr>
 *     <th></th>
 *     <th>single-stage</th>
 *     <th>stage 1/2</th>
 *     <th>stage 2/2</th>
 * </tr><tr>
 *     <th>batch,<br>no grouping</th>
 *
 *     <td>{@link #aggregateP(AggregateOperation1) aggregate()}</td>
 *     <td>{@link #accumulateP(AggregateOperation1) accumulate()}</td>
 *     <td>{@link #combineP(AggregateOperation1) combine()}</td>
 * </tr><tr>
 *     <th>batch, group by key</th>
 *
 *     <td>{@link #aggregateByKeyP(DistributedFunction, AggregateOperation1)
 *          aggregateByKey()}</td>
 *     <td>{@link #accumulateByKeyP(DistributedFunction, AggregateOperation1)
 *          accumulateByKey()}</td>
 *     <td>{@link #combineByKeyP(AggregateOperation) combineByKey()}</td>
 * </tr><tr>
 *     <th>stream, group by key<br>and aligned window</th>
 *
 *     <td>{@link #aggregateToSlidingWindowP(DistributedFunction, DistributedToLongFunction,
 *          TimestampKind, WindowDefinition, AggregateOperation1)
 *          aggregateToSlidingWindow()}</td>
 *     <td>{@link #accumulateByFrameP(DistributedFunction, DistributedToLongFunction,
 *          TimestampKind, WindowDefinition, AggregateOperation1)
 *          accumulateByFrame()}</td>
 *     <td>{@link #combineToSlidingWindowP(WindowDefinition, AggregateOperation1)
 *          combineToSlidingWindow()}</td>
 * </tr><tr>
 *     <th>stream, group by key<br>and session window</th>
 *     <td>{@link #aggregateToSessionWindowP(long, DistributedToLongFunction,
 *          DistributedFunction, AggregateOperation1)
 *          aggregateToSessionWindow()}</td>
 *     <td>N/A</td>
 *     <td>N/A</td>
 * </tr></table>
 * <p>
 * Tumbling window is a special case of sliding window with sliding step =
 * window size. To achieve the effect of aggregation without a
 * grouping key, specify {@link com.hazelcast.jet.function.DistributedFunctions#constantKey()
 * constantKey()} as the key-extracting function.
 */
public final class Processors {

    private Processors() {
    }

    /**
     * Returns a supplier of processors for a vertex that groups items by key
     * and performs the provided aggregate operation on each group. After
     * exhausting all its input it emits one {@code Map.Entry<K, R>} per
     * distinct key.
     * <p>
     * This processor has state, but does not save it to snapshot. On job
     * restart, the state will be lost.
     *
     * @param getKeyFn computes the key from the entry
     * @param aggrOp the aggregate operation to perform
     * @param <T> type of received item
     * @param <K> type of key
     * @param <A> type of accumulator returned from {@code aggregateOperation.
     *            createAccumulatorFn()}
     * @param <R> type of the finished result returned from {@code aggregateOperation.
     *            finishAccumulationFn()}
     */
    @Nonnull
    public static <T, K, A, R> DistributedSupplier<Processor> aggregateByKeyP(
            @Nonnull DistributedFunction<? super T, K> getKeyFn,
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp
    ) {
        return () -> new CoGroupP<>(getKeyFn, aggrOp);
    }

    /**
     * Returns a supplier of processors for the first-stage vertex in a
     * two-stage group-and-aggregate setup. The vertex groups items by the
     * grouping key (as obtained from the given key-extracting function) and
     * applies the {@link AggregateOperation1#accumulateFn()} accumulate}
     * primitive to each group. After exhausting all its input it emits one
     * {@code Map.Entry<K, A>} per observed key.
     * <p>
     * This processor has state, but does not save it to snapshot. On job
     * restart, the state will be lost.
     *
     * @param getKeyFn computes the key from the entry
     * @param aggrOp the aggregate operation to perform
     * @param <T> type of received item
     * @param <K> type of key
     * @param <A> type of accumulator returned from {@code aggrOp.createAccumulatorFn()}
     */
    @Nonnull
    public static <T, K, A> DistributedSupplier<Processor> accumulateByKeyP(
            @Nonnull DistributedFunction<? super T, K> getKeyFn,
            @Nonnull AggregateOperation1<? super T, A, ?> aggrOp
    ) {
        return () -> new CoGroupP<>(getKeyFn, aggrOp.withFinishFn(identity()));
    }

    /**
     * Returns a supplier of processors for a vertex that groups items by key
     * and performs the provided aggregate operation on each group. After
     * exhausting all its input it emits one {@code Map.Entry<K, R>} per
     * distinct key.
     * <p>
     * The vertex accepts input from one or more inbound edges. The type of
     * items may be different on each edge. For each edge a separate key
     * extracting function must be supplied and the aggregate operation must
     * contain a separate accumulation function for each edge.
     * <p>
     * This processor has state, but does not save it to snapshot. On job
     * restart, the state will be lost.
     *
     * @param getKeyFs functions that compute the grouping key
     * @param aggrOp the aggregate operation
     * @param <K> type of key
     * @param <A> type of accumulator returned from {@code aggrOp.createAccumulatorFn()}
     * @param <R> type of the finished result returned from {@code aggrOp.finishAccumulationFn()}
     */
    @Nonnull
    public static <K, A, R> DistributedSupplier<Processor> coAggregateByKeyP(
            @Nonnull List<DistributedFunction<?, ? extends K>> getKeyFs,
            @Nonnull AggregateOperation<A, R> aggrOp
    ) {
        return () -> new CoGroupP<>(getKeyFs, aggrOp);
    }

    /**
     * Returns a supplier of processors for the first-stage vertex in a
     * two-stage group-and-aggregate setup. The vertex groups items by the
     * grouping key and applies the {@link AggregateOperation#accumulateFn(
     * com.hazelcast.jet.datamodel.Tag) accumulate} primitive to each group.
     * After exhausting all its input it emits one {@code Map.Entry<K, A>} per
     * distinct key.
     * <p>
     * The vertex accepts input from one or more inbound edges. The type of
     * items may be different on each edge. For each edge a separate key
     * extracting function must be supplied and the aggregate operation must
     * contain a separate accumulation function for each edge.
     * <p>
     * This processor has state, but does not save it to snapshot. On job
     * restart, the state will be lost.
     *
     * @param getKeyFs functions that compute the grouping key
     * @param aggrOp the aggregate operation to perform
     * @param <K> type of key
     * @param <A> type of accumulator returned from {@code aggrOp.createAccumulatorFn()}
     */
    @Nonnull
    public static <K, A> DistributedSupplier<Processor> coAccumulateByKeyP(
            @Nonnull List<DistributedFunction<?, ? extends K>> getKeyFs,
            @Nonnull AggregateOperation<A, ?> aggrOp
    ) {
        return () -> new CoGroupP<>(getKeyFs, aggrOp.withFinishFn(identity()));
    }

    /**
     * Returns a supplier of processors for the second-stage vertex in a
     * two-stage group-and-aggregate setup. Each processor applies the {@link
     * AggregateOperation1#combineFn() combine} aggregation primitive to the
     * entries received from several upstream instances of {@link
     * #accumulateByKeyP(DistributedFunction, AggregateOperation1)
     * accumulateByKey()}. After exhausting all its input it emits one
     * {@code Map.Entry<K, R>} per distinct key.
     * <p>
     * Since the input to this vertex must be bounded, its primary use case
     * are batch jobs.
     * <p>
     * This processor has state, but does not save it to snapshot. On job
     * restart, the state will be lost.
     *
     * @param aggrOp the aggregate operation to perform
     * @param <A> type of accumulator returned from {@code aggrOp.createAccumulatorFn()}
     * @param <R> type of the finished result returned from
     *            {@code aggrOp.finishAccumulationFn()}
     */
    @Nonnull
    public static <A, R> DistributedSupplier<Processor> combineByKeyP(
            @Nonnull AggregateOperation<A, R> aggrOp
    ) {
        return () -> new CoGroupP<>(Entry::getKey, aggrOp.withCombiningAccumulateFn(Entry<Object, A>::getValue));
    }

    /**
     * Returns a supplier of processors for a vertex that performs the provided
     * aggregate operation on all the items it receives. After exhausting all
     * its input it emits a single item of type {@code R} &mdash;the result of
     * the aggregate operation.
     * <p>
     * Since the input to this vertex must be bounded, its primary use case are
     * batch jobs.
     * <p>
     * This processor has state, but does not save it to snapshot. On job
     * restart, the state will be lost.
     *
     * @param aggrOp the aggregate operation to perform
     * @param <T> type of received item
     * @param <A> type of accumulator returned from {@code
     *            aggrOp.createAccumulatorFn()}
     * @param <R> type of the finished result returned from {@code aggrOp.
     *            finishAccumulationFn()}
     */
    @Nonnull
    public static <T, A, R> DistributedSupplier<Processor> aggregateP(
            @Nonnull AggregateOperation1<T, A, R> aggrOp
    ) {
        return () -> new AggregateP<>(aggrOp);
    }

    /**
     * Returns a supplier of processors for a vertex that performs the provided
     * aggregate operation on all the items it receives. After exhausting all
     * its input it emits a single item of type {@code R} &mdash;the result of
     * the aggregate operation.
     * <p>
     * Since the input to this vertex must be bounded, its primary use case are
     * batch jobs.
     * <p>
     * This processor has state, but does not save it to snapshot. On job
     * restart, the state will be lost.
     *
     * @param aggrOp the aggregate operation to perform
     * @param <T> type of received item
     * @param <A> type of accumulator returned from {@code
     *            aggrOp.createAccumulatorFn()}
     * @param <R> type of the finished result returned from {@code aggrOp.
     *            finishAccumulationFn()}
     */
    @Nonnull
    public static <T, A, R> DistributedSupplier<Processor> accumulateP(
            @Nonnull AggregateOperation1<T, A, R> aggrOp
    ) {
        return () -> new AggregateP<>(aggrOp.withFinishFn(identity()));
    }

    /**
     * Returns a supplier of processors for a vertex that performs the provided
     * aggregate operation on all the items it receives. After exhausting all
     * its input it emits a single item of type {@code R} &mdash;the result of
     * the aggregate operation.
     * <p>
     * Since the input to this vertex must be bounded, its primary use case are
     * batch jobs.
     * <p>
     * This processor has state, but does not save it to snapshot. On job
     * restart, the state will be lost.
     *
     * @param aggrOp the aggregate operation to perform
     * @param <T> type of received item
     * @param <A> type of accumulator returned from {@code
     *            aggrOp.createAccumulatorFn()}
     * @param <R> type of the finished result returned from {@code aggrOp.
     *            finishAccumulationFn()}
     */
    @Nonnull
    public static <T, A, R> DistributedSupplier<Processor> combineP(
            @Nonnull AggregateOperation1<T, A, R> aggrOp
    ) {
        return () -> new AggregateP<>(aggrOp.withCombiningAccumulateFn(identity()));
    }

    /**
     * Returns a supplier of processors for a vertex that aggregates events
     * into a sliding window in a single stage (see the {@link Processors
     * class Javadoc} for an explanation of aggregation stages). The vertex
     * groups items by the grouping key (as obtained from the given
     * key-extracting function) and by <em>frame</em>, which is a range of
     * timestamps equal to the sliding step. It emits sliding window results
     * labeled with the timestamp denoting the window's end time (the exclusive
     * upper bound of the timestamps belonging to the window).
     * <p>
     * When the vertex receives a watermark with a given {@code wmVal}, it
     * emits the result of aggregation for all the positions of the sliding
     * window with {@code windowTimestamp <= wmVal}. It computes the window
     * result by combining the partial results of the frames belonging to it
     * and finally applying the {@code finish} aggregation primitive. After this
     * it deletes from storage all the frames that trail behind the emitted
     * windows. The type of emitted items is {@link TimestampedEntry
     * TimestampedEntry&lt;K, A>} so there is one item per key per window position.
     * <p>
     * <i>Behavior on job restart</i><br>
     * This processor saves its state to snapshot. After restart, it can
     * continue accumulating where it left off.
     * <p>
     * After a restart in at-least-once mode, watermarks are allowed to go back
     * in time. If such a watermark is received, some windows that were emitted
     * in previous execution will be re-emitted. These windows might miss
     * events as some of them had already been evicted before the snapshot was
     * done in previous execution.
     */
    @Nonnull
    public static <T, K, A, R> DistributedSupplier<Processor> aggregateToSlidingWindowP(
            @Nonnull DistributedFunction<? super T, K> getKeyFn,
            @Nonnull DistributedToLongFunction<? super T> getTimestampFn,
            @Nonnull TimestampKind timestampKind,
            @Nonnull WindowDefinition windowDef,
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp
    ) {
        return Processors.<T, K, A, R>aggregateByKeyAndWindowP(getKeyFn, getTimestampFn, timestampKind,
                windowDef, aggrOp, true);
    }

    /**
     * Returns a supplier of processors for the first-stage vertex in a
     * two-stage sliding window aggregation setup (see the {@link Processors
     * class Javadoc} for an explanation of aggregation stages). The vertex
     * groups items by the grouping key (as obtained from the given
     * key-extracting function) and by <em>frame</em>, which is a range of
     * timestamps equal to the sliding step. It applies the {@link
     * AggregateOperation1#accumulateFn() accumulate} aggregation primitive to
     * each key-frame group.
     * <p>
     * The frame is identified by the timestamp denoting its end time (equal to
     * the exclusive upper bound of its timestamp range). {@link
     * WindowDefinition#higherFrameTs(long)} maps the event timestamp to the
     * timestamp of the frame it belongs to.
     * <p>
     * When the processor receives a watermark with a given {@code wmVal}, it
     * emits the current accumulated state of all frames with {@code
     * timestamp <= wmVal} and deletes these frames from its storage.
     * The type of emitted items is {@link TimestampedEntry
     * TimestampedEntry&lt;K, A>} so there is one item per key per frame.
     * <p>
     * When a state snapshot is requested, the state is flushed to second-stage
     * processor and nothing is saved to snapshot.
     *
     * @param <T> input item type
     * @param <K> type of key returned from {@code getKeyFn}
     * @param <A> type of accumulator returned from {@code aggrOp.
     *            createAccumulatorFn()}
     */
    @Nonnull
    public static <T, K, A> DistributedSupplier<Processor> accumulateByFrameP(
            @Nonnull DistributedFunction<? super T, K> getKeyFn,
            @Nonnull DistributedToLongFunction<? super T> getTimestampFn,
            @Nonnull TimestampKind timestampKind,
            @Nonnull WindowDefinition windowDef,
            @Nonnull AggregateOperation1<? super T, A, ?> aggrOp
    ) {
        WindowDefinition tumblingByFrame = windowDef.toTumblingByFrame();
        return Processors.<T, K, A, A>aggregateByKeyAndWindowP(getKeyFn, getTimestampFn, timestampKind, tumblingByFrame,
                aggrOp.withFinishFn(identity()), false
        );
    }

    /**
     * Returns a supplier of processors for the second-stage vertex in a
     * two-stage sliding window aggregation setup (see the {@link Processors
     * class Javadoc} for an explanation of aggregation stages). Each
     * processor applies the {@link AggregateOperation1#combineFn() combine}
     * aggregation primitive to frames received from several upstream instances
     * of {@link #accumulateByFrameP(
     *      DistributedFunction, DistributedToLongFunction,
     *      TimestampKind, WindowDefinition, AggregateOperation1)
     * accumulateByFrame()}. It emits sliding window results labeled with
     * the timestamp denoting the window's end time. This timestamp is equal to
     * the exclusive upper bound of timestamps belonging to the window.
     * <p>
     * When the processor receives a watermark with a given {@code wmVal},
     * it emits the result of aggregation for all positions of the sliding
     * window with {@code windowTimestamp <= wmVal}. It computes the window
     * result by combining the partial results of the frames belonging to it
     * and finally applying the {@code finish} aggregation primitive. After this
     * it deletes from storage all the frames that trail behind the emitted
     * windows. The type of emitted items is {@link TimestampedEntry
     * TimestampedEntry&lt;K, A>} so there is one item per key per window position.
     * <p>
     * <i>Behavior on job restart</i><br>
     * This processor saves its state to snapshot. After restart, it can
     * continue accumulating where it left off.
     * <p>
     * After a restart in at-least-once mode, watermarks are allowed to go back
     * in time. If such a watermark is received, some windows that were emitted
     * in previous execution will be re-emitted. These windows might miss
     * events as some of them had already been evicted before the snapshot was
     * done in previous execution.
     *
     * @param <A> type of the accumulator
     * @param <R> type of the finished result returned from {@code aggrOp.
     *            finishAccumulationFn()}
     */
    @Nonnull
    public static <K, A, R> DistributedSupplier<Processor> combineToSlidingWindowP(
            @Nonnull WindowDefinition windowDef,
            @Nonnull AggregateOperation1<?, A, R> aggrOp
    ) {
        return aggregateByKeyAndWindowP(
                TimestampedEntry::getKey, TimestampedEntry::getTimestamp, TimestampKind.FRAME,
                windowDef, aggrOp.withCombiningAccumulateFn(TimestampedEntry<K, A>::getValue), true
        );
    }

    /**
     * Returns a supplier of processors for a vertex that performs a general
     * group-by-key-and-window operation and applies the provided aggregate
     * operation on groups.
     *
     * @param getKeyFn function that extracts the grouping key from the input item
     * @param getTimestampFn function that extracts the timestamp from the input item
     * @param timestampKind the kind of timestamp extracted by {@code getTimestampFn}: either the
     *                      event timestamp or the frame timestamp
     * @param windowDef definition of the window to compute
     * @param aggrOp aggregate operation to perform on each group in a window
     * @param isLastStage if this is the last stage of multi-stage setup
     *
     * @param <T> type of stream item
     * @param <K> type of grouping key
     * @param <A> type of the aggregate operation's accumulator
     * @param <R> type of the aggregated result
     */
    @Nonnull
    private static <T, K, A, R> DistributedSupplier<Processor> aggregateByKeyAndWindowP(
            @Nonnull DistributedFunction<? super T, K> getKeyFn,
            @Nonnull DistributedToLongFunction<? super T> getTimestampFn,
            @Nonnull TimestampKind timestampKind,
            @Nonnull WindowDefinition windowDef,
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp,
            boolean isLastStage
    ) {
        return () -> new SlidingWindowP<T, A, R>(
                getKeyFn,
                timestampKind == EVENT
                        ? item -> windowDef.higherFrameTs(getTimestampFn.applyAsLong(item))
                        : getTimestampFn,
                windowDef,
                aggrOp,
                isLastStage);
    }

    /**
     * Returns a supplier of processors for a vertex that aggregates events into
     * session windows. Events and windows under different grouping keys are
     * treated independently. Outputs objects of type {@link
     * com.hazelcast.jet.datamodel.Session}.
     * <p>
     * The functioning of this vertex is easiest to explain in terms of the
     * <em>event interval</em>: the range {@code [timestamp, timestamp +
     * sessionTimeout]}. Initially an event causes a new session window to be
     * created, covering exactly the event interval. A following event under
     * the same key belongs to this window iff its interval overlaps it. The
     * window is extended to cover the entire interval of the new event. The
     * event may happen to belong to two existing windows if its interval
     * bridges the gap between them; in that case they are combined into one.
     * <p>
     * <i>Behavior on job restart</i><br>
     * This processor saves its state to snapshot. After restart, it can
     * continue accumulating where it left off.
     * <p>
     * After a restart in at-least-once mode, watermarks are allowed to go back
     * in time. The processor evicts state based on watermarks it received. If
     * it receives duplicate watermark, it might emit sessions with missing
     * events, because they were already evicted. The sessions before and after
     * snapshot might overlap, which they normally don't.
     *
     * @param sessionTimeout maximum gap between consecutive events in the same session window
     * @param getTimestampFn function to extract the timestamp from the item
     * @param getKeyFn       function to extract the grouping key from the item
     * @param aggrOp         the aggregate operation
     *
     * @param <T> type of the stream event
     * @param <K> type of the item's grouping key
     * @param <A> type of the container of the accumulated value
     * @param <R> type of the session window's result value
     */
    @Nonnull
    public static <T, K, A, R> DistributedSupplier<Processor> aggregateToSessionWindowP(
            long sessionTimeout,
            @Nonnull DistributedToLongFunction<? super T> getTimestampFn,
            @Nonnull DistributedFunction<? super T, K> getKeyFn,
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp
    ) {
        return () -> new SessionWindowP<>(sessionTimeout, getTimestampFn, getKeyFn, aggrOp);
    }

    /**
     * Returns a supplier of processors for a vertex that inserts {@link
     * com.hazelcast.jet.core.Watermark watermark items} into the stream. The
     * value of the watermark is determined by the supplied {@link
     * com.hazelcast.jet.core.WatermarkPolicy} instance.
     * <p>
     * This processor also drops late items. It never allows an event which is
     * late with regard to already emitted watermark to pass.
     * <p>
     * The processor saves value of the last emitted watermark to snapshot.
     * Different instances of this processor can be at different watermark at
     * snapshot time. After restart all instances will start at watermark of
     * the most-behind instance before the restart.
     * <p>
     * This might sound as it could break the monotonicity requirement, but
     * thanks to watermark coalescing, watermarks are only delivered for
     * downstream processing after they have been received from <i>all</i>
     * upstream processors. Another side effect of this is, that a late event,
     * which was dropped before restart, is not considered late after restart.
     *
     * @param <T> the type of the stream item
     */
    @Nonnull
    public static <T> DistributedSupplier<Processor> insertWatermarksP(@Nonnull WatermarkGenerationParams<T> wmGenParams) {
        return () -> new InsertWatermarksP<>(wmGenParams);
    }

    /**
     * Returns a supplier of processors for a vertex which, for each received
     * item, emits the result of applying the given mapping function to it. If
     * the result is {@code null}, it emits nothing. Therefore this vertex can
     * be used to implement filtering semantics as well.
     * <p>
     * This processor is stateless.
     *
     * @param mapFn a stateless mapping function
     * @param <T> type of received item
     * @param <R> type of emitted item
     */
    @Nonnull
    public static <T, R> DistributedSupplier<Processor> mapP(
            @Nonnull DistributedFunction<T, R> mapFn
    ) {
        return () -> {
            final ResettableSingletonTraverser<R> trav = new ResettableSingletonTraverser<>();
            return new TransformP<T, R>(item -> {
                trav.accept(mapFn.apply(item));
                return trav;
            });
        };
    }

    /**
     * Returns a supplier of processors for a vertex that emits the same items
     * it receives, but only those that pass the given predicate.
     * <p>
     * This processor is stateless.
     *
     * @param predicate a stateless predicate to test each received item against
     * @param <T> type of received item
     */
    @Nonnull
    public static <T> DistributedSupplier<Processor> filterP(@Nonnull DistributedPredicate<T> predicate) {
        return () -> {
            final ResettableSingletonTraverser<T> trav = new ResettableSingletonTraverser<>();
            return new TransformP<T, T>(item -> {
                trav.accept(predicate.test(item) ? item : null);
                return trav;
            });
        };
    }

    /**
     * Returns a supplier of processors for a vertex that applies the provided
     * item-to-traverser mapping function to each received item and emits all
     * the items from the resulting traverser.
     * <p>
     * The traverser returned from the {@code flatMapFn} must be finite. That
     * is, this operation will not attempt to emit any items after the first
     * {@code null} item.
     * <p>
     * This processor is stateless.
     *
     * @param flatMapFn a stateless function that maps the received item to a traverser over output items
     * @param <T> received item type
     * @param <R> emitted item type
     */
    @Nonnull
    public static <T, R> DistributedSupplier<Processor> flatMapP(
            @Nonnull DistributedFunction<T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return () -> new TransformP<>(flatMapFn);
    }

    /**
     * Returns a supplier of processor that consumes all its input (if any) and
     * does nothing with it.
     */
    @Nonnull
    public static DistributedSupplier<Processor> noopP() {
        return NoopP::new;
    }

    /**
     * Decorates a processor meta-supplier with one that will declare all its
     * processors non-cooperative. The wrapped meta-supplier must return processors
     * that are {@code instanceof} {@link AbstractProcessor}.
     */
    @Nonnull
    public static ProcessorMetaSupplier nonCooperativeP(@Nonnull ProcessorMetaSupplier wrapped) {
        return new WrappingProcessorMetaSupplier(wrapped, p -> {
            ((AbstractProcessor) p).setCooperative(false);
            return p;
        });
    }

    /**
     * Decorates a {@code ProcessorSupplier} with one that will declare all its
     * processors non-cooperative. The wrapped supplier must return processors
     * that are {@code instanceof} {@link AbstractProcessor}.
     */
    @Nonnull
    public static ProcessorSupplier nonCooperativeP(@Nonnull ProcessorSupplier wrapped) {
        return new WrappingProcessorSupplier(wrapped, p -> {
            ((AbstractProcessor) p).setCooperative(false);
            return p;
        });
    }

    /**
     * Decorates a {@code Supplier<Processor>} into one that will declare
     * its processors non-cooperative. The wrapped supplier must return
     * processors that are {@code instanceof} {@link AbstractProcessor}.
     */
    @Nonnull
    public static DistributedSupplier<Processor> nonCooperativeP(@Nonnull DistributedSupplier<Processor> wrapped) {
        return () -> {
            final Processor p = wrapped.get();
            ((AbstractProcessor) p).setCooperative(false);
            return p;
        };
    }

    /** A no-operation processor. See {@link #noopP()} */
    private static class NoopP implements Processor {
        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            inbox.drain(noopConsumer());
        }
    }
}
