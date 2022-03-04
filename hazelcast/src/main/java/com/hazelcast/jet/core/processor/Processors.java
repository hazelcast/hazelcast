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

package com.hazelcast.jet.core.processor;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BiPredicateEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.function.KeyedWindowResultFunction;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.processor.AggregateP;
import com.hazelcast.jet.impl.processor.AsyncTransformUsingServiceOrderedP;
import com.hazelcast.jet.impl.processor.AsyncTransformUsingServiceUnorderedP;
import com.hazelcast.jet.impl.processor.GroupP;
import com.hazelcast.jet.impl.processor.InsertWatermarksP;
import com.hazelcast.jet.impl.processor.NoopP;
import com.hazelcast.jet.impl.processor.ProcessorSuppliers;
import com.hazelcast.jet.impl.processor.SessionWindowP;
import com.hazelcast.jet.impl.processor.SlidingWindowP;
import com.hazelcast.jet.impl.processor.SortP;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.jet.impl.processor.TransformStatefulP;
import com.hazelcast.jet.impl.processor.TransformUsingServiceP;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.jet.core.TimestampKind.EVENT;
import static com.hazelcast.jet.impl.util.Util.toList;
import static java.util.Collections.singletonList;

/**
 * Static utility class with factory methods for Jet processors. These are
 * meant to implement the internal vertices of the DAG; for other kinds of
 * processors refer to the {@linkplain com.hazelcast.jet.core.processor
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
 * <table border="1" summary="Overview of factory methods for aggregate operations">
 * <tr>
 *     <th></th>
 *     <th>single-stage</th>
 *     <th>stage 1/2</th>
 *     <th>stage 2/2</th>
 * </tr><tr>
 *     <th>batch,<br>no grouping</th>
 *
 *     <td>{@link #aggregateP}</td>
 *     <td>{@link #accumulateP}</td>
 *     <td>{@link #combineP}</td>
 * </tr><tr>
 *     <th>batch, group by key</th>
 *
 *     <td>{@link #aggregateByKeyP}</td>
 *     <td>{@link #accumulateByKeyP}</td>
 *     <td rowspan='2'>{@link #combineByKeyP}</td>
 * </tr><tr>
 *     <th>batch, co-group by key</th>
 *
 *     <td>{@link #aggregateByKeyP}</td>
 *     <td>{@link #accumulateByKeyP}</td>
 * </tr><tr>
 *     <th>stream, group by key<br>and aligned window</th>
 *
 *     <td>{@link #aggregateToSlidingWindowP}</td>
 *     <td>{@link #accumulateByFrameP}</td>
 *     <td rowspan='2'>{@link #combineToSlidingWindowP}</td>
 * </tr><tr>
 *     <th>stream, co-group by key<br>and aligned window</th>
 *
 *     <td>{@link #aggregateToSlidingWindowP}</td>
 *     <td>{@link #accumulateByFrameP}</td>
 * </tr><tr>
 *     <th>stream, group by key<br>and session window</th>
 *     <td>{@link #aggregateToSessionWindowP}</td>
 *     <td>N/A</td>
 *     <td>N/A</td>
 * </tr></table>
 * <p>
 * Tumbling window is a special case of sliding window with sliding step =
 * window size.
 *
 * @since Jet 3.0
 */
public final class Processors {

    private Processors() {
    }

    /**
     * Returns a supplier of processors for a vertex that performs the provided
     * aggregate operation on all the items it receives. After exhausting all
     * its input, it emits a single item of type {@code R} &mdash; the result of
     * the aggregate operation's {@link AggregateOperation#finishFn() finish}
     * primitive. The primitive may return {@code null}, in that case the vertex
     * will not produce any output.
     * <p>
     * Since the input to this vertex must be bounded, its primary use case are
     * batch jobs.
     * <p>
     * This processor has state, but does not save it to the snapshot. On job
     * restart, the state will be lost.
     * @param <A> type of accumulator returned from {@code
     *            aggrOp.createAccumulatorFn()}
     * @param <R> type of the finished result returned from {@code
     *            aggrOp.finishAccumulationFn()}
     * @param aggrOp the aggregate operation to perform
     */
    @Nonnull
    public static <A, R> SupplierEx<Processor> aggregateP(
            @Nonnull AggregateOperation<A, R> aggrOp
    ) {
        // We should use the same constant key as the input edges do, but since
        // the processor doesn't save the state, there's no need to.
        return () -> new AggregateP<>(aggrOp);
    }

    /**
     * Returns a supplier of processors for a vertex that performs the
     * accumulation step of the provided aggregate operation on all the items
     * it receives. After exhausting all its input, it emits a single item of
     * type {@code A} &mdash; the accumulator object.
     * <p>
     * Since the input to this vertex must be bounded, its primary use case are
     * batch jobs.
     * <p>
     * This processor has state, but does not save it to the snapshot. On job
     * restart, the state will be lost.
     * @param <A> type of accumulator returned from {@code
     *            aggrOp.createAccumulatorFn()}
     * @param <R> type of the finished result returned from {@code aggrOp.
     *            finishAccumulationFn()}
     * @param aggrOp the aggregate operation to perform
     */
    @Nonnull
    public static <A, R> SupplierEx<Processor> accumulateP(@Nonnull AggregateOperation<A, R> aggrOp) {
        return new ProcessorSuppliers.AggregatePSupplier<>(aggrOp.withIdentityFinish());
    }

    /**
     * Returns a supplier of processors for a vertex that performs the
     * combining and finishing steps of the provided aggregate operation. It
     * expects to receive the accumulator objects from the upstream {@link
     * #accumulateP} vertex and combines their state into a single
     * accumulator. After exhausting all its input, it emits a single result
     * of type {@code R} &mdash; the result of applying the {@code finish}
     * primitive to the combined accumulator. The primitive may return {@code
     * null}, in that case the vertex will not produce any output.
     * <p>
     * Since the input to this vertex must be bounded, its primary use case is
     * batch jobs.
     * <p>
     * This processor has state, but does not save it to the snapshot. On job
     * restart, the state will be lost.
     * @param <A> type of accumulator returned from {@code
     *            aggrOp.createAccumulatorFn()}
     * @param <R> type of the finished result returned from {@code aggrOp.
     *            finishAccumulationFn()}
     * @param aggrOp the aggregate operation to perform
     */
    @Nonnull
    public static <A, R> SupplierEx<Processor> combineP(
            @Nonnull AggregateOperation<A, R> aggrOp
    ) {
        return new ProcessorSuppliers.AggregatePSupplier<>(aggrOp.withCombiningAccumulateFn(identity()));
    }

    /**
     * Returns a supplier of processors for a vertex that groups items by key
     * and performs the provided aggregate operation on each group. After
     * exhausting all its input it emits one item per distinct key. It computes
     * the item to emit by passing each (key, result) pair to {@code
     * mapToOutputFn}.
     * <p>
     * The vertex accepts input from one or more inbound edges. The type of
     * items may be different on each edge. For each edge a separate key
     * extracting function must be supplied and the aggregate operation must
     * contain a separate accumulation function for each edge.
     * <p>
     * This processor has state, but does not save it to snapshot. On job
     * restart, the state will be lost.
     *
     * @param keyFns functions that compute the grouping key
     * @param aggrOp the aggregate operation
     * @param mapToOutputFn function that takes the key and the aggregation result and returns
     *                      the output item
     * @param <K> type of key
     * @param <A> type of accumulator returned from {@code aggrOp.createAccumulatorFn()}
     * @param <R> type of the result returned from {@code aggrOp.finishAccumulationFn()}
     * @param <OUT> type of the item to emit
     */
    @Nonnull
    public static <K, A, R, OUT> SupplierEx<Processor> aggregateByKeyP(
            @Nonnull List<FunctionEx<?, ? extends K>> keyFns,
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull BiFunctionEx<? super K, ? super R, OUT> mapToOutputFn
    ) {
        return () -> new GroupP<>(keyFns, aggrOp, mapToOutputFn);
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
     * @param getKeyFns functions that compute the grouping key
     * @param aggrOp the aggregate operation to perform
     * @param <K> type of key
     * @param <A> type of accumulator returned from {@code aggrOp.createAccumulatorFn()}
     */
    @Nonnull
    public static <K, A> SupplierEx<Processor> accumulateByKeyP(
            @Nonnull List<FunctionEx<?, ? extends K>> getKeyFns,
            @Nonnull AggregateOperation<A, ?> aggrOp
    ) {
        return () -> new GroupP<>(getKeyFns, aggrOp.withIdentityFinish(), Util::entry);
    }

    /**
     * Returns a supplier of processors for the second-stage vertex in a
     * two-stage group-and-aggregate setup. Each processor applies the {@link
     * AggregateOperation1#combineFn() combine} aggregation primitive to the
     * entries received from several upstream instances of {@link
     * #accumulateByKeyP}. After exhausting all its input it emits one item per
     * distinct key. It computes the item to emit by passing each (key, result)
     * pair to {@code mapToOutputFn}.
     * <p>
     * Since the input to this vertex must be bounded, its primary use case
     * are batch jobs.
     * <p>
     * This processor has state, but does not save it to snapshot. On job
     * restart, the state will be lost.
     *
     * @param aggrOp the aggregate operation to perform
     * @param mapToOutputFn function that takes the key and the aggregation result and returns
     *                      the output item
     * @param <A> type of accumulator returned from {@code aggrOp.createAccumulatorFn()}
     * @param <R> type of the finished result returned from
     *            {@code aggrOp.finishAccumulationFn()}
     * @param <OUT> type of the item to emit
     */
    @Nonnull
    public static <K, A, R, OUT> SupplierEx<Processor> combineByKeyP(
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull BiFunctionEx<? super K, ? super R, OUT> mapToOutputFn
    ) {
        return () -> new GroupP<>(
                Entry::getKey,
                aggrOp.withCombiningAccumulateFn(Entry<K, A>::getValue),
                mapToOutputFn);
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
     * The vertex accepts input from one or more inbound edges. The type of
     * items may be different on each edge. For each edge a separate key
     * extracting function must be supplied and the aggregate operation must
     * contain a separate accumulation function for each edge.
     * <p>
     * When the vertex receives a watermark with a given {@code wmVal}, it
     * emits the result of aggregation for all the positions of the sliding
     * window with {@code windowTimestamp <= wmVal}. It computes the window
     * result by combining the partial results of the frames belonging to it
     * and finally applying the {@code finish} aggregation primitive. After this
     * it deletes from storage all the frames that trail behind the emitted
     * windows. In the output there is one item per key per window position.
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
    public static <K, A, R, OUT> SupplierEx<Processor> aggregateToSlidingWindowP(
            @Nonnull List<FunctionEx<?, ? extends K>> keyFns,
            @Nonnull List<ToLongFunctionEx<?>> timestampFns,
            @Nonnull TimestampKind timestampKind,
            @Nonnull SlidingWindowPolicy winPolicy,
            long earlyResultsPeriod,
            @Nonnull AggregateOperation<A, ? extends R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> mapToOutputFn
    ) {
        return aggregateByKeyAndWindowP(
                keyFns, timestampFns, timestampKind, winPolicy, earlyResultsPeriod, aggrOp, mapToOutputFn, true);
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
     * SlidingWindowPolicy#higherFrameTs(long)} maps the event timestamp to the
     * timestamp of the frame it belongs to.
     * <p>
     * The vertex accepts input from one or more inbound edges. The type of
     * items may be different on each edge. For each edge a separate key
     * extracting function must be supplied and the aggregate operation must
     * contain a separate accumulation function for each edge.
     * <p>
     * When the processor receives a watermark with a given {@code wmVal}, it
     * emits the current accumulated state of all frames with {@code
     * timestamp <= wmVal} and deletes these frames from its storage. In the
     * output there is one item per key per frame.
     * <p>
     * When a state snapshot is requested, the state is flushed to second-stage
     * processor and nothing is saved to snapshot.
     *
     * @param <K> type of the grouping key
     * @param <A> type of accumulator returned from {@code aggrOp.
     *            createAccumulatorFn()}
     */
    @Nonnull
    public static <K, A> SupplierEx<Processor> accumulateByFrameP(
            @Nonnull List<FunctionEx<?, ? extends K>> keyFns,
            @Nonnull List<ToLongFunctionEx<?>> timestampFns,
            @Nonnull TimestampKind timestampKind,
            @Nonnull SlidingWindowPolicy winPolicy,
            @Nonnull AggregateOperation<A, ?> aggrOp
    ) {
        return aggregateByKeyAndWindowP(
                keyFns,
                timestampFns,
                timestampKind,
                winPolicy.toTumblingByFrame(),
                0L,
                aggrOp.withIdentityFinish(),
                KeyedWindowResult::new,
                false
        );
    }

    /**
     * Returns a supplier of processors for the second-stage vertex in a
     * two-stage sliding window aggregation setup (see the {@link Processors
     * class Javadoc} for an explanation of aggregation stages). Each
     * processor applies the {@link AggregateOperation1#combineFn() combine}
     * aggregation primitive to the frames received from several upstream
     * instances of {@link #accumulateByFrameP accumulateByFrame()}.
     * <p>
     * When the processor receives a watermark with a given {@code wmVal},
     * it emits the result of aggregation for all positions of the sliding
     * window with {@code windowTimestamp <= wmVal}. It computes the window
     * result by combining the partial results of the frames belonging to it
     * and finally applying the {@code finish} aggregation primitive. After
     * this it deletes from storage all the frames that trail behind the
     * emitted windows. To compute the item to emit, it calls {@code
     * mapToOutputFn} with the window's start and end timestamps, the key and
     * the aggregation result. The window end time is the exclusive upper bound
     * of the timestamps belonging to the window.
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
     * @param <OUT> type of the item to emit
     */
    @Nonnull
    public static <K, A, R, OUT> SupplierEx<Processor> combineToSlidingWindowP(
            @Nonnull SlidingWindowPolicy winPolicy,
            @Nonnull AggregateOperation<A, ? extends R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> mapToOutputFn
    ) {
        FunctionEx<KeyedWindowResult<K, A>, K> keyFn = KeyedWindowResult::key;
        ToLongFunctionEx<KeyedWindowResult<K, A>> timestampFn = KeyedWindowResult::end;
        return aggregateByKeyAndWindowP(
                singletonList(keyFn),
                singletonList(timestampFn),
                TimestampKind.FRAME,
                winPolicy,
                0L,
                aggrOp.withCombiningAccumulateFn(KeyedWindowResult<Object, A>::result),
                mapToOutputFn,
                true
        );
    }

    /**
     * Returns a supplier of processors for a vertex that performs a general
     * group-by-key-and-window operation and applies the provided aggregate
     * operation on groups.
     *
     * @param keyFns functions that extract the grouping key from the input item
     * @param timestampFns function that extracts the timestamp from the input item
     * @param timestampKind the kind of timestamp extracted by {@code timestampFns}: either the
     *                      event timestamp or the frame timestamp
     * @param winPolicy definition of the window to compute
     * @param earlyResultsPeriod the period (milliseconds) at which to emit the preliminary results of
     *                           windows before the watermark has advanced past them. Zero means "don't
     *                           emit early results"
     * @param aggrOp aggregate operation to perform on each group in a window
     * @param isLastStage if this is the last stage of multi-stage setup
     *
     * @param <K> type of the grouping key
     * @param <A> type of the aggregate operation's accumulator
     * @param <R> type of the aggregated result
     */
    @Nonnull
    private static <K, A, R, OUT> SupplierEx<Processor> aggregateByKeyAndWindowP(
            @Nonnull List<FunctionEx<?, ? extends K>> keyFns,
            @Nonnull List<ToLongFunctionEx<?>> timestampFns,
            @Nonnull TimestampKind timestampKind,
            @Nonnull SlidingWindowPolicy winPolicy,
            long earlyResultsPeriod,
            @Nonnull AggregateOperation<A, ? extends R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> mapToOutputFn,
            boolean isLastStage
    ) {
        return () -> new SlidingWindowP<>(
                keyFns,
                toList(timestampFns, f -> toFrameTimestampFn(f, timestampKind, winPolicy)),
                winPolicy,
                earlyResultsPeriod,
                aggrOp,
                mapToOutputFn,
                isLastStage);
    }

    private static ToLongFunctionEx<Object> toFrameTimestampFn(
            @Nonnull ToLongFunctionEx<?> timestampFnX,
            @Nonnull TimestampKind timestampKind,
            @Nonnull SlidingWindowPolicy winPolicy
    ) {
        @SuppressWarnings("unchecked")
        ToLongFunctionEx<Object> timestampFn = (ToLongFunctionEx<Object>) timestampFnX;
        return timestampKind == EVENT
                ? item -> winPolicy.higherFrameTs(timestampFn.applyAsLong(item))
                : item -> winPolicy.higherFrameTs(timestampFn.applyAsLong(item) - 1);
    }

    /**
     * Returns a supplier of processors for a vertex that aggregates events into
     * session windows. Events and windows under different grouping keys are
     * treated independently. Outputs objects of type {@link
     * com.hazelcast.jet.datamodel.WindowResult}.
     * <p>
     * The vertex accepts input from one or more inbound edges. The type of
     * items may be different on each edge. For each edge a separate key
     * extracting function must be supplied and the aggregate operation must
     * contain a separate accumulation function for each edge.
     * <p>
     * The functioning of this vertex is easiest to explain in terms of the
     * <em>event interval</em>: the range {@code [timestamp, timestamp +
     * sessionTimeout)}. Initially an event causes a new session window to be
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
     * @param timestampFns   functions to extract the timestamp from the item
     * @param keyFns         functions to extract the grouping key from the item
     * @param aggrOp         the aggregate operation
     *
     * @param <K> type of the item's grouping key
     * @param <A> type of the container of the accumulated value
     * @param <R> type of the session window's result value
     */
    @Nonnull
    public static <K, A, R, OUT> SupplierEx<Processor> aggregateToSessionWindowP(
            long sessionTimeout,
            long earlyResultsPeriod,
            @Nonnull List<ToLongFunctionEx<?>> timestampFns,
            @Nonnull List<FunctionEx<?, ? extends K>> keyFns,
            @Nonnull AggregateOperation<A, ? extends R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> mapToOutputFn
    ) {
        return () -> new SessionWindowP<>(
                sessionTimeout, earlyResultsPeriod, timestampFns, keyFns, aggrOp, mapToOutputFn);
    }

    /**
     * Returns a supplier of processors for a vertex that inserts {@link
     * com.hazelcast.jet.core.Watermark watermark items} into the stream. The
     * value of the watermark is determined by the supplied {@link
     * com.hazelcast.jet.core.EventTimePolicy} instance.
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
    public static <T> SupplierEx<Processor> insertWatermarksP(
            @Nonnull EventTimePolicy<? super T> eventTimePolicy
    ) {
        return () -> new InsertWatermarksP<>(eventTimePolicy);
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
    public static <T, R> SupplierEx<Processor> mapP(@Nonnull FunctionEx<? super T, ? extends R> mapFn) {
        return new ProcessorSuppliers.ProcessorMapPSupplier<>(mapFn);
    }

    /**
     * Returns a supplier of processors for a vertex that emits the same items
     * it receives, but only those that pass the given predicate.
     * <p>
     * This processor is stateless.
     *
     * @param filterFn a stateless predicate to test each received item against
     * @param <T> type of received item
     */
    @Nonnull
    public static <T> SupplierEx<Processor> filterP(@Nonnull PredicateEx<? super T> filterFn) {
        return mapP((T t) -> filterFn.test(t) ? t : null);
    }

    /**
     * Returns a supplier of processors for a vertex that applies the provided
     * item-to-traverser mapping function to each received item and emits all
     * the items from the resulting traverser. The traverser must be
     * <em>null-terminated</em>.
     * <p>
     * This processor is stateless.
     *
     * @param flatMapFn a stateless function that maps the received item
     *                 to a traverser over output items. It must not return
     *                 null traverser, but can return an {@linkplain
     *                 Traversers#empty() empty traverser}.
     * @param <T> received item type
     * @param <R> emitted item type
     */
    @Nonnull
    public static <T, R> SupplierEx<Processor> flatMapP(
            @Nonnull FunctionEx<? super T, ? extends Traverser<? extends R>> flatMapFn
    ) {
        return () -> new TransformP<>(flatMapFn);
    }

    /**
     * Returns a supplier of processors for a vertex that performs a stateful
     * mapping of its input. {@code createFn} returns the object that holds the
     * state. The processor passes this object along with each input item to
     * {@code mapFn}, which can update the object's state. For each grouping
     * key there's a separate state object. The state object will be included
     * in the state snapshot, so it survives job restarts. For this reason the
     * object must be serializable. If the mapping function maps an item to
     * {@code null}, it will have the effect of filtering out that item.
     * <p>
     * If the given {@code ttl} is greater than zero, the processor will
     * consider the state object stale if its time-to-live has expired. The
     * time-to-live refers to the event time as kept by the watermark: each
     * time it processes an event, the processor compares the state object's
     * timestamp with the current watermark. If it is less than {@code
     * wm - ttl}, it discards the state object. Otherwise it updates the
     * timestamp with the current watermark.
     *
     * @param ttl               state object's time to live
     * @param keyFn             function to extract the key from an input item
     * @param createFn          supplier of the state object
     * @param statefulMapFn the stateful mapping function
     * @param <T>               type of the input item
     * @param <K>               type of the key
     * @param <S>               type of the state object
     * @param <R>               type of the mapping function's result
     */
    @Nonnull
    public static <T, K, S, R> SupplierEx<Processor> mapStatefulP(
            long ttl,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn,
            @Nonnull ToLongFunctionEx<? super T> timestampFn,
            @Nonnull Supplier<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> statefulMapFn,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends R> onEvictFn
    ) {
        return () -> {
            final ResettableSingletonTraverser<R> mainTrav = new ResettableSingletonTraverser<>();
            final ResettableSingletonTraverser<R> evictTrav = new ResettableSingletonTraverser<>();
            @SuppressWarnings("UnnecessaryLocalVariable")
            TriFunction<? super S, ? super K, ? super Long, ? extends R> onEvictFnCopy = onEvictFn;
            return new TransformStatefulP<T, K, S, R>(
                    ttl,
                    keyFn,
                    timestampFn,
                    createFn,
                    (state, key, item) -> {
                        mainTrav.accept(statefulMapFn.apply(state, key, item));
                        return mainTrav;
                    },
                    onEvictFnCopy != null ? (s, k, wm) -> {
                        evictTrav.accept(onEvictFnCopy.apply(s, k, wm));
                        return evictTrav;
                    } : null
            );
        };
    }

    /**
     * Returns a supplier of processors for a vertex that performs a stateful
     * flat-mapping of its input. {@code createFn} returns the object that
     * holds the state. The processor passes this object along with each input
     * item to {@code mapFn}, which can update the object's state. For each
     * grouping key there's a separate state object. The state object will be
     * included in the state snapshot, so it survives job restarts. For this
     * reason the object must be serializable.
     * <p>
     * If the given {@code ttl} is greater than zero, the processor will
     * consider the state object stale if its time-to-live has expired. The
     * time-to-live refers to the event time as kept by the watermark: each
     * time it processes an event, the processor compares the state object's
     * timestamp with the current watermark. If it is less than {@code
     * wm - ttl}, it discards the state object. Otherwise it updates the
     * timestamp with the current watermark.
     *
     * @param ttl               state object's time to live
     * @param keyFn             function to extract the key from an input item
     * @param createFn          supplier of the state object
     * @param statefulFlatMapFn the stateful mapping function
     * @param <T>               type of the input item
     * @param <K>               type of the key
     * @param <S>               type of the state object
     * @param <R>               type of the mapping function's result
     */
    @Nonnull
    public static <T, K, S, R> SupplierEx<Processor> flatMapStatefulP(
            long ttl,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn,
            @Nonnull ToLongFunctionEx<? super T> timestampFn,
            @Nonnull Supplier<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> statefulFlatMapFn,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn
    ) {
        return () -> new TransformStatefulP<>(
                ttl,
                keyFn,
                timestampFn,
                createFn,
                statefulFlatMapFn,
                onEvictFn
        );
    }

    /**
     * Returns a supplier of processors for a vertex which, for each received
     * item, emits the result of applying the given mapping function to it. The
     * mapping function receives another parameter, the service object which
     * Jet will create using the supplied {@code serviceFactory}.
     * <p>
     * If the mapping result is {@code null}, the vertex emits nothing.
     * Therefore it can be used to implement filtering semantics as well.
     * <p>
     * Unlike {@link #mapStatefulP} (with the "{@code Keyed}" part),
     * this method creates one service object per processor.
     * <p>
     * While it's allowed to store some local state in the service object, it
     * won't be saved to the snapshot and will misbehave in a fault-tolerant
     * stream processing job.
     *
     * @param serviceFactory the service factory
     * @param mapFn a stateless mapping function
     * @param <C> type of context object
     * @param <S> type of service object
     * @param <T> type of received item
     * @param <R> type of emitted item
     */
    @Nonnull
    public static <C, S, T, R> ProcessorSupplier mapUsingServiceP(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        return TransformUsingServiceP.<C, S, T, R>supplier(serviceFactory, (singletonTraverser, context, item) -> {
            singletonTraverser.accept(mapFn.apply(context, item));
            return singletonTraverser;
        });
    }

    /**
     * Asynchronous version of {@link #mapUsingServiceP}: the {@code
     * mapAsyncFn} returns a {@code CompletableFuture<R>} instead of just
     * {@code R}.
     * <p>
     * The function can return a null future and the future can return a null
     * result: in both cases it will act just like a filter.
     * <p>
     * The {@code extractKeyFn} is used to extract keys under which to save
     * in-flight items to the snapshot. If the input to this processor is over
     * a partitioned edge, you should use the same key. If it's a round-robin
     * edge, you can use any key, for example {@code Object::hashCode}.
     *
     * @param serviceFactory the service factory
     * @param maxConcurrentOps maximum number of concurrent async operations per processor
     * @param preserveOrder whether the async responses are ordered or not
     * @param extractKeyFn a function to extract snapshot keys. Used only if preserveOrder==false
     * @param mapAsyncFn a stateless mapping function
     * @param <C> type of context object
     * @param <S> type of service object
     * @param <T> type of received item
     * @param <K> type of key
     * @param <R> type of result item
     */
    @Nonnull
    public static <C, S, T, K, R> ProcessorSupplier mapUsingServiceAsyncP(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            int maxConcurrentOps,
            boolean preserveOrder,
            @Nonnull FunctionEx<T, K> extractKeyFn,
            @Nonnull BiFunctionEx<? super S, ? super T, CompletableFuture<R>> mapAsyncFn
    ) {
        BiFunctionEx<S, T, CompletableFuture<Traverser<R>>> flatMapAsyncFn = (s, t) ->
                mapAsyncFn.apply(s, t).thenApply(Traversers::singleton);
        return preserveOrder
                ? AsyncTransformUsingServiceOrderedP.supplier(
                        serviceFactory, maxConcurrentOps, flatMapAsyncFn)
                : AsyncTransformUsingServiceUnorderedP.supplier(
                        serviceFactory, maxConcurrentOps, flatMapAsyncFn, extractKeyFn);
    }

    /**
     * Returns a supplier of processors for a vertex that emits the same items
     * it receives, but only those that pass the given predicate. The predicate
     * function receives another parameter, the service object which Jet will
     * create using the supplied {@code serviceFactory}.
     * <p>
     * While it's allowed to store some local state in the service object, it
     * won't be saved to the snapshot and will misbehave in a fault-tolerant
     * stream processing job.
     *
     * @param serviceFactory the service factory
     * @param filterFn a stateless predicate to test each received item against
     * @param <C> type of context object
     * @param <S> type of service object
     * @param <T> type of received item
     */
    @Nonnull
    public static <C, S, T> ProcessorSupplier filterUsingServiceP(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        return TransformUsingServiceP.<C, S, T, T>supplier(serviceFactory, (singletonTraverser, context, item) -> {
            singletonTraverser.accept(filterFn.test(context, item) ? item : null);
            return singletonTraverser;
        });
    }

    /**
     * Returns a supplier of processors for a vertex that applies the provided
     * item-to-traverser mapping function to each received item and emits all
     * the items from the resulting traverser. The traverser must be
     * <em>null-terminated</em>. The mapping function receives another parameter,
     * the service object which Jet will create using the supplied {@code
     * serviceFactory}.
     * <p>
     * While it's allowed to store some local state in the service object, it
     * won't be saved to the snapshot and will misbehave in a fault-tolerant
     * stream processing job.
     *
     * @param serviceFactory the service factory
     * @param flatMapFn a stateless function that maps the received item to a traverser over
     *                  the output items
     * @param <C> type of context object
     * @param <S> type of service object
     * @param <T> type of input item
     * @param <R> type of result item
     */
    @Nonnull
    public static <C, S, T, R> ProcessorSupplier flatMapUsingServiceP(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return TransformUsingServiceP.<C, S, T, R>supplier(serviceFactory,
                (singletonTraverser, service, item) -> flatMapFn.apply(service, item));
    }

    /**
     * Returns a supplier of processors for a vertex that sorts its input using
     * a {@link PriorityQueue} and emits it in the {@code complete} phase.
     * <p>
     * The output edge of this vertex should be {@link Edge#distributed
     * distributed} {@link Edge#ordered monotonicOrder} {@link
     * Edge#allToOne allToOne} so it preserves the ordering when merging
     * the data from all upstream processors.
     *
     * @since Jet 4.3
     */
    @Nonnull
    public static <T> SupplierEx<Processor> sortP(Comparator<T> comparator) {
        return () -> new SortP<>(comparator);
    }

    /**
     * Returns a supplier of a processor that swallows all its normal input (if
     * any), does nothing with it, forwards the watermarks, produces no output
     * and completes immediately. It also swallows any restored snapshot data.
     */
    @Nonnull
    public static SupplierEx<Processor> noopP() {
        return new NoopP.NoopPSupplier();
    }
}
