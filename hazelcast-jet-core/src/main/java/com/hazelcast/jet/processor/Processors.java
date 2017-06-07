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

package com.hazelcast.jet.processor;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.AggregateOperation;
import com.hazelcast.jet.Inbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.PunctuationPolicy;
import com.hazelcast.jet.ResettableSingletonTraverser;
import com.hazelcast.jet.TimestampKind;
import com.hazelcast.jet.TimestampedEntry;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.processor.AggregateP;
import com.hazelcast.jet.impl.processor.GroupByKeyP;
import com.hazelcast.jet.impl.processor.InsertPunctuationP;
import com.hazelcast.jet.impl.processor.SessionWindowP;
import com.hazelcast.jet.impl.processor.SlidingWindowP;
import com.hazelcast.jet.impl.processor.TransformP;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map.Entry;

import static com.hazelcast.jet.TimestampKind.EVENT;
import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;

/**
 * Static utility class with factory methods for Jet processors. These
 * are meant to implement the internal vertices of the DAG; for other
 * kinds of processors refer to the {@link com.hazelcast.jet.processor
 * package-level documentation}.
 * <p>
 * Many of the processors deal with an aggregating operation over stream
 * items. Prior to aggregation items may be grouped by an arbitrary key
 * and/or an event timestamp-based window. There are two main aggregation
 * setups: single-stage and two-stage.
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
 * {@link AggregateOperation#accumulateItemF() accumulate} aggregation
 * primitive and the second stage does {@link
 * AggregateOperation#combineAccumulatorsF() combine} and {@link
 * AggregateOperation#finishAccumulationF() finish}. The essential property
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
 *     <td>{@link #aggregate(AggregateOperation) aggregate()}</td>
 *     <td>{@link #accumulate(AggregateOperation) accumulate()}</td>
 *     <td>{@link #combine(AggregateOperation) combine()}</td>
 * </tr><tr>
 *     <th>batch, group by key</th>
 *
 *     <td>{@link #aggregateByKey(DistributedFunction, AggregateOperation)
 *          aggregateByKey()}</td>
 *     <td>{@link #accumulateByKey(DistributedFunction, AggregateOperation)
 *          accumulateByKey()}</td>
 *     <td>{@link #combineByKey(AggregateOperation) combineByKey()}</td>
 * </tr><tr>
 *     <th>stream, group by key<br>and aligned window</th>
 *
 *     <td>{@link #aggregateToSlidingWindow(DistributedFunction, DistributedToLongFunction,
 *          TimestampKind, WindowDefinition, AggregateOperation)
 *          aggregateToSlidingWindow()}</td>
 *     <td>{@link #accumulateByFrame(DistributedFunction, DistributedToLongFunction,
 *          TimestampKind, WindowDefinition, AggregateOperation)
 *          accumulateByFrame()}</td>
 *     <td>{@link #combineToSlidingWindow(WindowDefinition, AggregateOperation)
 *          combineToSlidingWindow()}</td>
 * </tr><tr>
 *     <th>stream, group by key<br>and session window</th>
 *     <td>{@link #aggregateToSessionWindow(long, DistributedToLongFunction,
 *          DistributedFunction, AggregateOperation)
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
     * Returns a supplier of processor that groups items by key and performs
     * the provided aggregate operation on each group. After exhausting all
     * its input it emits one {@code Map.Entry<K, R>} per observed key.
     * <p>
     * Since the input to this processor must be bounded, its primary use case
     * are batch jobs.
     *
     * @param getKeyF computes the key from the entry
     * @param aggregateOperation the aggregate operation to perform
     * @param <T> type of received item
     * @param <K> type of key
     * @param <A> type of accumulator returned from {@code aggregateOperation.
     *            createAccumulatorF()}
     * @param <R> type of the finished result returned from {@code aggregateOperation.
     *            finishAccumulationF()}
     */
    @Nonnull
    public static <T, K, A, R> DistributedSupplier<Processor> aggregateByKey(
            @Nonnull DistributedFunction<? super T, K> getKeyF,
            @Nonnull AggregateOperation<? super T, A, R> aggregateOperation
    ) {
        return () -> new GroupByKeyP<>(getKeyF, aggregateOperation);
    }

    /**
     * Returns a supplier of the first-stage processor in a two-stage
     * group-and-aggregate setup. The processor groups items by the grouping
     * key (as obtained from the given key-extracting function) and applies
     * the {@link AggregateOperation#accumulateItemF() accumulate} aggregation
     * primitive to each group. After exhausting all its input it emits one
     * {@code Map.Entry<K, A>} per observed key.
     * <p>
     * Since the input to this processor must be bounded, its primary use case
     * are batch jobs.
     *
     * @param getKeyF computes the key from the entry
     * @param aggregateOperation the aggregate operation to perform
     * @param <T> type of received item
     * @param <K> type of key
     * @param <A> type of accumulator returned from {@code aggregateOperation.
     *            createAccumulatorF()}
     */
    @Nonnull
    public static <T, K, A> DistributedSupplier<Processor> accumulateByKey(
            @Nonnull DistributedFunction<? super T, K> getKeyF,
            @Nonnull AggregateOperation<? super T, A, ?> aggregateOperation
    ) {
        return () -> new GroupByKeyP<>(getKeyF, aggregateOperation.withFinish(identity()));
    }

    /**
     * Returns a supplier of the second-stage processor in a two-stage
     * group-and-aggregate setup. It applies the {@link
     * AggregateOperation#combineAccumulatorsF() combine} aggregation
     * primitive to the entries received from several upstream instances of
     * {@link #accumulateByKey(DistributedFunction, AggregateOperation)
     * accumulateByKey()}. After exhausting all its input it emits one
     * {@code Map.Entry<K, R>} per observed key.
     * <p>
     * Since the input to this processor must be bounded, its primary use case
     * are batch jobs.
     *
     * @param aggregateOperation the aggregate operation to perform
     * @param <A> type of accumulator returned from {@code
     *            aggregateOperation.createAccumulatorF()}
     * @param <R> type of the finished result returned from {@code aggregateOperation.
     *            finishAccumulationF()}
     */
    @Nonnull
    public static <A, R> DistributedSupplier<Processor> combineByKey(
            @Nonnull AggregateOperation<?, A, R> aggregateOperation
    ) {
        return () -> new GroupByKeyP<>(Entry::getKey,
                withCombiningAccumulate(Entry<Object, A>::getValue, aggregateOperation));
    }

    /**
     * Returns a supplier of processor that performs the provided aggregate
     * operation on all the items it receives. After exhausting all its input
     * it emits a single item of type {@code R} &mdash;the result of the
     * aggregate operation.
     * <p>
     * Since the input to this processor must be bounded, its primary use case
     * is batch jobs.
     *
     * @param aggregateOperation the aggregate operation to perform
     * @param <T> type of received item
     * @param <A> type of accumulator returned from {@code
     *            aggregateOperation.createAccumulatorF()}
     * @param <R> type of the finished result returned from {@code aggregateOperation.
     *            finishAccumulationF()}
     */
    @Nonnull
    public static <T, A, R> DistributedSupplier<Processor> aggregate(
            @Nonnull AggregateOperation<T, A, R> aggregateOperation
    ) {
        return () -> new AggregateP<>(aggregateOperation);
    }

    /**
     * Returns a supplier of processor that performs the provided aggregate
     * operation on all the items it receives. After exhausting all its input
     * it emits a single item of type {@code R} &mdash;the result of the
     * aggregate operation.
     * <p>
     * Since the input to this processor must be bounded, its primary use case
     * are batch jobs.
     *
     * @param aggregateOperation the aggregate operation to perform
     * @param <T> type of received item
     * @param <A> type of accumulator returned from {@code
     *            aggregateOperation.createAccumulatorF()}
     * @param <R> type of the finished result returned from {@code aggregateOperation.
     *            finishAccumulationF()}
     */
    @Nonnull
    public static <T, A, R> DistributedSupplier<Processor> accumulate(
            @Nonnull AggregateOperation<T, A, R> aggregateOperation
    ) {
        return () -> new AggregateP<>(aggregateOperation.withFinish(identity()));
    }

    /**
     * Returns a supplier of processor that performs the provided aggregate
     * operation on all the items it receives. After exhausting all its input
     * it emits a single item of type {@code R} &mdash;the result of the
     * aggregate operation.
     * <p>
     * Since the input to this processor must be bounded, its primary use case
     * are batch jobs.
     *
     * @param aggregateOperation the aggregate operation to perform
     * @param <T> type of received item
     * @param <A> type of accumulator returned from {@code
     *            aggregateOperation.createAccumulatorF()}
     * @param <R> type of the finished result returned from {@code aggregateOperation.
     *            finishAccumulationF()}
     */
    @Nonnull
    public static <T, A, R> DistributedSupplier<Processor> combine(
            @Nonnull AggregateOperation<T, A, R> aggregateOperation
    ) {
        return () -> new AggregateP<>(withCombiningAccumulate(identity(), aggregateOperation));
    }

    /**
     * Returns a supplier of processor that aggregates events into a sliding
     * window in a single stage (see the {@link Processors class
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
        return Processors.<T, K, A, R>aggregateByKeyAndWindow(
                getKeyF,
                getTimestampF,
                timestampKind,
                windowDef,
                aggregateOperation
        );
    }

    /**
     * Returns a supplier of the first-stage processor in a two-stage sliding
     * window aggregation setup (see the {@link Processors class
     * Javadoc} for an explanation of aggregation stages). The processor groups
     * items by the grouping key (as obtained from the given key-extracting
     * function) and by <em>frame</em>, which is a range of timestamps equal to
     * the sliding step. It applies the {@link
     * AggregateOperation#accumulateItemF() accumulate} aggregation primitive to
     * each key-frame group.
     * <p>
     * The frame is identified by the timestamp denoting its end time (equal to
     * the exclusive upper bound of its timestamp range). {@link
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
    public static <T, K, A> DistributedSupplier<Processor> accumulateByFrame(
            @Nonnull DistributedFunction<? super T, K> getKeyF,
            @Nonnull DistributedToLongFunction<? super T> getTimestampF,
            @Nonnull TimestampKind timestampKind,
            @Nonnull WindowDefinition windowDef,
            @Nonnull AggregateOperation<? super T, A, ?> aggregateOperation
    ) {
        WindowDefinition tumblingByFrame = windowDef.toTumblingByFrame();
        return Processors.<T, K, A, A>aggregateByKeyAndWindow(
                getKeyF,
                getTimestampF,
                timestampKind,
                tumblingByFrame,
                aggregateOperation.withFinish(identity())
        );
    }

    /**
     * Returns a supplier of the second-stage processor in a two-stage sliding
     * window aggregation setup (see the {@link Processors class
     * Javadoc} for an explanation of aggregation stages). It applies the
     * {@link AggregateOperation#combineAccumulatorsF() combine} aggregation
     * primitive to frames received from several upstream instances of {@link
     * #accumulateByFrame(DistributedFunction, DistributedToLongFunction,
     * TimestampKind, WindowDefinition, AggregateOperation)
     * accumulateByFrame()}. It emits sliding window results labeled with
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
                withCombiningAccumulate(TimestampedEntry<K, A>::getValue, aggregateOperation)
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
    public static <T, K, A, R> DistributedSupplier<Processor> aggregateToSessionWindow(
            long sessionTimeout,
            @Nonnull DistributedToLongFunction<? super T> getTimestampF,
            @Nonnull DistributedFunction<? super T, K> getKeyF,
            @Nonnull AggregateOperation<? super T, A, R> aggregateOperation
    ) {
        return () -> new SessionWindowP<>(sessionTimeout, getTimestampF, getKeyF, aggregateOperation);
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
     * Returns a supplier of processor which, for each received item, emits the
     * result of applying the given mapping function to it. If the result is
     * {@code null}, it emits nothing. Therefore this processor can be used to
     * implement filtering semantics as well.
     *
     * @param mapper the mapping function
     * @param <T> type of received item
     * @param <R> type of emitted item
     */
    @Nonnull
    public static <T, R> DistributedSupplier<Processor> map(
            @Nonnull DistributedFunction<T, R> mapper
    ) {
        return () -> {
            final ResettableSingletonTraverser<R> trav = new ResettableSingletonTraverser<>();
            return new TransformP<T, R>(item -> {
                trav.accept(mapper.apply(item));
                return trav;
            });
        };
    }

    /**
     * Returns a supplier of processor which emits the same items it receives,
     * but only those that pass the given predicate.
     *
     * @param predicate the predicate to test each received item against
     * @param <T> type of received item
     */
    @Nonnull
    public static <T> DistributedSupplier<Processor> filter(@Nonnull DistributedPredicate<T> predicate) {
        return () -> {
            final ResettableSingletonTraverser<T> trav = new ResettableSingletonTraverser<>();
            return new TransformP<T, T>(item -> {
                trav.accept(predicate.test(item) ? item : null);
                return trav;
            });
        };
    }

    /**
     * Returns a supplier of processor which applies the provided
     * item-to-traverser mapping function to each received item and emits all
     * the items from the resulting traverser.
     *
     * @param mapper function that maps the received item to a traverser over output items
     * @param <T> received item type
     * @param <R> emitted item type
     */
    @Nonnull
    public static <T, R> DistributedSupplier<Processor> flatMap(
            @Nonnull DistributedFunction<T, ? extends Traverser<? extends R>> mapper
    ) {
        return () -> new TransformP<T, R>(mapper);
    }

    /**
     * Returns a supplier of processor that consumes all its input (if any) and
     * does nothing with it.
     */
    @Nonnull
    public static DistributedSupplier<Processor> noop() {
        return NoopP::new;
    }

    /**
     * Decorates a {@code ProcessorSupplier} with one that will declare all its
     * processors non-cooperative. The wrapped supplier must return processors
     * that are {@code instanceof} {@link AbstractProcessor}.
     */
    @Nonnull
    public static ProcessorSupplier nonCooperative(@Nonnull ProcessorSupplier wrapped) {
        return count -> {
            final Collection<? extends Processor> ps = wrapped.get(count);
            ps.forEach(p -> ((AbstractProcessor) p).setCooperative(false));
            return ps;
        };
    }

    /**
     * Decorates a {@code Supplier<Processor>} into one that will declare
     * its processors non-cooperative. The wrapped supplier must return
     * processors that are {@code instanceof} {@link AbstractProcessor}.
     */
    @Nonnull
    public static DistributedSupplier<Processor> nonCooperative(@Nonnull DistributedSupplier<Processor> wrapped) {
        return () -> {
            final Processor p = wrapped.get();
            ((AbstractProcessor) p).setCooperative(false);
            return p;
        };
    }

    private static <T, A, R> AggregateOperation<T, A, R> withCombiningAccumulate(
            @Nonnull DistributedFunction<T, A> mapper,
            @Nonnull AggregateOperation<?, A, R> aggregateOperation
    ) {
        return aggregateOperation.withAccumulate(
                (A acc, T item) ->
                        aggregateOperation.combineAccumulatorsF().accept(acc, mapper.apply(item)));
    }

    /** A no-operation processor. See {@link #noop()} */
    private static class NoopP implements Processor {
        @Override
        public void process(int ordinal, @Nonnull Inbox inbox) {
            inbox.drain(noopConsumer());
        }
    }
}
