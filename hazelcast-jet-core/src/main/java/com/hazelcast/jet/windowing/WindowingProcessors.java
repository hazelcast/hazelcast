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

package com.hazelcast.jet.windowing;

import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.Distributed.Function.identity;

/**
 * Contains factory methods for processors dealing with windowing operations.
 *
 * <h1>Two-stage aggregation</h1>
 *
 * This way of processing aggregates events first on local member and
 * only pre-accumulated results are sent over the network. This greatly
 * reduces the amount of data sent over the network. The DAG is:
 *
 * <pre>
 *          source
 *             |
 *             | (local, partitioned edge)
 *             V
 *     slidingWindowStage1
 *             |
 *             | (distributed, partitioned edge)
 *             V
 *     slidingWindowStage2
 *             |
 *             | (resulting frames)
 *             V
 * </pre>
 *
 * You should use the same {@link WindowDefinition} and {@link
 * WindowOperation} for both stages. Disadvantage of this setup is, that
 * you will process all keys on each node in the cluster.
 * <p>
 * If you have too many keys to fit one machine, you have to use a single
 * stage processor. Session windows are always processed in single stage.
 * The DAG:
 *
 * <pre>
 *          source
 *             |
 *             | (distributed, partitioned edge)
 *             V
 *   slidingWindowSingleStage
 *     (or sessionWindow)
 *             |
 *             | (resulting frames)
 *             V
 * </pre>
 */
public final class WindowingProcessors {

    private static final String GLOBAL_WINDOW_KEY = "ALL";

    private WindowingProcessors() {
    }

    /**
     * A processor that inserts punctuation into a data stream. A punctuation
     * item contains a {@code puncSeq} value with this meaning: "there will be
     * no more items in this stream with {@code eventSeq < puncSeq}". The value
     * of punctuation is determined by a separate policy object of type
     * {@link PunctuationPolicy}.
     *
     * @param <T> the type of stream item
     */
    @Nonnull
    public static <T> Distributed.Supplier<Processor> insertPunctuation(
            @Nonnull Distributed.ToLongFunction<T> extractEventSeqF,
            @Nonnull Distributed.Supplier<PunctuationPolicy> newPuncPolicyF
    ) {
        return () -> new InsertPunctuationP<>(extractEventSeqF, newPuncPolicyF.get());
    }

    /**
     * A first stage processor in two-stage sliding window aggregation. See
     * Two-stage aggregation {@link WindowingProcessors here}.
     * <p>
     * This vertex is supposed to be followed by {@link
     * #slidingWindowStage2(WindowDefinition, WindowOperation)}.
     *<p>
     * Output if this vertex is {@link Frame Frame&lt;K, A>}, that is a frame
     * with a key and accumulator from {@code windowOperation}.
     *<p>
     * A frame is identified by its {@code long frameSeq} and {@link
     * WindowDefinition#higherFrameSeq(long)} maps the item's {@code
     * eventSeq} to its {@code frameSeq}.
     * <p>
     * Within a frame items are further classified by a grouping key determined
     * by the {@code extractKeyF} function. When the processor receives a
     * punctuation with a given {@code puncSeq}, it emits the current state of
     * all frames with {@code frameSeq <= puncSeq} and deletes these frames
     * from its storage.
     *
     * @param <T> item type
     * @param <K> type of key returned from {@code extractKeyF}
     * @param <A> type of accumulator returned from {@code windowOperation.
     *            createAccumulatorF()}
     */
    @Nonnull
    public static <T, K, A> Distributed.Supplier<Processor> slidingWindowStage1(
            @Nonnull Distributed.Function<? super T, K> extractKeyF,
            @Nonnull Distributed.ToLongFunction<? super T> extractEventSeqF,
            @Nonnull WindowDefinition windowDef,
            @Nonnull WindowOperation<? super T, A, ?> windowOperation
    ) {
        // use a single-frame window in this stage; the downstream processor
        // combines the frames into a window with the user-requested size
        WindowDefinition tumblingWinDef = new WindowDefinition(
                windowDef.frameLength(), windowDef.frameOffset(), 1);

        return () -> new WindowingProcessor<T, A, A>(
                tumblingWinDef,
                item -> tumblingWinDef.higherFrameSeq(extractEventSeqF.applyAsLong(item)),
                extractKeyF,
                WindowOperation.of(
                        windowOperation.createAccumulatorF(),
                        windowOperation.accumulateItemF(),
                        windowOperation.combineAccumulatorsF(),
                        windowOperation.deductAccumulatorF(),
                        identity()
                )
        );
    }

    /**
     * Convenience for {@link #slidingWindowStage1(
     * Distributed.Function, Distributed.ToLongFunction, WindowDefinition, WindowOperation)
     * slidingWindowStage1(extractKeyF, extractEventSeqF, windowDef, windowOperation)}
     * which doesn't group by key.
     */
    @Nonnull
    public static <T, A> Distributed.Supplier<Processor> slidingWindowStage1(
            @Nonnull Distributed.ToLongFunction<? super T> extractEventSeqF,
            @Nonnull WindowDefinition windowDef,
            @Nonnull WindowOperation<? super T, A, ?> windowOperation
    ) {
        return slidingWindowStage1(t -> GLOBAL_WINDOW_KEY, extractEventSeqF, windowDef, windowOperation);
    }

    /**
     * Combines frames received from several upstream instances of {@link
     * #slidingWindowStage1(Function, ToLongFunction, WindowDefinition,
     * WindowOperation)} into finalized frames. Combines finalized frames into
     * sliding windows. Applies the finisher function to produce its emitted
     * output.
     * <p>
     * Output if this vertex is {@link Frame Frame&lt;K, R>}, that is a frame
     * with the key and result value from {@code windowOperation}. Frame's
     * {@link Frame#getSeq() seq} value is the window end sequence (exclusive).
     *
     * @param <A> type of accumulator
     * @param <R> type of the result derived from a frame
     */
    @Nonnull
    public static <A, R> Distributed.Supplier<Processor> slidingWindowStage2(
            @Nonnull WindowDefinition windowDef,
            @Nonnull WindowOperation<?, A, R> windowOperation
    ) {
        return () -> new WindowingProcessor<Frame<?, A>, A, R>(
                windowDef,
                Frame::getSeq,
                Frame::getKey,
                WindowOperation.of(
                        windowOperation.createAccumulatorF(),
                        (acc, frame) -> windowOperation.combineAccumulatorsF().apply(acc, frame.getValue()),
                        windowOperation.combineAccumulatorsF(),
                        windowOperation.deductAccumulatorF(),
                        windowOperation.finishAccumulationF()
                )
        );
    }

    /**
     * A single-stage processor to aggregate events into windows. See note on
     * Two-stage aggregation {@link WindowingProcessors here}.
     * <p>
     * Output if this vertex is {@link Frame Frame&lt;K, R>}, that is a frame
     * with the key and result value from {@code windowOperation}. Frame's
     * {@link Frame#getSeq() seq} value is the window end sequence (exclusive).
     * <p>
     * A frame is identified by its {@code long frameSeq} and {@link
     * WindowDefinition#higherFrameSeq(long)} maps the item's {@code
     * eventSeq} to its {@code frameSeq}.
     * <p>
     * Within a frame items are further classified by a grouping key determined
     * by the {@code extractKeyF} function. When the processor receives a
     * punctuation with a given {@code puncSeq}, it emits the current state of
     * all frames with {@code frameSeq <= puncSeq} and deletes these frames
     * from its storage.
     */
    @Nonnull
    public static <T, A, R> Distributed.Supplier<Processor> slidingWindowSingleStage(
            @Nonnull Distributed.Function<? super T, ?> extractKeyF,
            @Nonnull Distributed.ToLongFunction<? super T> extractEventSeqF,
            @Nonnull WindowDefinition windowDef,
            @Nonnull WindowOperation<? super T, A, R> windowOperation
    ) {
        return () -> new WindowingProcessor<T, A, R>(
                windowDef,
                item -> windowDef.higherFrameSeq(extractEventSeqF.applyAsLong(item)),
                extractKeyF,
                windowOperation
        );
    }

    /**
     * Convenience for {@link #slidingWindowSingleStage(Function,
     * ToLongFunction, WindowDefinition, WindowOperation)
     * slidingWindowSingleStage(extractKeyF, extractEventSeqF, windowDef,
     * windowOperation} which doesn't group by key.
     */
    @Nonnull
    public static <T, A, R> Distributed.Supplier<Processor> slidingWindowSingleStage(
            @Nonnull Distributed.ToLongFunction<? super T> extractEventSeqF,
            @Nonnull WindowDefinition windowDef,
            @Nonnull WindowOperation<? super T, A, R> windowOperation
    ) {
        return slidingWindowSingleStage(t -> GLOBAL_WINDOW_KEY, extractEventSeqF, windowDef, windowOperation);
    }

    /**
     * Aggregates events into session windows. Events and windows under
     * different grouping keys behave independently.
     * <p>
     * The functioning of this processor is easiest to explain in terms of
     * the <em>event interval</em>: the range {@code [eventSeq, eventSeq +
     * maxSeqGap]}. Initially an event causes a new session window to be
     * created, covering exactly the event interval. A following event under
     * the same key belongs to this window iff its interval overlaps it. The
     * window is extended to cover the entire interval of the new event. The
     * event may happen to belong to two existing windows if its interval
     * bridges the gap between them; in that case they are combined into one.
     *
     * @param maxSeqGap        maximum gap between consecutive events in the same session window
     * @param extractEventSeqF function to extract the event seq from the event item
     * @param extractKeyF      function to extract the grouping key from the event iem
     * @param windowOperation        contains aggregation logic
     *
     * @param <T> type of stream event
     * @param <K> type of event's grouping key
     * @param <A> type of the container of accumulated value
     * @param <R> type of the result value for a session window
     */
    @Nonnull
    public static <T, K, A, R> Distributed.Supplier<Processor> sessionWindow(
            long maxSeqGap,
            @Nonnull Distributed.ToLongFunction<? super T> extractEventSeqF,
            @Nonnull Distributed.Function<? super T, K> extractKeyF,
            @Nonnull DistributedCollector<? super T, A, R> windowOperation
    ) {
        return () -> new SessionWindowP<>(maxSeqGap, extractEventSeqF, extractKeyF, windowOperation);
    }
}
