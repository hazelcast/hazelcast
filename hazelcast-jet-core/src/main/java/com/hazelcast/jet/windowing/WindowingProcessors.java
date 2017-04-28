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
import com.hazelcast.jet.Distributed.BinaryOperator;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.Distributed.Function.identity;

/**
 * Contains factory methods for processors dealing with windowing operations.
 */
public final class WindowingProcessors {

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
     * Groups items into frames. A frame is identified by its {@code
     * long frameSeq} and {@link WindowDefinition#higherFrameSeq(long)} maps
     * the item's {@code eventSeq} to its {@code frameSeq}. Within a frame
     * items are further classified by a grouping key determined by the {@code
     * extractKeyF} function. When the processor receives a punctuation with a
     * given {@code puncSeq}, it emits the current state of all frames with
     * {@code frameSeq <= puncSeq} and deletes these frames from its storage.
     *
     * @param <T> item type
     * @param <K> type of key returned from {@code extractKeyF}
     * @param <A> type of accumulator returned from {@code windowOperation.
     *            createAccumulatorF()}
     */
    @Nonnull
    public static <T, K, A> Distributed.Supplier<Processor> groupByFrame(
            @Nonnull Distributed.Function<? super T, K> extractKeyF,
            @Nonnull Distributed.ToLongFunction<? super T> extractEventSeqF,
            @Nonnull WindowDefinition windowDef,
            @Nonnull WindowOperation<? super T, A, ?> windowOperation
    ) {
        // we'll use window with 1 frames per window, as a subsequent processor will merge subsequent frames
        WindowDefinition tumblingWinDef = new WindowDefinition(
                windowDef.frameLength(), windowDef.frameOffset(), 1);

        return () -> new WindowingProcessor<T, A, A>(
                tumblingWinDef,
                windowOperation.createAccumulatorF(),
                item -> tumblingWinDef.higherFrameSeq(extractEventSeqF.applyAsLong(item)),
                extractKeyF,
                windowOperation.accumulateItemF(),
                windowOperation.combineAccumulatorsF(),
                null, // no need to have it, it's always 1 frame long
                identity()
        );
    }

    /**
     * Convenience for {@link #groupByFrame(
     * Distributed.Function, Distributed.ToLongFunction, WindowDefinition, WindowOperation)
     * groupByFrame(extractKeyF, extractEventSeqF, windowDef, windowOperation)}
     * which doesn't group by key.
     */
    @Nonnull
    public static <T, A> Distributed.Supplier<Processor> groupByFrame(
            @Nonnull Distributed.ToLongFunction<? super T> extractEventSeqF,
            @Nonnull WindowDefinition windowDef,
            @Nonnull WindowOperation<? super T, A, ?> windowOperation
    ) {
        return groupByFrame(t -> "global", extractEventSeqF, windowDef, windowOperation);
    }

    /**
     * Combines frames received from several upstream instances of {@link
     * #groupByFrame(Function, ToLongFunction, WindowDefinition,
     * WindowOperation)} into finalized frames. Combines finalized frames into
     * sliding windows. Applies the finisher function to produce its emitted
     * output.
     *
     * @param <A> type of accumulator
     * @param <R> type of the result derived from a frame
     */
    @Nonnull
    public static <A, R> Distributed.Supplier<Processor> slidingWindow(
            @Nonnull WindowDefinition windowDef,
            @Nonnull WindowOperation<?, A, R> windowOperation
    ) {
        BinaryOperator<A> combineAccumulatorsF = windowOperation.combineAccumulatorsF();
        return () -> new WindowingProcessor<Frame<?, A>, A, R>(
                windowDef,
                windowOperation.createAccumulatorF(),
                Frame::getSeq,
                Frame::getKey,
                (acc, frame) -> combineAccumulatorsF.apply(acc, frame.getValue()),
                combineAccumulatorsF,
                windowOperation.deductAccumulatorF(),
                windowOperation.finishAccumulationF()
        );
    }

    /**
     * TODO
     *
     * @param extractKeyF
     * @param extractEventSeqF
     * @param windowDef
     * @param windowOperation
     * @param <T>
     * @param <A>
     * @param <R>
     * @return
     */
    @Nonnull
    public static <T, A, R> Distributed.Supplier<Processor> oneStageSlidingWindow(
            @Nonnull Distributed.Function<? super T, ?> extractKeyF,
            @Nonnull Distributed.ToLongFunction<? super T> extractEventSeqF,
            @Nonnull WindowDefinition windowDef,
            @Nonnull WindowOperation<? super T, A, R> windowOperation
    ) {
        return () -> new WindowingProcessor<T, A, R>(
                windowDef,
                windowOperation.createAccumulatorF(),
                item -> windowDef.higherFrameSeq(extractEventSeqF.applyAsLong(item)),
                extractKeyF,
                windowOperation.accumulateItemF(),
                windowOperation.combineAccumulatorsF(),
                windowOperation.deductAccumulatorF(),
                windowOperation.finishAccumulationF()
        );
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
