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

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Distributed.ToLongFunction;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.LongStream;

import static com.hazelcast.jet.Distributed.Comparator.naturalOrder;
import static java.lang.Math.min;
import static java.util.Collections.emptyMap;

/**
 * See {@link WindowingProcessors}.
 *
 * @param <T> type of input item (stream item in 1st stage, Frame, if 2nd stage)
 * @param <A> type of the frame accumulator object
 * @param <R> type of the finished result
 */
class WindowingProcessor<T, A, R> extends AbstractProcessor {

    // package-visible for testing
    final Map<Long, Map<Object, A>> tsToKeyToFrame = new HashMap<>();
    final Map<Object, A> slidingWindow = new HashMap<>();

    private final WindowDefinition wDef;
    private final ToLongFunction<? super T> extractFrameTimestampF;
    private final Function<? super T, ?> extractKeyF;
    private final WindowOperation<? super T, A, R> winOp;

    private final FlatMapper<Punctuation, Object> flatMapper;

    private long nextFrameTsToEmit = Long.MIN_VALUE;
    private final A emptyAcc;

    WindowingProcessor(
            WindowDefinition winDef,
            ToLongFunction<? super T> extractFrameTimestampF,
            Function<? super T, ?> extractKeyF,
            WindowOperation<? super T, A, R> winOp) {
        this.wDef = winDef;
        this.extractFrameTimestampF = extractFrameTimestampF;
        this.extractKeyF = extractKeyF;
        this.winOp = winOp;

        this.flatMapper = flatMapper(this::windowTraverser);
        this.emptyAcc = winOp.createAccumulatorF().get();
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        T t = (T) item;
        final Long frameTimestamp = extractFrameTimestampF.applyAsLong(t);
        final Object key = extractKeyF.apply(t);
        tsToKeyToFrame.computeIfAbsent(frameTimestamp, x -> new HashMap<>())
                      .compute(key, (x, acc) ->
                        winOp.accumulateItemF().apply(acc == null ? winOp.createAccumulatorF().get() : acc, t));
        return true;
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        if (nextFrameTsToEmit == Long.MIN_VALUE) {
            if (tsToKeyToFrame.isEmpty()) {
                // There are no frames on record; just forward the punctuation.
                return tryEmit(punc);
            }
            // This is the first punctuation we are acting upon. Find the lowest frame
            // timestamp that can be emitted: at most the top existing timestamp lower
            // than punc, but even lower than that if there are older frames on record.
            // The above guarantees that the sliding window can be correctly
            // initialized using the "add leading/deduct trailing" approach because we
            // start from a window that covers at most one existing frame -- the lowest
            // one on record.
            long bottomTs = tsToKeyToFrame
                    .keySet().stream()
                    .min(naturalOrder())
                    .orElseThrow(() -> new AssertionError("Failed to find the min key in a non-empty map"));
            nextFrameTsToEmit = min(bottomTs, wDef.floorFrameTs(punc.timestamp()));
        }
        return flatMapper.tryProcess(punc);
    }

    private Traverser<Object> windowTraverser(Punctuation punc) {
        long rangeStart = nextFrameTsToEmit;
        nextFrameTsToEmit = wDef.higherFrameTs(punc.timestamp());
        return Traversers.traverseStream(range(rangeStart, nextFrameTsToEmit, wDef.frameLength()).boxed())
                .<Object>flatMap(frameTs -> Traversers.traverseIterable(computeWindow(frameTs).entrySet())
                        .map(e -> new TimestampedEntry<>(
                                frameTs, e.getKey(), winOp.finishAccumulationF().apply(e.getValue())))
                        .onFirstNull(() -> completeWindow(frameTs)))
                .append(punc);
    }

    private Map<Object, A> computeWindow(long frameTs) {
        if (wDef.isTumbling()) {
            return tsToKeyToFrame.getOrDefault(frameTs, emptyMap());
        }
        if (winOp.deductAccumulatorF() != null) {
            // add leading-edge frame
            patchSlidingWindow(winOp.combineAccumulatorsF(), tsToKeyToFrame.get(frameTs));
            return slidingWindow;
        }
        // without deductF we have to recompute the window from scratch
        Map<Object, A> window = new HashMap<>();
        for (long ts = frameTs - wDef.windowLength() + wDef.frameLength(); ts <= frameTs; ts += wDef.frameLength()) {
            tsToKeyToFrame.getOrDefault(ts, emptyMap())
                          .forEach((key, currAcc) -> window.compute(key, (x, acc) -> winOp.combineAccumulatorsF().apply(
                            acc != null ? acc : winOp.createAccumulatorF().get(), currAcc)));
        }
        return window;
    }

    private void completeWindow(long frameTs) {
        Map<Object, A> evictedFrame = tsToKeyToFrame.remove(frameTs - wDef.windowLength() + wDef.frameLength());
        if (winOp.deductAccumulatorF() != null) {
            // deduct trailing-edge frame
            patchSlidingWindow(winOp.deductAccumulatorF(), evictedFrame);
        }
    }

    private void patchSlidingWindow(BinaryOperator<A> patchOp, Map<Object, A> patchingFrame) {
        if (patchingFrame == null) {
            return;
        }
        for (Entry<Object, A> e : patchingFrame.entrySet()) {
            slidingWindow.compute(e.getKey(), (k, acc) -> {
                A result = patchOp.apply(acc != null ? acc : winOp.createAccumulatorF().get(), e.getValue());
                return result.equals(emptyAcc) ? null : result;
            });
        }
    }

    private static LongStream range(long start, long end, long step) {
        return start >= end
                ? LongStream.empty()
                : LongStream.iterate(start, n -> n + step).limit(1 + (end - start - 1) / step);
    }
}
