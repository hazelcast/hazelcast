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
 * @param <T> type of input item (stream item or frame, if 2nd step)
 * @param <A> type of the frame accumulator object
 * @param <R> type of the finished result
 */
class WindowingProcessor<T, A, R> extends AbstractProcessor {

    // package-visible for testing
    final Map<Long, Map<Object, A>> seqToKeyToFrame = new HashMap<>();
    final Map<Object, A> slidingWindow = new HashMap<>();

    private final WindowDefinition wDef;
    private final ToLongFunction<? super T> extractFrameSeqF;
    private final Function<? super T, ?> extractKeyF;
    private final WindowOperation<? super T, A, R> winOp;

    private final FlatMapper<Punctuation, Object> flatMapper;

    private long nextFrameSeqToEmit = Long.MIN_VALUE;
    private final A emptyAcc;

    WindowingProcessor(
            WindowDefinition winDef,
            ToLongFunction<? super T> extractFrameSeqF,
            Function<? super T, ?> extractKeyF,
            WindowOperation<? super T, A, R> winOp) {
        this.wDef = winDef;
        this.extractFrameSeqF = extractFrameSeqF;
        this.extractKeyF = extractKeyF;
        this.winOp = winOp;

        this.flatMapper = flatMapper(this::windowTraverser);
        this.emptyAcc = winOp.createAccumulatorF().get();
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        T t = (T) item;
        final Long frameSeq = extractFrameSeqF.applyAsLong(t);
        final Object key = extractKeyF.apply(t);
        seqToKeyToFrame.computeIfAbsent(frameSeq, x -> new HashMap<>())
                .compute(key, (x, acc) ->
                        winOp.accumulateItemF().apply(acc == null ? winOp.createAccumulatorF().get() : acc, t));
        return true;
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        if (nextFrameSeqToEmit == Long.MIN_VALUE) {
            if (seqToKeyToFrame.isEmpty()) {
                // We have no data, just forward the punctuation.
                return tryEmit(punc);
            }
            // This is the first punctuation we are acting upon. Find the lowest
            // frameSeq that can be emitted: at most the top existing frameSeq lower
            // than punc seq, but even lower than that if there are older frames on
            // record. The above guarantees that the sliding window can be correctly
            // initialized using the "add leading/deduct trailing" approach because we
            // start from a window that covers at most one existing frame -- the lowest
            // one on record.
            long firstKey = seqToKeyToFrame
                    .keySet().stream()
                    .min(naturalOrder())
                    .orElseThrow(() -> new AssertionError("Failed to find the min key in a non-empty map"));
            nextFrameSeqToEmit = min(firstKey, punc.seq());
        }
        return flatMapper.tryProcess(punc);
    }

    private Traverser<Object> windowTraverser(Punctuation punc) {
        long rangeStart = nextFrameSeqToEmit;
        nextFrameSeqToEmit = wDef.higherFrameSeq(punc.seq());
        return Traversers.traverseStream(range(rangeStart, nextFrameSeqToEmit, wDef.frameLength()).boxed())
                .<Object>flatMap(frameSeq -> Traversers.traverseIterable(computeWindow(frameSeq).entrySet())
                        .map(e -> new Frame<>(frameSeq, e.getKey(), winOp.finishAccumulationF().apply(e.getValue())))
                        .onFirstNull(() -> completeWindow(frameSeq)))
                .append(punc);
    }

    private Map<Object, A> computeWindow(long frameSeq) {
        if (wDef.isTumbling()) {
            return seqToKeyToFrame.getOrDefault(frameSeq, emptyMap());
        }
        if (winOp.deductAccumulatorF() != null) {
            // add leading-edge frame
            patchSlidingWindow(winOp.combineAccumulatorsF(), seqToKeyToFrame.get(frameSeq));
            return slidingWindow;
        }
        // without deductF we have to recompute the window from scratch
        Map<Object, A> window = new HashMap<>();
        for (long seq = frameSeq - wDef.windowLength() + wDef.frameLength(); seq <= frameSeq; seq += wDef.frameLength()) {
            seqToKeyToFrame.getOrDefault(seq, emptyMap())
                    .forEach((key, currAcc) -> window.compute(key, (x, acc) -> winOp.combineAccumulatorsF().apply(
                            acc != null ? acc : winOp.createAccumulatorF().get(), currAcc)));
        }
        return window;
    }

    private void completeWindow(long frameSeq) {
        Map<Object, A> evictedFrame = seqToKeyToFrame.remove(frameSeq - wDef.windowLength() + wDef.frameLength());
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
