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
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static java.lang.Math.min;
import static java.util.function.Function.identity;

/**
 * Sliding window processor. See {@link
 * WindowingProcessors#slidingWindow(WindowDefinition, WindowOperation, boolean)
 * slidingWindow(frameLength, framesPerWindow, windowToolkit)} for
 * documentation.
 *
 * @param <K> type of the grouping key
 * @param <F> type of the frame accumulator object
 * @param <R> type of the finished result
 */
class SlidingWindowP<K, F, R> extends AbstractProcessor {
    private final WindowDefinition wDef;
    private final Supplier<F> createF;
    private final BinaryOperator<F> combineF;
    private final Distributed.BinaryOperator<F> deductF;
    private final Function<F, R> finishF;

    private final FlatMapper<Punctuation, Object> flatMapper;
    private final F emptyAcc;
    private final NavigableMap<Long, Map<K, F>> seqToKeyToFrame = new TreeMap<>();
    private final Map<K, F> slidingWindow = new HashMap<>();

    private long nextFrameSeqToEmit = Long.MIN_VALUE;

    SlidingWindowP(WindowDefinition winDef, @Nonnull WindowOperation<K, F, R> winOp) {
        this.wDef = winDef;
        this.createF = winOp.createAccumulatorF();
        this.combineF = winOp.combineAccumulatorsF();
        this.deductF = winOp.deductAccumulatorF();
        this.finishF = winOp.finishAccumulationF();
        this.flatMapper = flatMapper(this::slidingWindowTraverser);
        this.emptyAcc = createF.get();
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        final Frame<K, F> e = (Frame) item;
        final Long frameSeq = e.getSeq();
        final F frame = e.getValue();
        seqToKeyToFrame.computeIfAbsent(frameSeq, x -> new HashMap<>())
                       .merge(e.getKey(), frame, combineF);
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
            nextFrameSeqToEmit = min(seqToKeyToFrame.firstKey(), punc.seq());
        }
        return flatMapper.tryProcess(punc);
    }

    private Traverser<Object> slidingWindowTraverser(Punctuation punc) {
        Traverser<Object> traverser = Traversers.traverseStream(
                range(nextFrameSeqToEmit, nextFrameSeqToEmit = wDef.higherFrameSeq(punc.seq()), wDef.frameLength())
                        .mapToObj(frameSeq ->
                                computeWindow(frameSeq, seqToKeyToFrame.remove(frameSeq - wDef.windowLength()))
                                        .entrySet().stream()
                                        .map(e -> new Frame<>(frameSeq, e.getKey(), finishF.apply(e.getValue()))))
                        .flatMap(identity()));

        traverser = traverser.append(punc);
        return traverser;
    }

    private Map<K, F> computeWindow(long frameSeq, Map<K, F> evictedFrame) {
        if (deductF != null) {
            // add leading-edge frame
            patchSlidingWindow(combineF, seqToKeyToFrame.get(frameSeq));
            // deduct trailing-edge frame
            patchSlidingWindow(deductF, evictedFrame);
            return slidingWindow;
        }
        // without deductF we have to recompute the window from scratch
        Map<K, F> window = new HashMap<>();
        Map<Long, Map<K, F>> frames = seqToKeyToFrame.subMap(frameSeq - wDef.windowLength(), false, frameSeq, true);
        for (Map<K, F> keyToFrame : frames.values()) {
            keyToFrame.forEach((key, currAcc) ->
                    window.compute(key, (x, acc) -> combineF.apply(acc != null ? acc : createF.get(), currAcc)));
        }
        return window;
    }

    private void patchSlidingWindow(BinaryOperator<F> patchOp, Map<K, F> patchingFrame) {
        if (patchingFrame == null) {
            return;
        }
        for (Entry<K, F> e : patchingFrame.entrySet()) {
            slidingWindow.compute(e.getKey(), (k, acc) -> {
                F result = patchOp.apply(acc != null ? acc : createF.get(), e.getValue());
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
