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
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BinaryOperator;

import static com.hazelcast.jet.Distributed.Function.identity;
import static java.lang.Math.min;

/**
 * Sliding window processor. See {@link
 * WindowingProcessors#slidingWindow(WindowDefinition, WindowOperation)
 * slidingWindow(windowDef, windowOperation)} for
 * documentation.
 *
 * @param <K> type of the grouping key
 * @param <F> type of the frame accumulator object
 * @param <R> type of the finished result
 */
class SlidingWindowP<K, F, R> extends FrameCombinerBaseP<K, F, R> {

    private final Distributed.BinaryOperator<F> deductF;
    private final F emptyAcc;

    SlidingWindowP(WindowDefinition winDef, @Nonnull WindowOperation<?, F, R> winOp) {
        super(winDef, winOp);
        assert !winDef.isTumbling() : SlidingWindowP.class.getSimpleName() + " used with tumbling window";

        this.deductF = winOp.deductAccumulatorF();
        this.emptyAcc = createF.get();

        this.flatMapper = flatMapper(this::slidingWindowTraverser);
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
}
