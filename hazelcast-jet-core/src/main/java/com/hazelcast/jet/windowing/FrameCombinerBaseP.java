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
import com.hazelcast.jet.Punctuation;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.LongStream;

import static java.lang.Math.min;

/**
 * Base class for {@link SlidingWindowP} and {@link TumblingWindowP}
 *
 * @param <K> type of the grouping key
 * @param <F> type of the frame accumulator object
 * @param <R> type of the finished result
 */
class FrameCombinerBaseP<K, F, R> extends AbstractProcessor {

    // package-visible for test
    final NavigableMap<Long, Map<K, F>> seqToKeyToFrame = new TreeMap<>();
    final Map<K, F> slidingWindow = new HashMap<>();

    final WindowDefinition wDef;
    final Supplier<F> createF;
    final BinaryOperator<F> combineF;
    final Function<F, R> finishF;

    FlatMapper<Punctuation, Object> flatMapper;

    long nextFrameSeqToEmit = Long.MIN_VALUE;

    FrameCombinerBaseP(WindowDefinition winDef, @Nonnull WindowOperation<?, F, R> winOp) {
        this.wDef = winDef;
        this.createF = winOp.createAccumulatorF();
        this.combineF = winOp.combineAccumulatorsF();
        this.finishF = winOp.finishAccumulationF();
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

    static LongStream range(long start, long end, long step) {
        return start >= end
                ? LongStream.empty()
                : LongStream.iterate(start, n -> n + step).limit(1 + (end - start - 1) / step);
    }
}
