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

package com.hazelcast.jet.impl.processor;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.TimestampedEntry;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.Watermark;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.DistributedToLongFunction;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.LongStream;

import static com.hazelcast.jet.function.DistributedComparator.naturalOrder;
import static java.lang.Math.min;
import static java.util.Collections.emptyMap;

/**
 * Handles various setups of sliding and tumbling window aggregation.
 * See {@link com.hazelcast.jet.processor.Processors} for more documentation.
 *
 * @param <T> type of input item (stream item in 1st stage, Frame, if 2nd stage)
 * @param <A> type of the frame accumulator object
 * @param <R> type of the finished result
 */
public class SlidingWindowP<T, A, R> extends AbstractProcessor {

    // package-visible for testing
    final Map<Long, Map<Object, A>> tsToKeyToAcc = new HashMap<>();
    final Map<Object, A> slidingWindow = new HashMap<>();

    private final WindowDefinition wDef;
    private final DistributedToLongFunction<? super T> getFrameTimestampF;
    private final Function<? super T, ?> getKeyF;
    private final AggregateOperation1<? super T, A, R> aggrOp;

    private final FlatMapper<Watermark, Object> flatMapper;

    private long nextFrameTsToEmit = Long.MIN_VALUE;
    private final A emptyAcc;
    private Traverser<Object> finalTraverser;

    public SlidingWindowP(
            Function<? super T, ?> getKeyF,
            DistributedToLongFunction<? super T> getFrameTimestampF,
            WindowDefinition winDef,
            AggregateOperation1<? super T, A, R> aggrOp
    ) {
        this.wDef = winDef;
        this.getFrameTimestampF = getFrameTimestampF;
        this.getKeyF = getKeyF;
        this.aggrOp = aggrOp;

        this.flatMapper = flatMapper(
                wm -> windowTraverserAndEvictor(wm.timestamp())
                            .append(wm));
        this.emptyAcc = aggrOp.createAccumulatorF().get();
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        T t = (T) item;
        final Long frameTimestamp = getFrameTimestampF.applyAsLong(t);
        assert frameTimestamp == wDef.floorFrameTs(frameTimestamp) : "timestamp not on the verge of a frame";
        final Object key = getKeyF.apply(t);
        A acc = tsToKeyToAcc.computeIfAbsent(frameTimestamp, x -> new HashMap<>())
                            .computeIfAbsent(key, k -> aggrOp.createAccumulatorF().get());
        aggrOp.accumulateItemF().accept(acc, t);
        return true;
    }

    @Override
    protected boolean tryProcessWm0(@Nonnull Watermark wm) {
        return flatMapper.tryProcess(wm);
    }

    @Override
    public boolean complete() {
        if (finalTraverser == null) {
            if (tsToKeyToAcc.isEmpty()) {
                return true;
            }
            long topTs = tsToKeyToAcc
                    .keySet().stream()
                    .max(naturalOrder())
                    .get();
            finalTraverser = windowTraverserAndEvictor(topTs + wDef.frameLength());
        }
        return emitFromTraverser(finalTraverser);
    }

    private Traverser<Object> windowTraverserAndEvictor(long endTsExclusive) {
        if (nextFrameTsToEmit == Long.MIN_VALUE) {
            if (tsToKeyToAcc.isEmpty()) {
                return Traversers.empty();
            }
            // This is the first watermark we are acting upon. Find the lowest frame
            // timestamp that can be emitted: at most the top existing timestamp lower
            // than wm, but even lower than that if there are older frames on record.
            // The above guarantees that the sliding window can be correctly
            // initialized using the "add leading/deduct trailing" approach because we
            // start from a window that covers at most one existing frame -- the lowest
            // one on record.
            long bottomTs = tsToKeyToAcc
                    .keySet().stream()
                    .min(naturalOrder())
                    .orElseThrow(() -> new AssertionError("Failed to find the min key in a non-empty map"));
            nextFrameTsToEmit = min(bottomTs, wDef.floorFrameTs(endTsExclusive));
        }

        long rangeStart = nextFrameTsToEmit;
        nextFrameTsToEmit = wDef.higherFrameTs(endTsExclusive);
        return Traversers.traverseStream(range(rangeStart, nextFrameTsToEmit, wDef.frameLength()).boxed())
                         .flatMap(frameTs -> Traversers.traverseIterable(computeWindow(frameTs).entrySet())
                               .map(e -> new TimestampedEntry<>(
                                       frameTs, e.getKey(), aggrOp.finishAccumulationF().apply(e.getValue())))
                               .onFirstNull(() -> completeWindow(frameTs)));
    }

    private Map<Object, A> computeWindow(long frameTs) {
        if (wDef.isTumbling()) {
            return tsToKeyToAcc.getOrDefault(frameTs, emptyMap());
        }
        if (aggrOp.deductAccumulatorF() != null) {
            // add leading-edge frame
            patchSlidingWindow(aggrOp.combineAccumulatorsF(), tsToKeyToAcc.get(frameTs));
            return slidingWindow;
        }
        // without deductF we have to recompute the window from scratch
        Map<Object, A> window = new HashMap<>();
        for (long ts = frameTs - wDef.windowLength() + wDef.frameLength(); ts <= frameTs; ts += wDef.frameLength()) {
            tsToKeyToAcc.getOrDefault(ts, emptyMap())
                        .forEach((key, currAcc) -> aggrOp.combineAccumulatorsF().accept(
                                  window.computeIfAbsent(key, k -> aggrOp.createAccumulatorF().get()),
                                  currAcc));
        }
        return window;
    }

    private void completeWindow(long frameTs) {
        Map<Object, A> evictedFrame = tsToKeyToAcc.remove(frameTs - wDef.windowLength() + wDef.frameLength());
        if (aggrOp.deductAccumulatorF() != null) {
            // deduct trailing-edge frame
            patchSlidingWindow(aggrOp.deductAccumulatorF(), evictedFrame);
        }
    }

    private void patchSlidingWindow(BiConsumer<? super A, ? super A> patchOp, Map<Object, A> patchingFrame) {
        if (patchingFrame == null) {
            return;
        }
        for (Entry<Object, A> e : patchingFrame.entrySet()) {
            slidingWindow.compute(e.getKey(), (k, acc) -> {
                A result = acc != null ? acc : aggrOp.createAccumulatorF().get();
                patchOp.accept(result, e.getValue());
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
