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

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.WindowDefinition;
import com.hazelcast.jet.function.DistributedToLongFunction;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.LongStream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.function.DistributedComparator.naturalOrder;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.emptyMap;

/**
 * Handles various setups of sliding and tumbling window aggregation.
 * See {@link com.hazelcast.jet.core.processor.Processors} for more documentation.
 *
 * @param <T> type of input item (stream item in 1st stage, Frame, if 2nd stage)
 * @param <A> type of the frame accumulator object
 * @param <R> type of the finished result
 */
public class SlidingWindowP<T, A, R> extends AbstractProcessor {

    // package-visible for testing
    final Map<Long, Map<Object, A>> tsToKeyToAcc = new HashMap<>();
    Map<Object, A> slidingWindow;
    long nextWinToEmit = Long.MIN_VALUE;

    private final WindowDefinition wDef;
    private final DistributedToLongFunction<? super T> getFrameTsFn;
    private final Function<? super T, ?> getKeyFn;
    private final AggregateOperation1<? super T, A, R> aggrOp;
    private final boolean isLastStage;

    private final FlatMapper<Watermark, ?> wmFlatMapper;

    private final A emptyAcc;
    private Traverser<Object> flushTraverser;
    private Traverser<Entry> snapshotTraverser;

    // This field tracks the upper bounds for the keyset of
    // tsToKeyToAcc. It serves as an optimization that avoids a full scan
    // over the entire keyset.
    private long topTs = Long.MIN_VALUE;

    // value to be used temporarily during snapshot restore
    private long minRestoredNextWinToEmit = Long.MAX_VALUE;
    private ProcessingGuarantee processingGuarantee;

    public SlidingWindowP(
            Function<? super T, ?> getKeyFn,
            DistributedToLongFunction<? super T> getFrameTsFn,
            WindowDefinition winDef,
            AggregateOperation1<? super T, A, R> aggrOp,
            boolean isLastStage
    ) {
        if (!winDef.isTumbling()) {
            checkNotNull(aggrOp.combineFn(), "AggregateOperation lacks the combine primitive");
        }
        this.wDef = winDef;
        this.getFrameTsFn = getFrameTsFn;
        this.getKeyFn = getKeyFn;
        this.aggrOp = aggrOp;
        this.isLastStage = isLastStage;
        this.wmFlatMapper = flatMapper(
                wm -> windowTraverserAndEvictor(wm.timestamp())
                        .append(wm)
                        .onFirstNull(() -> nextWinToEmit = wDef.higherFrameTs(wm.timestamp()))
        );
        this.emptyAcc = aggrOp.createFn().get();
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        processingGuarantee = context.processingGuarantee();
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        @SuppressWarnings("unchecked")
        T t = (T) item;
        final long frameTs = getFrameTsFn.applyAsLong(t);
        assert frameTs == wDef.floorFrameTs(frameTs) : "getFrameTsFn returned an invalid frame timestamp";
        assert frameTs + wDef.windowLength() >= nextWinToEmit : "late event received, it should have been filtered out " +
                "by InsertWatermarksP: item=" + item + ", nextWinToEmit=" + nextWinToEmit;
        final Object key = getKeyFn.apply(t);
        A acc = tsToKeyToAcc.computeIfAbsent(frameTs, x -> new HashMap<>())
                            .computeIfAbsent(key, k -> aggrOp.createFn().get());
        aggrOp.accumulateFn().accept(acc, t);
        topTs = max(topTs, frameTs);
        return true;
    }

    @Override
    protected boolean tryProcessWm0(@Nonnull Watermark wm) {
        return wmFlatMapper.tryProcess(wm);
    }

    @Override
    public boolean complete() {
        return flushBuffers();
    }

    @Override
    public boolean saveToSnapshot() {
        if (!isLastStage || flushTraverser != null) {
            return flushBuffers();
        }
        if (snapshotTraverser == null) {
            snapshotTraverser = traverseIterable(tsToKeyToAcc.entrySet())
                    .<Entry>flatMap(e -> traverseIterable(e.getValue().entrySet())
                            .map(e2 -> entry(new SnapshotKey(e.getKey(), e2.getKey()), e2.getValue()))
                    )
                    .append(entry(broadcastKey(Keys.NEXT_WIN_TO_EMIT), nextWinToEmit))
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        if (key instanceof BroadcastKey) {
            assert Keys.NEXT_WIN_TO_EMIT.equals(((BroadcastKey) key).key()) : "key=" + ((BroadcastKey) key).key();
            long newNextWinToEmit = (long) value;
            assert processingGuarantee != ProcessingGuarantee.EXACTLY_ONCE
                        || minRestoredNextWinToEmit == Long.MAX_VALUE || minRestoredNextWinToEmit == newNextWinToEmit
                    : "different values for nextWinToEmit restored, before=" + minRestoredNextWinToEmit
                            + ", new=" + newNextWinToEmit;
            minRestoredNextWinToEmit = Math.min(newNextWinToEmit, minRestoredNextWinToEmit);
            return;
        }
        SnapshotKey k = (SnapshotKey) key;
        if (tsToKeyToAcc.computeIfAbsent(k.timestamp, x -> new HashMap<>())
                        .put(k.key, (A) value) != null) {
            throw new JetException("Duplicate key in snapshot: " + k);
        }
        topTs = max(topTs, k.timestamp);
    }

    @Override
    public boolean finishSnapshotRestore() {
        // For first-stage, we should theoretically have saved nextWinToEmit to snapshot. But we don't bother, since the
        // first stage is always tumbling window and it makes no difference in that case. So we don't restore and remain
        // at MIN_VALUE.
        if (isLastStage) {
            nextWinToEmit = minRestoredNextWinToEmit;
        }
        logFine(getLogger(), "Restored nextWinToEmit from snapshot to: %s", nextWinToEmit);
        return true;
    }

    private Traverser<Object> windowTraverserAndEvictor(long wm) {
        long rangeStart;
        if (nextWinToEmit != Long.MIN_VALUE) {
            rangeStart = nextWinToEmit;
        } else {
            if (tsToKeyToAcc.isEmpty()) {
                // no item was observed, but initialize nextWinToEmit to the next window
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
            rangeStart = min(bottomTs, wDef.floorFrameTs(wm));
        }
        return traverseStream(range(rangeStart, wm, wDef.frameLength()).boxed())
                .flatMap(window -> traverseIterable(computeWindow(window).entrySet())
                        .map(e -> new TimestampedEntry<>(window, e.getKey(), aggrOp.finishFn().apply(e.getValue())))
                        .onFirstNull(() -> completeWindow(window)));
    }

    private Map<Object, A> computeWindow(long frameTs) {
        if (wDef.isTumbling()) {
            return tsToKeyToAcc.getOrDefault(frameTs, emptyMap());
        }
        if (aggrOp.deductFn() == null) {
            return recomputeWindow(frameTs);
        }
        if (slidingWindow == null) {
            slidingWindow = recomputeWindow(frameTs);
        } else {
            // add leading-edge frame
            patchSlidingWindow(aggrOp.combineFn(), tsToKeyToAcc.get(frameTs));
        }
        return slidingWindow;
    }

    private Map<Object, A> recomputeWindow(long frameTs) {
        Map<Object, A> window = new HashMap<>();
        for (long ts = frameTs - wDef.windowLength() + wDef.frameLength(); ts <= frameTs; ts += wDef.frameLength()) {
            tsToKeyToAcc.getOrDefault(ts, emptyMap())
                        .forEach((key, currAcc) -> aggrOp.combineFn().accept(
                                window.computeIfAbsent(key, k -> aggrOp.createFn().get()),
                                currAcc));
        }
        return window;
    }

    private void patchSlidingWindow(BiConsumer<? super A, ? super A> patchOp, Map<Object, A> patchingFrame) {
        if (patchingFrame == null) {
            return;
        }
        for (Entry<Object, A> e : patchingFrame.entrySet()) {
            slidingWindow.compute(e.getKey(), (k, acc) -> {
                A result = acc != null ? acc : aggrOp.createFn().get();
                patchOp.accept(result, e.getValue());
                return result.equals(emptyAcc) ? null : result;
            });
        }
    }

    private void completeWindow(long frameTs) {
        long frameToEvict = frameTs - wDef.windowLength() + wDef.frameLength();
        Map<Object, A> evictedFrame = tsToKeyToAcc.remove(frameToEvict);
        if (!wDef.isTumbling() && aggrOp.deductFn() != null) {
            // deduct trailing-edge frame
            patchSlidingWindow(aggrOp.deductFn(), evictedFrame);
        }
    }

    private boolean flushBuffers() {
        if (flushTraverser == null) {
            if (tsToKeyToAcc.isEmpty()) {
                return true;
            }
            flushTraverser = windowTraverserAndEvictor(topTs + wDef.windowLength() - wDef.frameLength())
                    .onFirstNull(() -> flushTraverser = null);
        }
        return emitFromTraverser(flushTraverser);
    }

    /**
     * Returns a stream of {@code long}s:
     * {@code for (long i = start; i <= end; i += step) yield i;}
     */
    private static LongStream range(long start, long end, long step) {
        return start > end
                ? LongStream.empty()
                : LongStream.iterate(start, n -> n + step).limit(1 + (end - start) / step);
    }

    // package-visible for test
    enum Keys {
        NEXT_WIN_TO_EMIT
    }
}
