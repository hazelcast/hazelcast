/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.function.KeyedWindowResultFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.LongStream;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.function.DistributedComparator.naturalOrder;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.emptyMap;

/**
 * Handles various setups of sliding and tumbling window aggregation.
 * See {@link com.hazelcast.jet.core.processor.Processors} for more documentation.
 *
 * @param <A> type of the frame accumulator object
 * @param <R> type of the finished result
 */
public class SlidingWindowP<K, A, R, OUT> extends AbstractProcessor {

    // package-visible for testing
    final Map<Long, Map<K, A>> tsToKeyToAcc = new HashMap<>();
    Map<K, A> slidingWindow;
    long nextWinToEmit = Long.MIN_VALUE;

    @Nonnull
    private final SlidingWindowPolicy winPolicy;
    @Nonnull
    private final List<ToLongFunction<Object>> frameTimestampFns;
    @Nonnull
    private final List<Function<Object, ? extends K>> keyFns;
    @Nonnull
    private final AggregateOperation<A, R> aggrOp;
    @Nonnull
    private final KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn;
    @Nullable
    private final BiConsumer<? super A, ? super A> combineFn;
    private final boolean isLastStage;

    @Nonnull
    private final FlatMapper<Watermark, ?> wmFlatMapper;

    @Nonnull
    private final A emptyAcc;
    private Traverser<Object> flushTraverser;
    private Traverser<Entry> snapshotTraverser;

    // This field tracks the upper bound for the keyset of tsToKeyToAcc.
    // It serves as an optimization that avoids a full scan over the
    // entire keyset.
    private long topTs = Long.MIN_VALUE;

    // value to be used temporarily during snapshot restore
    private long minRestoredNextWinToEmit = Long.MAX_VALUE;
    private ProcessingGuarantee processingGuarantee;

    @SuppressWarnings("unchecked")
    public SlidingWindowP(
            @Nonnull List<? extends Function<?, ? extends K>> keyFns,
            @Nonnull List<? extends ToLongFunction<?>> frameTimestampFns,
            @Nonnull SlidingWindowPolicy winPolicy,
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn,
            boolean isLastStage
    ) {
        checkTrue(keyFns.size() == aggrOp.arity(), keyFns.size() + " key functions " +
                "provided for " + aggrOp.arity() + "-arity aggregate operation");
        if (!winPolicy.isTumbling()) {
            checkNotNull(aggrOp.combineFn(), "combine primitive of AggregateOperation is required for sliding windows");
        }
        this.winPolicy = winPolicy;
        this.frameTimestampFns = (List<ToLongFunction<Object>>) frameTimestampFns;
        this.keyFns = (List<Function<Object, ? extends K>>) keyFns;
        this.aggrOp = aggrOp;
        this.combineFn = aggrOp.combineFn();
        this.mapToOutputFn = mapToOutputFn;
        this.isLastStage = isLastStage;
        this.wmFlatMapper = flatMapper(
                wm -> windowTraverserAndEvictor(wm.timestamp())
                        .onFirstNull(() -> nextWinToEmit = this.winPolicy.higherFrameTs(wm.timestamp()))
        );
        this.emptyAcc = aggrOp.createFn().get();
    }

    @Override
    protected void init(@Nonnull Context context) {
        processingGuarantee = context.processingGuarantee();
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        @SuppressWarnings("unchecked")
        final long frameTs = frameTimestampFns.get(ordinal).applyAsLong(item);
        assert frameTs == winPolicy.floorFrameTs(frameTs) : "getFrameTsFn returned an invalid frame timestamp";

        // Ensure the event isn't late. We don't allow a "partially late" event:
        // one which belongs to some windows that are already emitted, even though
        // we still have the frame it belongs to. Such frames were already combined
        // into `slidingWindow` and we can't modify the value because that would
        // disturb the value that we'll deduct from `slidingWindow` later on.
        if (frameTs < nextWinToEmit) {
            if (getLogger().isInfoEnabled()) {
                getLogger().info("Dropped late event: " + item);
            }
            return true;
        }
        final K key = keyFns.get(ordinal).apply(item);
        A acc = tsToKeyToAcc.computeIfAbsent(frameTs, x -> new HashMap<>())
                            .computeIfAbsent(key, k -> aggrOp.createFn().get());
        aggrOp.accumulateFn(ordinal).accept(acc, item);
        topTs = max(topTs, frameTs);
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark wm) {
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
    @SuppressWarnings("unchecked")
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        if (key instanceof BroadcastKey) {
            BroadcastKey bcastKey = (BroadcastKey) key;
            assert Keys.NEXT_WIN_TO_EMIT.equals(bcastKey.key()) : "key=" + bcastKey.key();
            long newNextWinToEmit = (long) value;
            assert processingGuarantee != EXACTLY_ONCE
                    || minRestoredNextWinToEmit == Long.MAX_VALUE
                    || minRestoredNextWinToEmit == newNextWinToEmit
                    : "different values for nextWinToEmit restored, before=" + minRestoredNextWinToEmit
                            + ", new=" + newNextWinToEmit;
            minRestoredNextWinToEmit = Math.min(newNextWinToEmit, minRestoredNextWinToEmit);
            return;
        }
        SnapshotKey k = (SnapshotKey) key;
        if (tsToKeyToAcc.computeIfAbsent(k.timestamp, x -> new HashMap<>())
                        .put((K) k.key, (A) value) != null) {
            throw new JetException("Duplicate key in snapshot: " + k);
        }
        topTs = max(topTs, k.timestamp);
    }

    @Override
    public boolean finishSnapshotRestore() {
        // In the first stage we should theoretically have saved `nextWinToEmit`
        // to the snapshot. We don't bother since the first stage is effectively a
        // tumbling window and it makes no difference in that case. So we don't
        // restore and remain at MIN_VALUE.
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
            rangeStart = min(bottomTs, winPolicy.floorFrameTs(wm));
        }
        return traverseStream(range(rangeStart, wm, winPolicy.frameSize()).boxed())
                .flatMap(winEnd -> traverseIterable(computeWindow(winEnd).entrySet())
                        .map(e -> mapToOutputFn.apply(
                                winEnd - winPolicy.windowSize(), winEnd,
                                e.getKey(), aggrOp.finishFn().apply(e.getValue())))
                        .onFirstNull(() -> completeWindow(winEnd)));
    }

    private Map<K, A> computeWindow(long frameTs) {
        if (winPolicy.isTumbling()) {
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

    private Map<K, A> recomputeWindow(long frameTs) {
        Map<K, A> window = new HashMap<>();
        for (long ts = frameTs - winPolicy.windowSize() + winPolicy.frameSize();
             ts <= frameTs;
             ts += winPolicy.frameSize()
        ) {
            tsToKeyToAcc.getOrDefault(ts, emptyMap())
                        .forEach((key, currAcc) -> combineFn.accept(
                                window.computeIfAbsent(key, k -> aggrOp.createFn().get()),
                                currAcc));
        }
        return window;
    }

    private void patchSlidingWindow(BiConsumer<? super A, ? super A> patchOp, Map<K, A> patchingFrame) {
        if (patchingFrame == null) {
            return;
        }
        for (Entry<K, A> e : patchingFrame.entrySet()) {
            slidingWindow.compute(e.getKey(), (k, acc) -> {
                A result = acc != null ? acc : aggrOp.createFn().get();
                patchOp.accept(result, e.getValue());
                return result.equals(emptyAcc) ? null : result;
            });
        }
    }

    private void completeWindow(long frameTs) {
        long frameToEvict = frameTs - winPolicy.windowSize() + winPolicy.frameSize();
        Map<K, A> evictedFrame = tsToKeyToAcc.remove(frameToEvict);
        if (!winPolicy.isTumbling() && aggrOp.deductFn() != null) {
            // deduct trailing-edge frame
            patchSlidingWindow(aggrOp.deductFn(), evictedFrame);
        }
    }

    private boolean flushBuffers() {
        if (flushTraverser == null) {
            if (tsToKeyToAcc.isEmpty()) {
                return true;
            }
            flushTraverser = windowTraverserAndEvictor(topTs + winPolicy.windowSize() - winPolicy.frameSize())
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
