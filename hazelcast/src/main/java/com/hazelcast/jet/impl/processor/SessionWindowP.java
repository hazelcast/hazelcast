/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.QuickMath;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.core.function.KeyedWindowResultFunction;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Stream;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.config.ProcessingGuarantee.EXACTLY_ONCE;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.Util.logLateEvent;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Session window processor. See {@link
 *      com.hazelcast.jet.core.processor.Processors#aggregateToSessionWindowP
 * Processors.aggregateToSessionWindowP()} for documentation.
 *
 * @param <K> type of the extracted grouping key
 * @param <A> type of the accumulator object
 * @param <R> type of the finished result
 */
public class SessionWindowP<K, A, R, OUT> extends AbstractProcessor {
    private static final Watermark COMPLETING_WM = new Watermark(Long.MAX_VALUE);

    // exposed for testing, to check for memory leaks
    final Map<K, Windows<A>> keyToWindows = new HashMap<>();
    final SortedMap<Long, Set<K>> deadlineToKeys = new TreeMap<>();
    long currentWatermark = Long.MIN_VALUE;

    private final long sessionTimeout;
    @Nonnull
    private final List<ToLongFunction<Object>> timestampFns;
    @Nonnull
    private final List<Function<Object, K>> keyFns;
    @Nonnull
    private final AggregateOperation<A, ? extends R> aggrOp;
    @Nonnull
    private final BiConsumer<? super A, ? super A> combineFn;
    @Nonnull
    private final KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> mapToOutputFn;
    @Nonnull
    private final FlatMapper<Watermark, Object> closedWindowFlatmapper;
    private ProcessingGuarantee processingGuarantee;

    @Probe(name = "lateEventsDropped")
    private final Counter lateEventsDropped = SwCounter.newSwCounter();
    @Probe(name = "totalKeys")
    private final Counter totalKeys = SwCounter.newSwCounter();
    @Probe(name = "totalWindows")
    private final Counter totalWindows = SwCounter.newSwCounter();

    // Fields for early results emission
    private final long earlyResultsPeriod;
    private long lastTimeEarlyResultsEmitted;
    private Traverser<OUT> earlyWinTraverser;

    private Traverser snapshotTraverser;
    private long minRestoredCurrentWatermark = Long.MAX_VALUE;
    private boolean inComplete;

    // extracted lambdas to reduce GC litter
    private final Function<K, Windows<A>> newWindowsFunction = k -> {
        totalKeys.inc();
        return new Windows<>();
    };

    @SuppressWarnings("unchecked")
    public SessionWindowP(
            long sessionTimeout,
            long earlyResultsPeriod,
            @Nonnull List<? extends ToLongFunction<?>> timestampFns,
            @Nonnull List<? extends Function<?, ? extends K>> keyFns,
            @Nonnull AggregateOperation<A, ? extends R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, ? extends OUT> mapToOutputFn
    ) {
        checkTrue(keyFns.size() == aggrOp.arity(), keyFns.size() + " key functions " +
                "provided for " + aggrOp.arity() + "-arity aggregate operation");
        this.timestampFns = (List<ToLongFunction<Object>>) timestampFns;
        this.keyFns = (List<Function<Object, K>>) keyFns;
        this.earlyResultsPeriod = earlyResultsPeriod;
        this.aggrOp = aggrOp;
        this.combineFn = requireNonNull(aggrOp.combineFn());
        this.mapToOutputFn = mapToOutputFn;
        this.sessionTimeout = sessionTimeout;
        this.closedWindowFlatmapper = flatMapper(this::traverseClosedWindows);
    }

    @Override
    protected void init(@Nonnull Context context) {
        processingGuarantee = context.processingGuarantee();
        lastTimeEarlyResultsEmitted = NANOSECONDS.toMillis(System.nanoTime());
    }

    @Override
    public boolean tryProcess() {
        if (earlyResultsPeriod == 0) {
            return true;
        }
        if (earlyWinTraverser != null) {
            return emitFromTraverser(earlyWinTraverser);
        }
        long now = NANOSECONDS.toMillis(System.nanoTime());
        if (now < lastTimeEarlyResultsEmitted + earlyResultsPeriod) {
            return true;
        }
        lastTimeEarlyResultsEmitted = now;
        earlyWinTraverser = traverseIterable(keyToWindows.entrySet())
                .flatMap(e -> earlyWindows(e.getKey(), e.getValue()))
                .onFirstNull(() -> earlyWinTraverser = null);
        return emitFromTraverser(earlyWinTraverser);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        final long timestamp = timestampFns.get(ordinal).applyAsLong(item);
        if (timestamp < currentWatermark) {
            logLateEvent(getLogger(), currentWatermark, item);
            lateEventsDropped.inc();
            return true;
        }
        K key = keyFns.get(ordinal).apply(item);
        addItem(ordinal,
                keyToWindows.computeIfAbsent(key, newWindowsFunction),
                key, timestamp, item);
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark wm) {
        currentWatermark = wm.timestamp();
        assert totalWindows.get() == deadlineToKeys.values().stream().mapToInt(Set::size).sum()
                : "unexpected totalWindows. Expected=" + deadlineToKeys.values().stream().mapToInt(Set::size).sum()
                + ", actual=" + totalWindows.get();
        return closedWindowFlatmapper.tryProcess(wm);
    }

    @Override
    public boolean complete() {
        inComplete = true;
        return closedWindowFlatmapper.tryProcess(COMPLETING_WM);
    }

    private Traverser<Object> traverseClosedWindows(Watermark wm) {
        SortedMap<Long, Set<K>> windowsToClose = deadlineToKeys.headMap(wm.timestamp());

        Stream<Object> closedWindows = windowsToClose
                .values().stream()
                .flatMap(Set::stream)
                .map(key -> closeWindows(keyToWindows.get(key), key, wm.timestamp()))
                .flatMap(List::stream);
        Traverser<Object> result = traverseStream(closedWindows)
                .onFirstNull(() -> {
                    totalWindows.inc(-windowsToClose.values().stream().mapToInt(Set::size).sum());
                    windowsToClose.clear();
                });
        if (wm != COMPLETING_WM) {
            result = result.append(wm);
        }
        return result;
    }

    private void addToDeadlines(K key, long deadline) {
        if (deadlineToKeys.computeIfAbsent(deadline, x -> new HashSet<>()).add(key)) {
            totalWindows.inc();
        }
    }

    private void removeFromDeadlines(K key, long deadline) {
        Set<K> ks = deadlineToKeys.get(deadline);
        ks.remove(key);
        totalWindows.inc(-1);
        if (ks.isEmpty()) {
            deadlineToKeys.remove(deadline);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean saveToSnapshot() {
        if (inComplete) {
            // If we are in completing phase, we can have a half-emitted item. Instead of finishing it and
            // writing a snapshot, we finish the final items and save no state.
            return complete();
        }
        if (snapshotTraverser == null) {
            snapshotTraverser = Traversers.<Object>traverseIterable(keyToWindows.entrySet())
                    .append(entry(broadcastKey(Keys.CURRENT_WATERMARK), currentWatermark))
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        if (key instanceof BroadcastKey) {
            BroadcastKey bcastKey = (BroadcastKey) key;
            if (!Keys.CURRENT_WATERMARK.equals(bcastKey.key())) {
                throw new JetException("Unexpected broadcast key: " + bcastKey.key());
            }
            long newCurrentWatermark = (long) value;
            assert processingGuarantee != EXACTLY_ONCE
                    || minRestoredCurrentWatermark == Long.MAX_VALUE
                    || minRestoredCurrentWatermark == newCurrentWatermark
                    : "different values for currentWatermark restored, before=" + minRestoredCurrentWatermark
                    + ", new=" + newCurrentWatermark;
            minRestoredCurrentWatermark = Math.min(newCurrentWatermark, minRestoredCurrentWatermark);
            return;
        }

        if (keyToWindows.put((K) key, (Windows) value) != null) {
            throw new JetException("Duplicate key in snapshot: " + key);
        }
    }

    @Override
    public boolean finishSnapshotRestore() {
        assert deadlineToKeys.isEmpty();
        // populate deadlineToKeys
        for (Entry<K, Windows<A>> entry : keyToWindows.entrySet()) {
            for (long end : entry.getValue().ends) {
                addToDeadlines(entry.getKey(), end);
            }
        }
        currentWatermark = minRestoredCurrentWatermark;
        totalKeys.set(keyToWindows.size());
        logFine(getLogger(), "Restored currentWatermark from snapshot to: %s", currentWatermark);
        return true;
    }

    private void addItem(int ordinal, Windows<A> w, K key, long timestamp, Object item) {
        aggrOp.accumulateFn(ordinal).accept(resolveAcc(w, key, timestamp), item);
    }

    private Traverser<OUT> earlyWindows(K key, Windows<A> w) {
        return new Traverser<OUT>() {
            private int i;

            @Override
            public OUT next() {
                while (i < w.size) {
                    OUT out = mapToOutputFn.apply(
                            w.starts[i], w.ends[i], key, aggrOp.exportFn().apply(w.accs[i]), true);
                    i++;
                    if (out != null) {
                        return out;
                    }
                }
                return null;
            }
        };
    }

    private List<OUT> closeWindows(Windows<A> w, K key, long wm) {
        if (w == null) {
            return emptyList();
        }
        List<OUT> results = new ArrayList<>();
        int i = 0;
        for (; i < w.size && w.ends[i] < wm; i++) {
            OUT out = mapToOutputFn.apply(w.starts[i], w.ends[i], key, aggrOp.finishFn().apply(w.accs[i]), false);
            if (out != null) {
                results.add(out);
            }
        }
        if (i != w.size) {
            w.removeHead(i);
        } else {
            keyToWindows.remove(key);
            totalKeys.set(keyToWindows.size());
        }
        return results;
    }

    private A resolveAcc(Windows<A> w, K key, long timestamp) {
        /*
            If two sessions "touch", we merge them. That is when `window1.end ==
            window2.start`. Reason: otherwise we'd depend on the order of input in
            edge case. Mathematically, there's no item that will go to both sessions,
            so they don't overlap.

            Example: timeout=2. If input is: `1, wm(3), 3`, the output will be two
            sessions: the event 1 creates a session (1, 3) which can be emitted at
            wm(3). But if input is: `1, 3, wm(3)`, output would be 1 session (1, 5),
            if we merge "touching" sessions. In neither case item 3 is late. The
            wm(3) can come at any time depending on exact order in which other
            partitions are processed and when the wm is coalesced.
        */
        long eventEnd = timestamp + sessionTimeout;
        int i = 0;
        for (; i < w.size && w.starts[i] < eventEnd; i++) {
            // the window `i` is not after the event interval

            if (w.ends[i] <= timestamp) {
                // the window `i` is before the event interval
                continue;
            }
            if (w.starts[i] <= timestamp && w.ends[i] >= eventEnd) {
                // the window `i` fully covers the event interval
                return w.accs[i];
            }
            // the window `i` overlaps the event interval

            if (i + 1 == w.size || w.starts[i + 1] >= eventEnd) {
                // the window `i + 1` doesn't overlap the event interval
                w.starts[i] = min(w.starts[i], timestamp);
                if (w.ends[i] < eventEnd) {
                    removeFromDeadlines(key, w.ends[i]);
                    w.ends[i] = eventEnd;
                    addToDeadlines(key, w.ends[i]);
                }
                return w.accs[i];
            }
            // both `i` and `i + 1` windows overlap the event interval
            removeFromDeadlines(key, w.ends[i]);
            w.ends[i] = w.ends[i + 1];
            combineFn.accept(w.accs[i], w.accs[i + 1]);
            w.removeWindow(i + 1);
            return w.accs[i];
        }
        addToDeadlines(key, eventEnd);
        return insertWindow(w, i, timestamp, eventEnd);
    }

    private A insertWindow(Windows<A> w, int idx, long windowStart, long windowEnd) {
        w.expandIfNeeded();
        w.copy(idx, idx + 1, w.size - idx);
        w.size++;
        w.starts[idx] = windowStart;
        w.ends[idx] = windowEnd;
        w.accs[idx] = aggrOp.createFn().get();
        return w.accs[idx];
    }

    public static class Windows<A> implements IdentifiedDataSerializable {
        private int size;
        private long[] starts = new long[2];
        private long[] ends = new long[2];
        @SuppressWarnings("unchecked")
        private A[] accs = (A[]) new Object[2];

        private void removeWindow(int idx) {
            size--;
            copy(idx + 1, idx, size - idx);
        }

        private void removeHead(int count) {
            copy(count, 0, size - count);
            size -= count;
        }

        private void copy(int from, int to, int length) {
            arraycopy(starts, from, starts, to, length);
            arraycopy(ends, from, ends, to, length);
            arraycopy(accs, from, accs, to, length);
        }

        private void expandIfNeeded() {
            if (size == starts.length) {
                starts = Arrays.copyOf(starts, 2 * starts.length);
                ends = Arrays.copyOf(ends, 2 * ends.length);
                accs = Arrays.copyOf(accs, 2 * accs.length);
            }
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return JetInitDataSerializerHook.SESSION_WINDOW_P_WINDOWS;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(size);
            for (int i = 0; i < size; i++) {
                out.writeLong(starts[i]);
                out.writeLong(ends[i]);
                out.writeObject(accs[i]);
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        public void readData(ObjectDataInput in) throws IOException {
            size = in.readInt();
            if (size > starts.length) {
                int newSize = QuickMath.nextPowerOfTwo(size);
                starts = new long[newSize];
                ends = new long[newSize];
                accs = (A[]) new Object[newSize];
            }

            for (int i = 0; i < size; i++) {
                starts[i] = in.readLong();
                ends[i] = in.readLong();
                accs[i] = in.readObject();
            }
        }

        @Override
        public String toString() {
            StringJoiner sj = new StringJoiner(", ", getClass().getSimpleName() + '{', "}");
            for (int i = 0; i < size; i++) {
                sj.add("[s=" + toLocalDateTime(starts[i]).toLocalTime()
                        + ", e=" + toLocalDateTime(ends[i]).toLocalTime()
                        + ", a=" + accs[i] + ']');
            }
            return sj.toString();
        }
    }

    // package-visible for test
    enum Keys {
        CURRENT_WATERMARK
    }
}
