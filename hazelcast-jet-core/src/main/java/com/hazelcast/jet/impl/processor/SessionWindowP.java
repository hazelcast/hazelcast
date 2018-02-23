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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.function.KeyedWindowResultFunction;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.QuickMath;

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

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.impl.util.Util.toLocalDateTime;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

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

    private final long sessionTimeout;
    @Nonnull
    private final List<ToLongFunction<Object>> timestampFns;
    @Nonnull
    private final List<Function<Object, K>> keyFns;
    @Nonnull
    private final AggregateOperation<A, R> aggrOp;
    @Nonnull
    private final BiConsumer<? super A, ? super A> combineFn;
    @Nonnull
    private final KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn;
    @Nonnull
    private final FlatMapper<Watermark, OUT> closedWindowFlatmapper;

    private Traverser snapshotTraverser;

    @SuppressWarnings("unchecked")
    public SessionWindowP(
            long sessionTimeout,
            @Nonnull List<? extends ToLongFunction<?>> timestampFns,
            @Nonnull List<? extends Function<?, ? extends K>> keyFns,
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        checkTrue(keyFns.size() == aggrOp.arity(), keyFns.size() + " key functions " +
                "provided for " + aggrOp.arity() + "-arity aggregate operation");
        this.timestampFns = (List<ToLongFunction<Object>>) timestampFns;
        this.keyFns = (List<Function<Object, K>>) keyFns;
        this.aggrOp = aggrOp;
        this.combineFn = requireNonNull(aggrOp.combineFn());
        this.mapToOutputFn = mapToOutputFn;
        this.sessionTimeout = sessionTimeout;
        this.closedWindowFlatmapper = flatMapper(this::traverseClosedWindows);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        @SuppressWarnings("unchecked")
        final long timestamp = timestampFns.get(ordinal).applyAsLong(item);
        K key = keyFns.get(ordinal).apply(item);
        addItem(ordinal, keyToWindows.computeIfAbsent(key, k -> new Windows<>()),
                key, timestamp, item);
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark wm) {
        return closedWindowFlatmapper.tryProcess(wm);
    }

    @Override
    public boolean complete() {
        return closedWindowFlatmapper.tryProcess(COMPLETING_WM);
    }

    private Traverser<OUT> traverseClosedWindows(Watermark wm) {
        SortedMap<Long, Set<K>> windowsToClose = deadlineToKeys.headMap(wm.timestamp());
        List<K> distinctKeys = windowsToClose
                .values().stream()
                .flatMap(Set::stream)
                .distinct()
                .collect(toList());
        windowsToClose.clear();

        Stream<OUT> closedWindows = distinctKeys
                .stream()
                .map(key -> closeWindows(keyToWindows.get(key), key, wm.timestamp()))
                .flatMap(List::stream);
        return traverseStream(closedWindows);
    }

    private void addToDeadlines(K key, long deadline) {
        deadlineToKeys.computeIfAbsent(deadline, x -> new HashSet<>()).add(key);
    }

    private void removeFromDeadlines(K key, long deadline) {
        Set<K> ks = deadlineToKeys.get(deadline);
        ks.remove(key);
        if (ks.isEmpty()) {
            deadlineToKeys.remove(deadline);
        }
    }

    @Override
    public boolean saveToSnapshot() {
        if (snapshotTraverser == null) {
            snapshotTraverser = traverseIterable(keyToWindows.entrySet())
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        keyToWindows.put((K) key, (Windows) value);
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
        return true;
    }

    private void addItem(int ordinal, Windows<A> w, K key, long timestamp, Object item) {
        aggrOp.accumulateFn(ordinal).accept(resolveAcc(w, key, timestamp), item);
    }

    private List<OUT> closeWindows(Windows<A> w, K key, long wm) {
        List<OUT> results = new ArrayList<>();
        int i = 0;
        for (; i < w.size && w.ends[i] < wm; i++) {
            results.add(mapToOutputFn.apply(w.starts[i], w.ends[i], key, aggrOp.finishFn().apply(w.accs[i])));
        }
        if (i != w.size) {
            w.removeHead(i);
        } else {
            keyToWindows.remove(key);
        }
        return results;
    }

    private A resolveAcc(Windows<A> w, K key, long timestamp) {
        long eventEnd = timestamp + sessionTimeout;
        int i = 0;
        for (; i < w.size && w.starts[i] <= eventEnd; i++) {
            // the window `i` is not after the event interval

            if (w.ends[i] < timestamp) {
                // the window `i` is before the event interval
                continue;
            }
            if (w.starts[i] <= timestamp && w.ends[i] >= eventEnd) {
                // the window `i` fully covers the event interval
                return w.accs[i];
            }
            // the window `i` overlaps the event interval

            if (i + 1 == w.size || w.starts[i + 1] > eventEnd) {
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
        public int getId() {
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
}
