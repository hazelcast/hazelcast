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
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.Punctuation;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.stream.DistributedCollector;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static java.lang.Math.min;
import static java.lang.System.arraycopy;

/**
 * Session window processor. See {@link
 * WindowingProcessors#sessionWindow(long, DistributedToLongFunction, DistributedFunction,
 * DistributedCollector) sessionWindow(sessionTimeout, extractTimestampF,
 * extractKeyF, collector)} for documentation.
 *
 * @param <T> type of the stream item
 * @param <K> type of the extracted grouping key
 * @param <A> type of the accumulator object
 * @param <R> type of the finished result
 */
public class SessionWindowP<T, K, A, R> extends AbstractProcessor {

    // exposed for testing, to check for memory leaks
    final Map<K, Windows> keyToWindows = new HashMap<>();
    SortedMap<Long, Set<K>> deadlineToKeys = new TreeMap<>();

    private final long sessionTimeout;
    private final DistributedToLongFunction<? super T> extractTimestampF;
    private final DistributedFunction<? super T, K> extractKeyF;
    private final DistributedSupplier<A> newAccumulatorF;
    private final BiConsumer<? super A, ? super T> accumulateF;
    private final DistributedFunction<A, R> finishAccumulationF;
    private final DistributedBinaryOperator<A> combineAccF;
    private final FlatMapper<Punctuation, Session<K, R>> expiredSessionFlatmapper;

    SessionWindowP(
            long sessionTimeout,
            DistributedToLongFunction<? super T> extractTimestampF,
            DistributedFunction<? super T, K> extractKeyF,
            DistributedCollector<? super T, A, R> collector
    ) {
        this.extractTimestampF = extractTimestampF;
        this.extractKeyF = extractKeyF;
        this.newAccumulatorF = collector.supplier();
        this.accumulateF = collector.accumulator();
        this.combineAccF = collector.combiner();
        this.finishAccumulationF = collector.finisher();
        this.sessionTimeout = sessionTimeout;
        this.expiredSessionFlatmapper = flatMapper(this::expiredSessionTraverser);
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        final T event = (T) item;
        final long timestamp = extractTimestampF.applyAsLong(event);
        K key = extractKeyF.apply(event);
        keyToWindows.computeIfAbsent(key, k -> new Windows())
                    .addEvent(key, timestamp, event);
        return true;
    }

    @Override
    protected boolean tryProcessPunc0(@Nonnull Punctuation punc) {
        return expiredSessionFlatmapper.tryProcess(punc);
    }

    private Traverser<Session<K, R>> expiredSessionTraverser(Punctuation punc) {
        Stream<Session<K, R>> sessions = deadlineToKeys
                .headMap(punc.timestamp())
                .values().stream()
                .flatMap(Set::stream)
                .distinct()
                .map(key -> keyToWindows.get(key).closeWindows(key, punc.timestamp()))
                .flatMap(List::stream);
        deadlineToKeys = deadlineToKeys.tailMap(punc.timestamp());
        return traverseStream(sessions);
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

    private class Windows {
        private int size;
        private long[] starts = new long[2];
        private long[] ends = new long[2];
        private A[] accs = (A[]) new Object[2];

        void addEvent(K key, long timestamp, T event) {
            accumulateF.accept(resolveAcc(key, timestamp), event);
        }

        List<Session<K, R>> closeWindows(K key, long punc) {
            List<Session<K, R>> sessions = new ArrayList<>();
            int i = 0;
            for (; i < size && ends[i] < punc; i++) {
                sessions.add(new Session<>(key, starts[i], ends[i], finishAccumulationF.apply(accs[i])));
            }
            if (i != size) {
                removeHead(i);
            } else {
                keyToWindows.remove(key);
            }
            return sessions;
        }

        private A resolveAcc(K key, long timestamp) {
            long eventEnd = timestamp + sessionTimeout;
            int i = 0;
            for (; i < size && starts[i] <= eventEnd; i++) {
                // the window `i` is not after the event interval

                if (ends[i] < timestamp) {
                    // the window `i` is before the event interval
                    continue;
                }
                if (starts[i] <= timestamp && ends[i] >= eventEnd) {
                    // the window `i` fully covers the event interval
                    return accs[i];
                }
                // the window `i` overlaps the event interval

                if (i + 1 == size || starts[i + 1] > eventEnd) {
                    // the window `i + 1` doesn't overlap the event interval
                    starts[i] = min(starts[i], timestamp);
                    if (ends[i] < eventEnd) {
                        removeFromDeadlines(key, ends[i]);
                        ends[i] = eventEnd;
                        addToDeadlines(key, ends[i]);
                    }
                    return accs[i];
                }
                // both `i` and `i + 1` windows overlap the event interval
                removeFromDeadlines(key, ends[i]);
                ends[i] = ends[i + 1];
                accs[i] = combineAccF.apply(accs[i], accs[i + 1]);
                removeWindow(i + 1);
                return accs[i];
            }
            addToDeadlines(key, eventEnd);
            return insertWindow(i, timestamp, eventEnd);
        }

        private A insertWindow(int idx, long windowStart, long windowEnd) {
            expandIfNeeded();
            copy(idx, idx + 1, size - idx);
            size++;
            starts[idx] = windowStart;
            ends[idx] = windowEnd;
            accs[idx] = newAccumulatorF.get();
            return accs[idx];
        }

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
    }
}
