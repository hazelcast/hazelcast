/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.concurrent.ManyToOneConcurrentArrayQueue;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.BroadcastKey;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.impl.util.LoggingUtil;
import com.hazelcast.jet.pipeline.ServiceFactory;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.core.BroadcastKey.broadcastKey;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.impl.processor.ProcessorSupplierWithService.supplierWithService;
import static com.hazelcast.internal.util.ExceptionUtil.withTryCatch;

/**
 * Processor which, for each received item, emits all the items from the
 * traverser returned by the given async item-to-traverser function, using a
 * service.
 * <p>
 * This processors might reorder items: results are emitted as they are
 * asynchronously delivered. However, this processor doesn't reorder items with
 * respect to the watermarks that followed them. That is, a watermark is
 * guaranteed to be emitted <i>after</i> results for all items that occurred
 * before it are emitted.
 *
 * @param <S> service type
 * @param <T> received item type
 * @param <K> extracted key type
 * @param <R> emitted item type
 */
public final class AsyncTransformUsingServiceUnorderedP<C, S, T, K, R> extends AbstractAsyncTransformUsingServiceP<C, S> {

    /*
    HOW IT WORKS

    When an event is received, we submit the async op, and when response is received, we add it to resultQueue.
    In the resultQueue we also store the last received WM value for each key.

    Separately, we track watermark counts for each key, which we increment for each WM key when an event is received,
    and decrement when a response is processed. The count is the count of events received _since_ that WM, _before_
    the next WM. When the count gets to 0, we know we can emit the next watermark, because all the responses
    for events received before it were already sent.

    Snapshot contains in-flight elements at the time of taking the snapshot.
    They are replayed when state is restored from the snapshot.
     */

    private final BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<Traverser<R>>> callAsyncFn;
    private final Function<? super T, ? extends K> extractKeyFn;

    private ManyToOneConcurrentArrayQueue<Tuple3<T, long[], Object>> resultQueue;
    /**
     * Each watermark count map contains:
     * <ul>
     *     <li>key: watermark timestamp or {@link Long#MIN_VALUE} for items before first watermark</li>
     *     <li>value: number of items received _after_ this WM and _before_ next WM (if any)
     *     that are still being processed.</li>
     * </ul>
     */
    // TODO we can use more efficient structure: we only remove from the beginning and add to the end
    @SuppressWarnings("unchecked")
    private SortedMap<Long, Integer>[] watermarkCounts = new SortedMap[0];
    /**
     * Current in-flight items.
     * <p>
     * Invariants:
     * <ol>
     *     <li>for each key, value > 0. Finished items are immediately removed
     *     <li>sum of all values in {@link #inFlightItems} is equal to
     *     {@link #asyncOpsCounter}, and to the sum of values in every map in the
     *     {@link #watermarkCounts} array.
     * </ol>
     * <p>
     * This is {@link IdentityHashMap} but after restoring from snapshot objects
     * that used single shared instance (e.g. {@link String})
     * may no longer be the same shared instance.
     */
    private final Map<T, Integer> inFlightItems = new IdentityHashMap<>();
    private Traverser<Object> currentTraverser = Traversers.empty();
    @SuppressWarnings("rawtypes")
    private Traverser<Entry> snapshotTraverser;

    /**
     * Array of received WM keys. It maps wmIndex to wmKey.
     */
    private byte[] wmKeys = {};
    /**
     * The inverse of {@link #wmKeys}, it maps wmKey to wmIndex.
     * It's used when we receive a WM to find at which index we store data for that
     * WM key.
     */
    private byte[] wmKeysInv = {};
    /**
     * Last received watermark for given key index (wmIndex).
     * Copy-on-write.
     */
    private long[] lastReceivedWms = {};
    private long[] lastEmittedWms = {};
    private long[] minRestoredWms = {};
    /**
     * Number of submitted asynchronous operations that have not yet finished.
     * <p>
     * Invariants:
     * <ol>
     *     <li>asyncOpsCounter >= 0</li>
     *     <li>asyncOpsCounter <= maxConcurrentOps</li>
     *     <li>see invariant in {@link #inFlightItems}</li>
     * </ol>
     */
    private int asyncOpsCounter;

    /** Temporary collection for restored objects during snapshot restore. */
    private ArrayDeque<T> restoredObjects = new ArrayDeque<>();

    @Probe(name = "numInFlightOps")
    private final Counter asyncOpsCounterMetric = SwCounter.newSwCounter();

    /**
     * Constructs a processor with the given mapping function.
     */
    private AsyncTransformUsingServiceUnorderedP(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            @Nullable C serviceContext,
            int maxConcurrentOps,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<Traverser<R>>> callAsyncFn,
            @Nonnull Function<? super T, ? extends K> extractKeyFn
    ) {
        super(serviceFactory, serviceContext, maxConcurrentOps, false);
        this.callAsyncFn = callAsyncFn;
        this.extractKeyFn = extractKeyFn;
    }

    @Override
    protected void init(@Nonnull Processor.Context context) throws Exception {
        super.init(context);
        resultQueue = new ManyToOneConcurrentArrayQueue<>(maxConcurrentOps);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        if (getOutbox().hasUnfinishedItem() && !emitFromTraverser(currentTraverser)) {
            return false;
        }
        asyncOpsCounterMetric.set(asyncOpsCounter);
        @SuppressWarnings("unchecked")
        T castItem = (T) item;
        if (!processItem(castItem)) {
            // if queue is full, try to emit and apply backpressure
            tryFlushQueue();
            return false;
        }
        return true;
    }

    @CheckReturnValue
    private boolean processItem(@Nonnull T item) {
        if (asyncOpsCounter == maxConcurrentOps) {
            return false;
        }
        CompletableFuture<Traverser<R>> future = callAsyncFn.apply(service, item);
        if (future == null) {
            return true;
        }
        asyncOpsCounter++;
        for (int i = 0; i < wmKeys.length; i++) {
            watermarkCounts[i].merge(lastReceivedWms[i], 1, Integer::sum);
        }

        long[] lastWatermarksAtReceiveTime = lastReceivedWms;
        future.whenComplete(withTryCatch(getLogger(),
                (r, e) -> resultQueue.add(tuple3(item, lastWatermarksAtReceiveTime, r != null ? r : e))));
        inFlightItems.merge(item, 1, Integer::sum);
        return true;
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        if (!emitFromTraverser(currentTraverser)) {
            return false;
        }
        int wmIndex = getWmIndex(watermark.key());

        assert lastEmittedWms[wmIndex] <= lastReceivedWms[wmIndex]
                : "lastEmittedWm=" + lastEmittedWms[wmIndex] + ", lastReceivedWm=" + lastReceivedWms[wmIndex];
        // Ignore a watermark that is going back. This is possible after restoring from a snapshot
        // taken in at-least-once mode.
        if (watermark.timestamp() <= lastReceivedWms[wmIndex]) {
            return true;
        }
        if (allEmpty(watermarkCounts)) {
            // Emit watermark eagerly if there are no pending items to wait for.
            if (!tryEmit(watermark)) {
                return false;
            }
            lastEmittedWms[wmIndex] = watermark.timestamp();
        }
        // We must not mutate lastReceivedWms, because we share the instance in the inFlightItems - we would
        // mutate the instance they have. Instead, we copy and mutate it.
        lastReceivedWms = lastReceivedWms.clone();
        lastReceivedWms[wmIndex] = watermark.timestamp();
        return true;
    }

    /**
     * Get the index for the given WM key. Extends the arrays, if needed.
     */
    @SuppressWarnings("checkstyle:MagicNumber")
    private int getWmIndex(byte wmKey0) {
        int wmKey = wmKey0 & 0xff; // convert the key to range 0..255
        if (wmKeysInv.length <= wmKey) {
            int oldLength = wmKeysInv.length;
            int newLength = wmKey + 1;
            wmKeysInv = Arrays.copyOf(wmKeysInv, newLength);
            for (int i = oldLength; i < newLength; i++) {
                wmKeysInv[i] = -1; // -1 marks unused wm key
            }
        }
        int wmIndex = wmKeysInv[wmKey];

        // if the wm key is seen for the 1st time, extend the arrays
        if (wmIndex < 0) {
            wmIndex = wmKeys.length;
            wmKeysInv[wmIndex] = wmKey0;
            int newLength = wmKeys.length + 1;

            wmKeys = Arrays.copyOf(wmKeys, newLength);
            wmKeys[wmIndex] = (byte) wmKey; // thanks to the cast, the key will be again in range -128 .. 127

            lastReceivedWms = Arrays.copyOf(lastReceivedWms, newLength);
            lastReceivedWms[wmIndex] = Long.MIN_VALUE;

            lastEmittedWms = Arrays.copyOf(lastEmittedWms, newLength);
            lastEmittedWms[wmIndex] = Long.MIN_VALUE;

            watermarkCounts = Arrays.copyOf(watermarkCounts, newLength);
            watermarkCounts[wmIndex] = new TreeMap<>();
            if (asyncOpsCounter > 0) {
                // This is the first time we have seen this watermark key.
                // Current in-flight items were received before any watermark for this key.
                // Note that the same item can be processed multiple times
                // if it appeared many times in the inbox.
                watermarkCounts[wmIndex].put(Long.MIN_VALUE, asyncOpsCounter);
            }

            minRestoredWms = Arrays.copyOf(minRestoredWms, newLength);
            minRestoredWms[wmIndex] = Long.MAX_VALUE;
        }
        return wmIndex;
    }

    @Override
    public boolean tryProcess() {
        tryFlushQueue();
        asyncOpsCounterMetric.set(asyncOpsCounter);
        return true;
    }

    @Override
    public boolean complete() {
        return tryFlushQueue();
    }

    @Override
    public boolean saveToSnapshot() {
        assert restoredObjects.isEmpty() : "restoredObjects not empty";
        if (!emitFromTraverser(currentTraverser)) {
            return false;
        }
        if (snapshotTraverser == null) {
            Map<Byte, Long> lastReceivedWmsMap = new HashMap<>();
            for (int i = 0; i < lastReceivedWms.length; i++) {
                lastReceivedWmsMap.put(wmKeys[i], lastReceivedWms[i]);
            }
            LoggingUtil.logFinest(getLogger(), "Saving to snapshot: %s, lastReceivedWm=%s",
                    inFlightItems, lastReceivedWmsMap);
            snapshotTraverser = traverseIterable(inFlightItems.entrySet())
                    .<Entry>map(en -> entry(
                            extractKeyFn.apply(en.getKey()),
                            tuple2(en.getKey(), en.getValue())))
                    .append(entry(broadcastKey(Keys.LAST_RECEIVED_WMS), lastReceivedWmsMap))
                    .onFirstNull(() -> snapshotTraverser = null);
        }
        return emitFromTraverserToSnapshot(snapshotTraverser);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void restoreFromSnapshot(@Nonnull Object key, @Nonnull Object value) {
        if (key instanceof BroadcastKey) {
            assert ((BroadcastKey) key).key().equals(Keys.LAST_RECEIVED_WMS) : "Unexpected key: " + key;
            // we restart at the oldest WM any instance was at the time of snapshot
            for (Entry<Byte, Long> en : ((Map<Byte, Long>) value).entrySet()) {
                int wmIndex = getWmIndex(en.getKey());
                minRestoredWms[wmIndex] = Math.min(minRestoredWms[wmIndex], en.getValue());
            }

            return;
        }
        Tuple2<T, Integer> value1 = (Tuple2<T, Integer>) value;
        // we can't apply backpressure here, we have to store the items and execute them later
        assert value1.f0() != null && value1.f1() != null;
        // replay each item appropriate number of times, order does not matter
        for (int i = 0; i < value1.f1(); i++) {
            restoredObjects.add(value1.f0());
            LoggingUtil.logFinest(getLogger(), "Restored: %s", value1.f0());
        }
    }

    @Override
    public boolean finishSnapshotRestore() {
        for (T t; (t = restoredObjects.peek()) != null && processItem(t); ) {
            restoredObjects.remove();
        }
        if (restoredObjects.isEmpty()) {
            // finish current object, we can't return true with a half-emitted item
            if (!emitFromTraverser(currentTraverser)) {
                return false;
            }
            restoredObjects = new ArrayDeque<>(0); // minimize the internal storage
            lastReceivedWms = minRestoredWms;
            return true;
        } else {
            tryFlushQueue();
            return false;
        }
    }

    /**
     * Drains items from the queue until either:
     * <ul><li>
     *     encountering an incomplete item
     * </li><li>
     *     the outbox gets full
     * </li></ul>
     *
     * @return true if there are no more in-flight items and everything was emitted
     *         to the outbox
     */
    @SuppressWarnings("unchecked")
    private boolean tryFlushQueue() {
        for (;;) {
            if (!emitFromTraverser(currentTraverser)) {
                return false;
            }
            Tuple3<T, long[], Object> tuple = resultQueue.poll();
            if (tuple == null) {
                // done if there are no ready and no in-flight items
                return asyncOpsCounter == 0;
            }
            assert asyncOpsCounter > 0;
            asyncOpsCounter--;
            Integer inFlightItemsCount = inFlightItems.compute(tuple.f0(), (k, v) -> v == 1 ? null : v - 1);
            assert inFlightItemsCount == null || inFlightItemsCount > 0 : "inFlightItemsCount=" + inFlightItemsCount;
            // the result is either Throwable or Traverser<Object>
            if (tuple.f2() instanceof Throwable) {
                throw new JetException("Async operation completed exceptionally: " + tuple.f2(),
                        (Throwable) tuple.f2());
            }
            currentTraverser = (Traverser<Object>) tuple.f2();
            if (currentTraverser == null) {
                currentTraverser = Traversers.empty();
            }

            for (int i = 0; i < watermarkCounts.length; i++) {
                SortedMap<Long, Integer> watermarkCount = watermarkCounts[i];
                assert tuple.f1() != null;
                long wmAtReceiveTime = tuple.f1().length > i ? tuple.f1()[i] : Long.MIN_VALUE;
                int count = watermarkCount.merge(wmAtReceiveTime, -1, Integer::sum);
                assert count >= 0 : "count=" + count;

                if (count > 0) {
                    continue;
                }
                long wmToEmit = Long.MIN_VALUE;
                // The first watermark with non-zero counter is ready to be emitted:
                // - all items before it have completed (and can be removed from watermarkCount map)
                // - there are in-flight items received after it, so the next watermark is not ready
                for (Iterator<Entry<Long, Integer>> it = watermarkCount.entrySet().iterator(); it.hasNext(); ) {
                    Entry<Long, Integer> entry = it.next();
                    if (entry.getValue() != 0) {
                        wmToEmit = entry.getKey();
                        break;
                    } else {
                        it.remove();
                    }
                }
                if (watermarkCount.isEmpty() && lastReceivedWms[i] > lastEmittedWms[i]) {
                    wmToEmit = lastReceivedWms[i];
                }
                if (wmToEmit > Long.MIN_VALUE && wmToEmit > lastEmittedWms[i]) {
                    lastEmittedWms[i] = wmToEmit;
                    currentTraverser = currentTraverser.append(new Watermark(wmToEmit, wmKeys[i]));
                }
            }
        }
    }

    /**
     * The {@link ResettableSingletonTraverser} is passed as a first argument to
     * {@code callAsyncFn}, it can be used if needed.
     */
    public static <C, S, T, K, R> ProcessorSupplier supplier(
            @Nonnull ServiceFactory<C, S> serviceFactory,
            int maxConcurrentOps,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends CompletableFuture<Traverser<R>>> callAsyncFn,
            @Nonnull FunctionEx<? super T, ? extends K> extractKeyFn
    ) {
        return supplierWithService(serviceFactory, (serviceFn, context) ->
                new AsyncTransformUsingServiceUnorderedP<>(
                        serviceFn, context, maxConcurrentOps, callAsyncFn, extractKeyFn));
    }

    private enum Keys {
        LAST_RECEIVED_WMS
    }

    /**
     * Returns `true` if all maps in the given array are empty.
     */
    private static <T extends Map<?, ?>> boolean allEmpty(T[] collections) {
        for (Map<?, ?> c : collections) {
            if (!c.isEmpty()) {
                return false;
            }
        }
        return true;
    }
}
