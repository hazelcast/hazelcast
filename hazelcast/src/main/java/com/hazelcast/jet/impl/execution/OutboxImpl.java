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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.jet.impl.util.Util;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.IntStream;

import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.Util.lazyIncrement;

public class OutboxImpl implements OutboxInternal {

    private final OutboundCollector[] outstreams;
    private final ProgressTracker progTracker;
    private final SerializationService serializationService;
    private final int batchSize;
    private final AtomicLongArray counters;

    private final int[] singleEdge = {0};
    private final int[] allEdges;
    private final int[] allEdgesAndSnapshot;
    private final int[] snapshotEdge;
    private final BitSet broadcastTracker;
    private Entry<Data, Data> pendingSnapshotEntry;
    private int numRemainingInBatch;

    private Object unfinishedItem;
    private int[] unfinishedItemOrdinals;
    private Object unfinishedSnapshotKey;
    private Object unfinishedSnapshotValue;
    private final Counter lastForwardedWm = SwCounter.newSwCounter(Long.MIN_VALUE);

    private boolean blocked;

    /**
     * @param outstreams The output queues
     * @param hasSnapshot If the last queue in {@code outstreams} is the snapshot queue
     * @param progTracker Tracker to track progress. Only madeProgress will be called,
     *                    done status won't be ever changed
     * @param batchSize Maximum number of items that will be allowed to offer until
     *                  {@link #reset()} is called.
     */
    public OutboxImpl(OutboundCollector[] outstreams, boolean hasSnapshot, ProgressTracker progTracker,
                      SerializationService serializationService, int batchSize, AtomicLongArray counters) {
        this.outstreams = outstreams;
        this.progTracker = progTracker;
        this.serializationService = serializationService;
        this.batchSize = batchSize;
        this.counters = counters;
        checkPositive(batchSize, "batchSize must be positive");

        allEdges = IntStream.range(0, outstreams.length - (hasSnapshot ? 1 : 0)).toArray();
        allEdgesAndSnapshot = IntStream.range(0, outstreams.length).toArray();
        snapshotEdge = hasSnapshot ? new int[] {outstreams.length - 1} : null;
        broadcastTracker = new BitSet(outstreams.length);
    }

    @Override
    public final int bucketCount() {
        return allEdges.length;
    }

    @Override
    public final boolean offer(int ordinal, @Nonnull Object item) {
        if (ordinal == -1) {
            return offerInternal(allEdges, item);
        } else {
            if (ordinal == bucketCount()) {
                // ordinal beyond bucketCount will add to snapshot queue, which we don't allow through this method
                throw new IllegalArgumentException("Illegal edge ordinal: " + ordinal);
            }
            singleEdge[0] = ordinal;
            return offerInternal(singleEdge, item);
        }
    }

    @Override
    public final boolean offer(@Nonnull int[] ordinals, @Nonnull Object item) {
        assert snapshotEdge == null || Util.arrayIndexOf(snapshotEdge[0], ordinals) < 0
                : "Ordinal " + snapshotEdge[0] + " is out of range";
        return offerInternal(ordinals, item);
    }

    private boolean offerInternal(@Nonnull int[] ordinals, @Nonnull Object item) {
        if (shouldBlock()) {
            return false;
        }
        assert unfinishedItem == null || item.equals(unfinishedItem)
                : "Different item offered after previous call returned false: expected=" + unfinishedItem
                        + ", got=" + item;
        assert unfinishedItemOrdinals == null || Arrays.equals(unfinishedItemOrdinals, ordinals)
                : "Offered to different ordinals after previous call returned false: expected="
                + Arrays.toString(unfinishedItemOrdinals) + ", got=" + Arrays.toString(ordinals);

        assert numRemainingInBatch != -1 : "Outbox.offer() called again after it returned false, without a " +
                "call to reset(). You probably didn't return from Processor method after Outbox.offer() " +
                "or AbstractProcessor.tryEmit() returned false";
        numRemainingInBatch--;
        boolean done = true;
        if (numRemainingInBatch == -1) {
            done = false;
        } else {
            if (ordinals.length == 0) {
                // edge case - emitting to outbox with 0 ordinals is a progress
                progTracker.madeProgress();
            }
            for (int i = 0; i < ordinals.length; i++) {
                if (broadcastTracker.get(i)) {
                    continue;
                }
                ProgressState result = doOffer(outstreams[ordinals[i]], item);
                if (result.isMadeProgress()) {
                    progTracker.madeProgress();
                }
                if (result.isDone()) {
                    broadcastTracker.set(i);
                    if (!(item instanceof BroadcastItem)) {
                        // we are the only updating thread, no need for CAS operations
                        lazyIncrement(counters, ordinals[i]);
                    }
                } else {
                    done = false;
                }
            }
        }
        if (done) {
            broadcastTracker.clear();
            unfinishedItem = null;
            unfinishedItemOrdinals = null;
            if (item instanceof Watermark) {
                long wmTimestamp = ((Watermark) item).timestamp();
                if (wmTimestamp != WatermarkCoalescer.IDLE_MESSAGE.timestamp()) {
                    // We allow equal timestamp here, even though the WMs should be increasing.
                    // But we don't track WMs per ordinal and the same WM can be offered to different
                    // ordinals in different calls. Theoretically a completely different WM could be
                    // emitted to each ordinal, but we don't do that currently.
                    assert lastForwardedWm.get() <= wmTimestamp
                            : "current=" + lastForwardedWm.get() + ", new=" + wmTimestamp;
                    lastForwardedWm.set(wmTimestamp);
                }
            }
        } else {
            numRemainingInBatch = -1;
            unfinishedItem = item;
            // Defensively copy the array as it can be mutated.
            // We intentionally only do it when assertions are enabled to reduce the overhead.
            //noinspection ConstantConditions,AssertWithSideEffects
            assert (unfinishedItemOrdinals = Arrays.copyOf(ordinals, ordinals.length)) != null;
        }
        return done;
    }

    @Override
    public final boolean offer(@Nonnull Object item) {
        return offerInternal(allEdges, item);
    }

    @Override
    public final boolean offerToSnapshot(@Nonnull Object key, @Nonnull Object value) {
        if (snapshotEdge == null) {
            throw new IllegalStateException("Outbox does not have snapshot queue");
        }
        if (shouldBlock()) {
            return false;
        }

        assert unfinishedSnapshotKey == null || unfinishedSnapshotKey.equals(key)
                : "Different key offered after previous call returned false: expected="
                + unfinishedSnapshotKey + ", got=" + key;
        assert unfinishedSnapshotValue == null || unfinishedSnapshotValue.equals(value)
                : "Different value offered after previous call returned false: expected="
                + unfinishedSnapshotValue + ", got=" + value;

        // pendingSnapshotEntry is used to avoid duplicate serialization when the queue rejects the entry
        if (pendingSnapshotEntry == null) {
            // We serialize the key and value immediately to effectively clone them,
            // so the caller can modify them right after they are accepted by this method.
            Data sKey = serializationService.toData(key);
            Data sValue = serializationService.toData(value);
            pendingSnapshotEntry = entry(sKey, sValue);
        }

        boolean success = offerInternal(snapshotEdge, pendingSnapshotEntry);
        if (success) {
            pendingSnapshotEntry = null;
            unfinishedSnapshotKey = null;
            unfinishedSnapshotValue = null;
        } else {
            unfinishedSnapshotKey = key;
            unfinishedSnapshotValue = value;
        }
        return success;
    }

    @Override
    public boolean hasUnfinishedItem() {
        return unfinishedItem != null || unfinishedSnapshotKey != null;
    }

    @Override
    public void block() {
        blocked = true;
    }

    @Override
    public void unblock() {
        blocked = false;
    }

    private boolean shouldBlock() {
        return blocked && !hasUnfinishedItem();
    }

    @Override
    public void reset() {
        numRemainingInBatch = batchSize;
    }

    private ProgressState doOffer(OutboundCollector collector, Object item) {
        if (item instanceof BroadcastItem) {
            return collector.offerBroadcast((BroadcastItem) item);
        }
        return collector.offer(item);
    }

    final boolean offerToEdgesAndSnapshot(Object item) {
        return offerInternal(allEdgesAndSnapshot, item);
    }

    @Override
    public long lastForwardedWm() {
        return lastForwardedWm.get();
    }
}
