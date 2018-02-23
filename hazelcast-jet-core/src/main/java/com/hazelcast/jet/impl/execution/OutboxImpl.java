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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.impl.util.ProgressState;
import com.hazelcast.jet.impl.util.ProgressTracker;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import javax.annotation.Nonnull;
import java.util.BitSet;
import java.util.Map.Entry;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.util.Preconditions.checkPositive;

public class OutboxImpl implements Outbox {

    private final OutboundCollector[] outstreams;
    private final ProgressTracker progTracker;
    private final SerializationService serializationService;
    private final int batchSize;

    private final int[] singleEdge = {0};
    private final int[] allEdges;
    private final int[] allEdgesAndSnapshot;
    private final int[] snapshotEdge;
    private final BitSet broadcastTracker;
    private Entry<Data, Data> pendingSnapshotEntry;
    private int numRemainingInBatch;

    /**
     * @param outstreams The output queues
     * @param hasSnapshot If the last queue in {@code outstreams} is the snapshot queue
     * @param progTracker Tracker to track progress. Only madeProgress will be called,
     *                    done status won't be ever changed
     * @param batchSize Maximum number of items that will be allowed to offer until
     *                  {@link #reset()} is called.
     */
    public OutboxImpl(OutboundCollector[] outstreams, boolean hasSnapshot, ProgressTracker progTracker,
                      SerializationService serializationService, int batchSize) {
        this.outstreams = outstreams;
        this.progTracker = progTracker;
        this.serializationService = serializationService;
        this.batchSize = batchSize;
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
            return offer(allEdges, item);
        } else {
            if (ordinal == bucketCount()) {
                // ordinal beyond bucketCount will add to snapshot queue, which we don't allow through this method
                throw new IllegalArgumentException("Illegal edge ordinal: " + ordinal);
            }
            singleEdge[0] = ordinal;
            return offer(singleEdge, item);
        }
    }

    @Override
    public final boolean offer(int[] ordinals, @Nonnull Object item) {
        assert numRemainingInBatch != -1 : "Outbox.offer() called again after it returned false, without a " +
                "call to reset(). You probably didn't return from Processor method after Outbox.offer() " +
                "or AbstractProcessor.tryEmit() returned false";
        numRemainingInBatch--;
        if (numRemainingInBatch == -1) {
            return false;
        }
        boolean done = true;
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
            } else {
                done = false;
            }
        }
        if (done) {
            broadcastTracker.clear();
        } else {
            numRemainingInBatch = -1;
        }
        return done;
    }

    @Override
    public final boolean offer(@Nonnull Object item) {
        return offer(allEdges, item);
    }

    @Override
    public final boolean offerToSnapshot(@Nonnull Object key, @Nonnull Object value) {
        if (snapshotEdge == null) {
            throw new IllegalStateException("Outbox does not have snapshot queue");
        }

        // pendingSnapshotEntry is used to avoid duplicate serialization when the queue rejects the entry
        if (pendingSnapshotEntry == null) {
            // We serialize the key and value immediately to effectively clone them,
            // so the caller can modify them right after they are accepted by this method.
            Data sKey = serializationService.toData(key);
            Data sValue = serializationService.toData(value);
            pendingSnapshotEntry = entry(sKey, sValue);
        }

        boolean success = offer(snapshotEdge, pendingSnapshotEntry);
        if (success) {
            pendingSnapshotEntry = null;
        }
        return success;
    }

    /**
     * Resets the outbox so that it is available to receive another batch of
     * items after any {@code offer()} method previously returned {@code
     * false}.
     */
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
        return offer(allEdgesAndSnapshot, item);
    }
}
