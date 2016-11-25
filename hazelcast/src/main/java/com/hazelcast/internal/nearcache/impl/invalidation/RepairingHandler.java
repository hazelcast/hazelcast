/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.internal.nearcache.impl.invalidation.InvalidationUtils.NULL_KEY;
import static java.lang.String.format;

/**
 * Handler used on near-cache side.
 * <p>
 * Decorates an {@link InvalidationHandler} to repair near-cache in the event of invalidation-event-miss
 * or partition UUID changes. Here repairing is done by making relevant near-cache data unreachable. To make
 * stale data unreachable {@link StaleReadWriteDetectorImpl} is used.
 * <p>
 * An instance of this class is created per near-cache and can concurrently be used by many threads.
 *
 * @see StaleReadWriteDetectorImpl
 */
@SuppressWarnings({"checkstyle:nestedifdepth", "checkstyle:npathcomplexity"})
public class RepairingHandler extends InvalidationHandler {

    private final ILogger logger;
    private final String name;
    private final NearCache nearCache;
    private final MinimalPartitionService partitionService;
    private final AtomicLongArray receivedSequences;
    private final AtomicLongArray missedSequenceCounters;
    private final AtomicLongArray staleSequences;
    private final AtomicReferenceArray<UUID> partitionUuids;
    private final String localUuid;

    public RepairingHandler(String name, NearCache nearCache, AtomicReferenceArray<UUID> partitionUUIDs,
                            MinimalPartitionService partitionService, String localUuid, ILogger logger) {
        this.name = name;
        this.nearCache = nearCache;
        this.logger = logger;
        this.partitionService = partitionService;
        this.partitionUuids = partitionUUIDs;
        int partitionCount = partitionService.getPartitionCount();
        this.receivedSequences = new AtomicLongArray(partitionCount);
        this.missedSequenceCounters = new AtomicLongArray(partitionCount);
        this.staleSequences = new AtomicLongArray(partitionCount);
        this.localUuid = localUuid;
    }

    public long getLastReceivedSequence(int partition) {
        return receivedSequences.get(partition);
    }

    public UUID getUuid(int partition) {
        return partitionUuids.get(partition);
    }

    public long getLastStaleSequence(int partition) {
        return staleSequences.get(partition);
    }

    @Override
    public void handle(Data key, String sourceUuid, UUID partitionUuid, long sequence) {
        // Apply invalidation if it is not originated by local member/client. Because local near-caches are invalidated
        // immediately. No need to invalidate them twice.
        if (!localUuid.equals(sourceUuid)) {
            // sourceUuid is allowed to be null.
            if (NULL_KEY.equals(key)) {
                nearCache.clear();
            } else {
                nearCache.remove(key);
            }
        }

        int partitionId = partitionService.getPartitionId(key);
        checkOrRepairUuid(partitionId, partitionUuid);
        checkOrRepairSequence(partitionId, sequence, false);
    }

    public String getName() {
        return name;
    }

    public void addMissCount(int partition, long count) {
        missedSequenceCounters.addAndGet(partition, count);
    }

    public long getMissCount(int partition) {
        return missedSequenceCounters.get(partition);
    }

    public void updateLastKnownStaleSequence(int partition) {
        long lastReceivedSequence;
        long lastKnownStaleSequence;

        do {
            lastReceivedSequence = receivedSequences.get(partition);
            lastKnownStaleSequence = staleSequences.get(partition);

            if (lastKnownStaleSequence >= lastReceivedSequence) {
                break;
            }

        } while (!staleSequences.compareAndSet(partition, lastKnownStaleSequence, lastReceivedSequence));


        if (logger.isFinestEnabled()) {
            logger.finest(format("%s:[map=%s,partition=%d,lowerSequencesStaleThan=%d,lastReceivedSequence=%d]",
                    "Stale sequences updated", name, partition, staleSequences.get(partition), receivedSequences.get(partition)));
        }

    }

    // more than one thread can concurrently call this method: one is anti-entropy, other one is event service thread
    public void checkOrRepairUuid(final int partitionId, final UUID newUuid) {
        assert newUuid != null;

        do {
            UUID prevUuid = partitionUuids.get(partitionId);
            if (prevUuid != null && prevUuid.equals(newUuid)) {
                break;
            }

            if (partitionUuids.compareAndSet(partitionId, prevUuid, newUuid)) {
                receivedSequences.set(partitionId, 0);

                if (logger.isFinestEnabled()) {
                    logger.finest(format("%s:[name=%s,partition=%d,prevUuid=%s,newUuid=%s]",
                            "Invalid uuid, lost remote partition data unexpectedly",
                            name, partitionId, prevUuid, newUuid));
                }

                break;
            }
        } while (true);

    }

    /**
     * Checks {@code nextSequence} against current one. And updates current sequence if next one is bigger.
     */
    // more than one thread can concurrently call this method: one is anti-entropy, other one is event service thread
    public void checkOrRepairSequence(final int partitionId, final long nextSequence, final boolean viaAntiEntropy) {
        assert nextSequence > 0;

        do {
            final long currentSequence = receivedSequences.get(partitionId);
            if (currentSequence >= nextSequence) {
                break;
            }

            if (receivedSequences.compareAndSet(partitionId, currentSequence, nextSequence)) {
                final long sequenceDiff = nextSequence - currentSequence;
                if (viaAntiEntropy || sequenceDiff > 1L) {
                    // we have found at least one missing sequence between current and next sequences. if miss is detected by
                    // anti-entropy, number of missed sequences will be `miss = next - current`, otherwise it means miss is
                    // detected by observing received invalidation event sequence numbers and number of missed sequences will be
                    // `miss = next - current - 1`.
                    final long missCount = viaAntiEntropy ? sequenceDiff : sequenceDiff - 1;
                    final long totalMissCount = missedSequenceCounters.addAndGet(partitionId, missCount);

                    if (logger.isFinestEnabled()) {
                        logger.finest(format("%s:[map=%s,partition=%d,currentSequence=%d,nextSequence=%d,totalMissCount=%d]",
                                "Invalid sequence", name, partitionId, currentSequence, nextSequence, totalMissCount));
                    }

                }

                break;
            }
        } while (true);
    }

    @Override
    public String toString() {
        return "RepairingHandler{"
                + "name='" + name + '\''
                + ", localUuid='" + localUuid + '\''
                + '}';
    }
}
