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

import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static java.lang.String.format;

/**
 * Handler used on near-cache side. Observes local and remote invalidations and registers relevant
 * data to {@link MetaDataContainer}s
 *
 * Used to repair near-cache in the event of invalidation-event-miss
 * or partition uuid changes. Here repairing is done by making relevant near-cache data unreachable. To make
 * stale data unreachable {@link StaleReadDetectorImpl} is used.
 *
 * An instance of this class is created per near-cache and can concurrently be used by many threads.
 *
 * @see StaleReadDetectorImpl
 */
@SuppressWarnings({"checkstyle:nestedifdepth", "checkstyle:npathcomplexity"})
public final class RepairingHandler {

    private final int partitionCount;
    private final String name;
    private final String localUuid;
    private final NearCache nearCache;
    private final ILogger logger;
    private final MinimalPartitionService partitionService;
    private final MetaDataContainer[] metaDataContainers;

    public RepairingHandler(String name, NearCache nearCache, MinimalPartitionService partitionService,
                            String localUuid, ILogger logger) {
        this.name = name;
        this.localUuid = localUuid;
        this.nearCache = nearCache;
        this.logger = logger;
        this.partitionService = partitionService;
        this.partitionCount = partitionService.getPartitionCount();
        this.metaDataContainers = createMetadataContainers(partitionCount);
    }

    private static MetaDataContainer[] createMetadataContainers(int partitionCount) {
        MetaDataContainer[] metaData = new MetaDataContainer[partitionCount];
        for (int partition = 0; partition < partitionCount; partition++) {
            metaData[partition] = new MetaDataContainer();
        }
        return metaData;
    }

    public void initUnknownUuids(AtomicReferenceArray<UUID> partitionUuids) {
        for (int partition = 0; partition < partitionCount; partition++) {
            metaDataContainers[partition].casUuid(null, partitionUuids.get(partition));
        }
    }

    public MetaDataContainer getMetaDataContainer(int partition) {
        return metaDataContainers[partition];
    }

    /**
     * Handles a single invalidation
     */
    public void handle(Data key, String sourceUuid, UUID partitionUuid, long sequence) {
        // Apply invalidation if it is not originated by local member/client. Because local near-caches are invalidated
        // immediately. No need to invalidate them twice.
        if (!localUuid.equals(sourceUuid)) {
            // sourceUuid is allowed to be null.
            if (key == null) {
                nearCache.clear();
            } else {
                nearCache.remove(key);
            }
        }

        int partitionId = getPartitionIdOrDefault(key);
        checkOrRepairUuid(partitionId, partitionUuid);
        checkOrRepairSequence(partitionId, sequence, false);
    }

    private int getPartitionIdOrDefault(Data key) {
        if (key == null) {
            // `name` is used to determine partition-id of map-wide events like clear.
            // since key is null, we are using `name` to find partition-id
            return partitionService.getPartitionId(name);
        }
        return partitionService.getPartitionId(key);
    }

    /**
     * Handles batch invalidations
     */
    public void handle(Collection<Data> keys, Collection<String> sourceUuids,
                       Collection<UUID> partitionUuids, Collection<Long> sequences) {
        Iterator<Data> keyIterator = keys.iterator();
        Iterator<Long> sequenceIterator = sequences.iterator();
        Iterator<UUID> partitionUuidIterator = partitionUuids.iterator();
        Iterator<String> sourceUuidsIterator = sourceUuids.iterator();

        do {
            if (!(keyIterator.hasNext() && sequenceIterator.hasNext()
                    && partitionUuidIterator.hasNext() && sourceUuidsIterator.hasNext())) {
                break;
            }

            handle(keyIterator.next(), sourceUuidsIterator.next(), partitionUuidIterator.next(), sequenceIterator.next());

        } while (true);
    }

    public String getName() {
        return name;
    }

    // TODO really need to pass partition-id?
    public void updateLastKnownStaleSequence(MetaDataContainer metaData, int partition) {
        long lastReceivedSequence;
        long lastKnownStaleSequence;

        do {
            lastReceivedSequence = metaData.getSequence();
            lastKnownStaleSequence = metaData.getStaleSequence();

            if (lastKnownStaleSequence >= lastReceivedSequence) {
                break;
            }

        } while (!metaData.casStaleSequence(lastKnownStaleSequence, lastReceivedSequence));


        if (logger.isFinestEnabled()) {
            logger.finest(format("%s:[map=%s,partition=%d,lowerSequencesStaleThan=%d,lastReceivedSequence=%d]",
                    "Stale sequences updated", name, partition, metaData.getStaleSequence(), metaData.getSequence()));
        }

    }

    // more than one thread can concurrently call this method: one is anti-entropy, other one is event service thread
    public void checkOrRepairUuid(final int partition, final UUID newUuid) {
        assert newUuid != null;

        MetaDataContainer metaData = getMetaDataContainer(partition);

        do {
            UUID prevUuid = metaData.getUuid();
            if (prevUuid != null && prevUuid.equals(newUuid)) {
                break;
            }

            if (metaData.casUuid(prevUuid, newUuid)) {
                metaData.resetSequence();
                metaData.resetStaleSequence();

                if (logger.isFinestEnabled()) {
                    logger.finest(format("%s:[name=%s,partition=%d,prevUuid=%s,newUuid=%s]",
                            "Invalid uuid, lost remote partition data unexpectedly",
                            name, partition, prevUuid, newUuid));
                }

                break;
            }
        } while (true);

    }

    /**
     * Checks {@code nextSequence} against current one. And updates current sequence if next one is bigger.
     */
    // more than one thread can concurrently call this method: one is anti-entropy, other one is event service thread
    public void checkOrRepairSequence(final int partition, final long nextSequence, final boolean viaAntiEntropy) {
        assert nextSequence > 0;

        MetaDataContainer metaData = getMetaDataContainer(partition);

        do {
            final long currentSequence = metaData.getSequence();
            if (currentSequence >= nextSequence) {
                break;
            }

            if (metaData.casSequence(currentSequence, nextSequence)) {
                final long sequenceDiff = nextSequence - currentSequence;
                if (viaAntiEntropy || sequenceDiff > 1L) {
                    // we have found at least one missing sequence between current and next sequences. if miss is detected by
                    // anti-entropy, number of missed sequences will be `miss = next - current`, otherwise it means miss is
                    // detected by observing received invalidation event sequence numbers and number of missed sequences will be
                    // `miss = next - current - 1`.
                    final long missCount = viaAntiEntropy ? sequenceDiff : sequenceDiff - 1;
                    final long totalMissCount = metaData.addAndGetMissedSequenceCount(missCount);

                    if (logger.isFinestEnabled()) {
                        logger.finest(format("%s:[map=%s,partition=%d,currentSequence=%d,nextSequence=%d,totalMissCount=%d]",
                                "Invalid sequence", name, partition, currentSequence, nextSequence, totalMissCount));
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
