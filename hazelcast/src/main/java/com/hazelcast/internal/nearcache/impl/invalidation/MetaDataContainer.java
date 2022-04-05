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

package com.hazelcast.internal.nearcache.impl.invalidation;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * Contains one partitions' invalidation metadata.
 */
public final class MetaDataContainer {

    private static final AtomicLongFieldUpdater<MetaDataContainer> SEQUENCE
            = newUpdater(MetaDataContainer.class, "sequence");
    private static final AtomicLongFieldUpdater<MetaDataContainer> STALE_SEQUENCE
            = newUpdater(MetaDataContainer.class, "staleSequence");
    private static final AtomicLongFieldUpdater<MetaDataContainer> MISSED_SEQUENCE_COUNT =
            newUpdater(MetaDataContainer.class, "missedSequenceCount");
    private static final AtomicReferenceFieldUpdater<MetaDataContainer, UUID> UUID =
            newUpdater(MetaDataContainer.class, java.util.UUID.class, "uuid");

    /**
     * Sequence number of last received invalidation event
     */
    private volatile long sequence;

    /**
     * Holds the biggest sequence number that is lost, lower sequences from this sequence are accepted as stale
     *
     * @see StaleReadDetector
     */
    private volatile long staleSequence;

    /**
     * Number of missed sequence count
     */
    private volatile long missedSequenceCount;

    /**
     * UUID of the source partition that generates invalidation events
     */
    private volatile UUID uuid;

    public MetaDataContainer() {
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        UUID.set(this, uuid);
    }

    public boolean casUuid(UUID prevUuid, UUID newUuid) {
        return UUID.compareAndSet(this, prevUuid, newUuid);
    }

    public long getSequence() {
        return sequence;
    }

    public void setSequence(long sequence) {
        SEQUENCE.set(this, sequence);
    }

    public boolean casSequence(long currentSequence, long nextSequence) {
        return SEQUENCE.compareAndSet(this, currentSequence, nextSequence);
    }

    public void resetSequence() {
        SEQUENCE.set(this, 0);
    }

    public long getStaleSequence() {
        return STALE_SEQUENCE.get(this);
    }

    public boolean casStaleSequence(long lastKnownStaleSequence, long lastReceivedSequence) {
        return STALE_SEQUENCE.compareAndSet(this, lastKnownStaleSequence, lastReceivedSequence);
    }

    public void resetStaleSequence() {
        STALE_SEQUENCE.set(this, 0);
    }

    public long addAndGetMissedSequenceCount(long missCount) {
        return MISSED_SEQUENCE_COUNT.addAndGet(this, missCount);
    }

    public long getMissedSequenceCount() {
        return missedSequenceCount;
    }
}
