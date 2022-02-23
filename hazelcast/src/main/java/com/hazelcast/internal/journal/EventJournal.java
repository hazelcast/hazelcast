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

package com.hazelcast.internal.journal;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

/**
 * The event journal is a container for events related to a data structure.
 * This interface provides methods for distributed object event journals.
 * Each distributed object and partition has its own event journal.
 * <p>
 * If an object is destroyed or migrated, the related event journal will be destroyed or
 * migrated as well. In this regard, the event journal is co-located with the object partition
 * and its replicas.
 *
 * @param <E> journal event type
 * @since 3.9
 */
public interface EventJournal<E> {
    /**
     * Returns the sequence of the newest event stored in the event journal.
     *
     * @param namespace   the object namespace
     * @param partitionId the partition ID of the event journal
     * @return the sequence of the last stored event
     * @throws IllegalStateException if there is no event journal configured for this object
     */
    long newestSequence(ObjectNamespace namespace, int partitionId);

    /**
     * Returns the sequence of the oldest event stored in the event journal.
     *
     * @param namespace   the object namespace
     * @param partitionId the partition ID of the event journal
     * @return the sequence of the oldest stored event
     * @throws IllegalStateException if there is no event journal configured for this object
     */
    long oldestSequence(ObjectNamespace namespace, int partitionId);

    /**
     * Returns {@code true} if the event journal has persistence enabled and
     * can be queried for events older than the
     * {@link #oldestSequence(ObjectNamespace, int)}. If the journal is not
     * backed by a persistent store, this method will return {@code false}.
     *
     * @param namespace   the object namespace
     * @param partitionId the partition ID of the event journal
     * @return if the journal is backed by a persistent store and can serve events older than the oldest sequence
     * @throws IllegalStateException if there is no event journal configured for this object
     */
    boolean isPersistenceEnabled(ObjectNamespace namespace, int partitionId);

    /**
     * Destroys the event journal for the given object and partition ID.
     *
     * @param namespace   the object namespace
     * @param partitionId the partition ID of the entries in the journal
     */
    void destroy(ObjectNamespace namespace, int partitionId);

    /**
     * Checks if the sequence is of an item that can be read immediately
     * or is the sequence of the next item to be added into the event journal.
     * Since this method allows the sequence to be one larger than
     * the {@link #newestSequence(ObjectNamespace, int)}, the caller can use this method
     * to check the sequence before performing a possibly blocking read.
     *
     * @param namespace   the object namespace
     * @param partitionId the partition ID of the entries in the journal
     * @param sequence    the sequence wanting to be read
     * @throws StaleSequenceException   if the requested sequence is smaller than the
     *                                  sequence of the oldest event (the sequence has
     *                                  been overwritten)
     * @throws IllegalArgumentException if the requested sequence is greater than the
     *                                  next available sequence + 1
     * @throws IllegalStateException    if there is no event journal configured for this object
     */
    void isAvailableOrNextSequence(ObjectNamespace namespace, int partitionId, long sequence);

    /**
     * Checks if the {@code sequence} is the sequence of the next event to
     * be added to the event journal.
     *
     * @param namespace   the object namespace
     * @param partitionId the partition ID of the entries in the journal
     * @param sequence    the sequence to be checked
     * @return {@code true} if the {@code sequence} is one greater
     * than the sequence of the last event, {@code false} otherwise
     * @throws IllegalStateException if there is no event journal configured for this object
     */
    boolean isNextAvailableSequence(ObjectNamespace namespace, int partitionId, long sequence);

    /**
     * Return the {@link WaitNotifyKey} for objects waiting and notifying on the event journal.
     *
     * @param namespace   the object namespace
     * @param partitionId the partition ID of the entries in the journal
     * @return the key for a wait notify object
     * @throws IllegalStateException if there is no event journal configured for this object
     */
    WaitNotifyKey getWaitNotifyKey(ObjectNamespace namespace, int partitionId);

    /**
     * Reads events from the journal in batches. It will read up to
     * the maximum number that the set can hold - see {@link ReadResultSetImpl#isMaxSizeReached()}
     * or until the journal is exhausted. The {@code resultSet} allows
     * filtering and projections on journal items so that the caller
     * can control which data is returned.
     * <p>
     * If the set has reached its max size, the returned sequence is
     * one greater than the sequence of the last item in the set.
     * In other cases it means that the set hasn't reached its full
     * size because we have reached the end of the event journal. In
     * this case the returned sequence is one greater than the sequence
     * of the last stored event.
     *
     * @param namespace     the object namespace
     * @param partitionId   the partition ID of the entries in the journal
     * @param beginSequence the sequence of the first item to read.
     * @param resultSet     the container for read, filtered and projected events
     * @param <T>           the return type of the projected events
     * @return returns the sequenceId of the next item to read
     * @throws IllegalStateException    if there is no event journal configured for this object
     * @throws StaleSequenceException   if the requested sequence is smaller than the sequence of the oldest event
     * @throws IllegalArgumentException if the requested sequence is greater than the sequence of the newest event + 1
     * @see #isAvailableOrNextSequence(ObjectNamespace, int, long)
     */
    <T> long readMany(ObjectNamespace namespace, int partitionId, long beginSequence, ReadResultSetImpl<E, T> resultSet);

    /**
     * Cleans up the event journal by removing any expired items. Items are considered
     * expired according to the configured expiration policy.
     *
     * @param namespace   the object namespace
     * @param partitionId the partition ID of the entries in the journal
     * @throws IllegalStateException if there is no event journal configured for this object
     */
    void cleanup(ObjectNamespace namespace, int partitionId);

    /**
     * Returns {@code true} if the object has a configured and enabled event journal.
     *
     * @param namespace the object namespace
     * @return {@code true} if the object has a configured and enabled event journal, {@code false} otherwise
     */
    boolean hasEventJournal(ObjectNamespace namespace);

    /**
     * Returns the event journal configuration or {@code null} if there is none or the journal is disabled
     * for the given {@code namespace}.
     *
     * @param namespace the object namespace of the specific distributed object
     * @return the journal configuration or {@code null} if the journal is not enabled or available
     */
    EventJournalConfig getEventJournalConfig(ObjectNamespace namespace);

    /**
     * Creates a new {@link RingbufferConfig} for a ringbuffer that will keep
     * event journal events for a single partition.
     *
     * @param config    the event journal config
     * @param namespace the object namespace
     * @return the ringbuffer config for a single partition of the event journal
     */
    RingbufferConfig toRingbufferConfig(EventJournalConfig config, ObjectNamespace namespace);
}
