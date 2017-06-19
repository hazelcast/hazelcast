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

package com.hazelcast.journal;

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.WaitNotifyKey;

/**
 * The event journal is a container for events related to a data structure.
 * This interface provides methods for distributed object event journals.
 * Each distributed object and partition has it's own event journal.
 * <p>
 * If a object is destroyed or the migrated, the related event journal will be destroyed or
 * migrated as well. In this sense, the event journal is co-located with the object partition
 * and it's replicas.
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
     * If the set has reached it's max size, the returned sequence
     * one greater than the sequence of the last item in the set.
     * In other cases it means that the set hasn't reached it's full
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
     * expired as according to the configured expiration policy.
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

    EventJournalConfig getEventJournalConfig(ObjectNamespace namespace);

    RingbufferConfig toRingbufferConfig(EventJournalConfig config);
}
