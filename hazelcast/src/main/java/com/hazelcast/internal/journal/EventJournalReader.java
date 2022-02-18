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

import com.hazelcast.ringbuffer.ReadResultSet;

import java.util.concurrent.CompletionStage;

/**
 * This interface provides methods to subscribe and read from an event journal.
 *
 * @param <E> journal event type
 */
public interface EventJournalReader<E> {

    /**
     * Subscribe to the event journal for this reader and a specific partition ID.
     * The method will return the newest and oldest event journal sequence.
     *
     * @param partitionId the partition ID of the entries to which we are subscribing
     * @return {@link CompletionStage} with the initial subscriber state containing the newest and oldest event journal sequence
     * @throws UnsupportedOperationException if the cluster version is lower than 3.9 or there is no event journal
     *                                       configured for this data structure
     * @since 3.9
     */
    CompletionStage<EventJournalInitialSubscriberState> subscribeToEventJournal(int partitionId);

    /**
     * Reads from the event journal. The returned future may throw {@link UnsupportedOperationException}
     * if the cluster version is lower than 3.9 or there is no event journal configured for this data structure.
     * <p>
     * <b>NOTE:</b>
     * Configuring evictions may cause unexpected results when reading from the event journal and
     * there are cluster changes (a backup replica is promoted into a partition owner). See
     * {@link com.hazelcast.map.impl.journal.MapEventJournal} or
     * {@link com.hazelcast.cache.impl.journal.CacheEventJournal} for more details.
     *
     * @param startSequence the sequence of the first item to read
     * @param maxSize       the maximum number of items to read
     * @param partitionId   the partition ID of the entries in the journal
     * @param predicate     the predicate which the events must pass to be included in the response.
     *                      May be {@code null} in which case all events pass the predicate
     * @param projection    the projection which is applied to the events before returning.
     *                      May be {@code null} in which case the event is returned without being projected
     * @param <T>           the return type of the projection. It is equal to the journal event type
     *                      if the projection is {@code null} or it is the identity projection
     * @return {@link CompletionStage} with the filtered and projected journal items
     * @throws IllegalArgumentException if {@code maxSize} is less than {@code minSize}
     * @since 3.9
     */
    <T> CompletionStage<ReadResultSet<T>> readFromEventJournal(
            long startSequence,
            int minSize,
            int maxSize,
            int partitionId,
            java.util.function.Predicate<? super E> predicate,
            java.util.function.Function<? super E, ? extends T> projection
    );
}
