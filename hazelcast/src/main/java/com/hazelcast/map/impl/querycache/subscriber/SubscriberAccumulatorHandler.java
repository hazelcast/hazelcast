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

package com.hazelcast.map.impl.querycache.subscriber;

import com.hazelcast.core.EntryEventType;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorHandler;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.internal.serialization.Data;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.map.impl.querycache.subscriber.EventPublisherHelper.hasListener;
import static com.hazelcast.map.impl.querycache.subscriber.EventPublisherHelper.publishCacheWideEvent;
import static java.lang.String.format;

/**
 * - Processes accumulated event-data of {@link SubscriberAccumulator}.
 * - Multiple thread can access this class.
 * - Created per query-cache
 */
class SubscriberAccumulatorHandler implements AccumulatorHandler<QueryCacheEventData> {

    // if a thread has this permission, that thread can process queues.
    private static final Queue<Integer> POLL_PERMIT = new ConcurrentLinkedQueue<>();

    private final int partitionCount;
    private final boolean includeValue;
    private final InternalQueryCache queryCache;
    private final InternalSerializationService serializationService;
    private final AtomicReferenceArray<Queue<Integer>> clearAllRemovedCountHolders;
    private final AtomicReferenceArray<Queue<Integer>> evictAllRemovedCountHolders;

    SubscriberAccumulatorHandler(boolean includeValue, InternalQueryCache queryCache,
                                        InternalSerializationService serializationService) {
        this.includeValue = includeValue;
        this.queryCache = queryCache;
        this.serializationService = serializationService;
        this.partitionCount = ((DefaultQueryCache) queryCache).context.getPartitionCount();
        this.clearAllRemovedCountHolders = initRemovedCountHolders(partitionCount);
        this.evictAllRemovedCountHolders = initRemovedCountHolders(partitionCount);
    }

    @Override
    public void reset() {
        queryCache.clear();
        for (int i = 0; i < partitionCount; i++) {
            clearAllRemovedCountHolders.set(i, new ConcurrentLinkedQueue<>());
            evictAllRemovedCountHolders.set(i, new ConcurrentLinkedQueue<>());
        }
    }

    private static AtomicReferenceArray<Queue<Integer>> initRemovedCountHolders(int partitionCount) {
        AtomicReferenceArray<Queue<Integer>> removedCountHolders
                = new AtomicReferenceArray<>(partitionCount + 1);
        for (int i = 0; i < partitionCount; i++) {
            removedCountHolders.set(i, new ConcurrentLinkedQueue<>());
        }
        removedCountHolders.set(partitionCount, POLL_PERMIT);

        return removedCountHolders;
    }

    @Override
    public void handle(QueryCacheEventData eventData, boolean ignored) {
        eventData.setSerializationService(serializationService);

        Data keyData = eventData.getDataKey();
        Data valueData = includeValue ? eventData.getDataNewValue() : null;

        int eventType = eventData.getEventType();
        EntryEventType entryEventType = EntryEventType.getByType(eventType);
        if (entryEventType == null) {
            throwException(format("No matching EntryEventType found for event type id `%d`", eventType));
        }

        switch (entryEventType) {
            case ADDED:
            case UPDATED:
            case MERGED:
            case LOADED:
                queryCache.set(keyData, valueData, entryEventType);
                break;
            case REMOVED:
            case EVICTED:
            case EXPIRED:
                queryCache.delete(keyData, entryEventType);
                break;
            case CLEAR_ALL:
                handleMapWideEvent(eventData, entryEventType, clearAllRemovedCountHolders);
                break;
            case EVICT_ALL:
                handleMapWideEvent(eventData, entryEventType, evictAllRemovedCountHolders);
                break;
            default:
                throwException(format("Unexpected EntryEventType was found: `%s`", entryEventType));
        }
    }

    private void handleMapWideEvent(QueryCacheEventData eventData, EntryEventType eventType,
                                    AtomicReferenceArray<Queue<Integer>> removedCountHolders) {

        int partitionId = eventData.getPartitionId();
        int removedCount = queryCache.removeEntriesOf(partitionId);

        tryPublishMapWideEvent(eventType, partitionId, removedCount, removedCountHolders);
    }

    private void tryPublishMapWideEvent(EntryEventType eventType, int partitionId, int removedEntryCount,
                                        AtomicReferenceArray<Queue<Integer>> removedCountHolders) {
        if (!hasListener(queryCache)) {
            return;
        }

        // add this `removedEntryCount` to partitions' removed-entry-count-holder queue
        removedCountHolders.get(partitionId).offer(removedEntryCount);

        if (noMissingMapWideEvent(removedCountHolders)
                && removedCountHolders.compareAndSet(partitionCount, POLL_PERMIT, null)) {
            try {
                if (noMissingMapWideEvent(removedCountHolders)) {
                    int totalRemovedCount = pollRemovedCountHolders(removedCountHolders);
                    publishCacheWideEvent(queryCache, totalRemovedCount, eventType);
                }
            } finally {
                removedCountHolders.set(partitionCount, POLL_PERMIT);
            }
        }
    }

    /**
     * Check if all map-wide events like {@link
     * EntryEventType#CLEAR_ALL} or {@link EntryEventType#EVICT_ALL}
     * were received. If an event is received, we populate its
     * partitions' removed-entry-count-holder queue.
     *
     * @return {@code true} if we have received map-wide events from all
     * partitions, otherwise return {@code false} to indicate there is
     * still not-received map-wide events for some partitions.
     */
    private boolean noMissingMapWideEvent(AtomicReferenceArray<Queue<Integer>> removedCountHolders) {
        for (int i = 0; i < partitionCount; i++) {
            if (removedCountHolders.get(i).isEmpty()) {
                // means we still have not-received map-wide event for this partition
                return false;
            }
        }
        return true;
    }

    // should be called when `noMissingMapWideEvent` `false`, otherwise polling can cause NPE
    private int pollRemovedCountHolders(AtomicReferenceArray<Queue<Integer>> removedCountHolders) {
        int count = 0;
        for (int i = 0; i < partitionCount; i++) {
            Queue<Integer> removalCounts = removedCountHolders.get(i);
            count += removalCounts.poll();
        }
        return count;
    }

    private static void throwException(String msg) {
        throw new IllegalArgumentException(msg);
    }
}
