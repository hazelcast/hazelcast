/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Data;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.map.impl.querycache.subscriber.EventPublisherHelper.publishCacheWideEvent;

/**
 * - Processes accumulated event-data of {@link SubscriberAccumulator}.
 * - Multiple thread can access this class.
 * - Created per query-cache
 */
class SubscriberAccumulatorHandler implements AccumulatorHandler<QueryCacheEventData> {

    // if a thread has this permission, that thread can process queues.
    private static final Queue<Integer> POLL_PERMIT = new ConcurrentLinkedQueue<Integer>();

    private final int partitionCount;
    private final boolean includeValue;
    private final InternalQueryCache queryCache;
    private final InternalSerializationService serializationService;
    private final AtomicReferenceArray<Queue<Integer>> clearAllRemovedEntryCounts;
    private final AtomicReferenceArray<Queue<Integer>> evictAllRemovedEntryCounts;

    public SubscriberAccumulatorHandler(boolean includeValue, InternalQueryCache queryCache,
                                        InternalSerializationService serializationService) {
        this.includeValue = includeValue;
        this.queryCache = queryCache;
        this.serializationService = serializationService;
        this.partitionCount = ((DefaultQueryCache) queryCache).context.getPartitionCount();
        this.clearAllRemovedEntryCounts = initRemovedEntryCounts(partitionCount);
        this.evictAllRemovedEntryCounts = initRemovedEntryCounts(partitionCount);
    }

    private static AtomicReferenceArray<Queue<Integer>> initRemovedEntryCounts(int partitionCount) {
        AtomicReferenceArray<Queue<Integer>> removedEntryCounts
                = new AtomicReferenceArray<Queue<Integer>>(partitionCount + 1);
        for (int i = 0; i < partitionCount; i++) {
            removedEntryCounts.set(i, new ConcurrentLinkedQueue<Integer>());
        }
        removedEntryCounts.set(partitionCount, POLL_PERMIT);

        return removedEntryCounts;
    }

    @Override
    public void handle(QueryCacheEventData eventData, boolean ignored) {
        eventData.setSerializationService(serializationService);

        Data keyData = eventData.getDataKey();
        Data valueData = includeValue ? eventData.getDataNewValue() : null;

        int eventType = eventData.getEventType();
        EntryEventType entryEventType = EntryEventType.getByType(eventType);
        switch (entryEventType) {
            case ADDED:
            case UPDATED:
            case MERGED:
                queryCache.setInternal(keyData, valueData, false, entryEventType);
                break;
            case REMOVED:
            case EVICTED:
                queryCache.deleteInternal(keyData, false, entryEventType);
                break;
            case CLEAR_ALL:
                handleMapWideEvent(eventData, entryEventType, clearAllRemovedEntryCounts);
                break;
            case EVICT_ALL:
                handleMapWideEvent(eventData, entryEventType, evictAllRemovedEntryCounts);
                break;
            default:
                throw new IllegalArgumentException("Not a known type EntryEventType." + entryEventType);
        }
    }

    private void handleMapWideEvent(QueryCacheEventData eventData, EntryEventType eventType,
                                    AtomicReferenceArray<Queue<Integer>> removedEntryCounts) {

        int partitionId = eventData.getPartitionId();
        int removedEntryCount = queryCache.removeEntriesOf(partitionId);

        // add this `removedEntryCount` to partitions' removed-entry-count-holder queue
        removedEntryCounts.get(partitionId).offer(removedEntryCount);

        if (!hasMissingCount(removedEntryCounts)) {
            if (removedEntryCounts.compareAndSet(partitionCount, POLL_PERMIT, null)) {
                try {
                    int totalRemovedEntryCount = pollRemovedEntryCounts(removedEntryCounts);
                    publishCacheWideEvent(((DefaultQueryCache) queryCache).context,
                            ((DefaultQueryCache) queryCache).mapName,
                            ((DefaultQueryCache) queryCache).cacheId,
                            totalRemovedEntryCount,
                            eventType);
                } finally {
                    removedEntryCounts.set(partitionCount, POLL_PERMIT);
                }
            }
        }
    }

    /**
     * @return {@code true} if we have removed entries from all partitions,
     * otherwise return {@code false} to indicate there is still
     * not-received events for some partitions.
     */
    private boolean hasMissingCount(AtomicReferenceArray<Queue<Integer>> removedEntryCounts) {
        for (int i = 0; i < partitionCount; i++) {
            if (removedEntryCounts.get(i).size() < 1) {
                // we don't receive any map-wide event for this partition
                return true;
            }
        }
        return false;
    }

    // should be called when `hasMissingCount` `false`, otherwise polling can cause NPE
    private int pollRemovedEntryCounts(AtomicReferenceArray<Queue<Integer>> removedEntryCounts) {
        int count = 0;
        for (int i = 0; i < partitionCount; i++) {
            count += removedEntryCounts.get(i).poll();
        }
        return count;
    }
}
