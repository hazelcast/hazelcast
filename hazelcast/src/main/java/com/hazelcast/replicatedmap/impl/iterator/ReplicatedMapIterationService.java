/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.iterator;

import com.hazelcast.internal.iteration.IterationResult;
import com.hazelcast.internal.iteration.IteratorWithCursor;
import com.hazelcast.internal.iteration.IteratorWithCursorManager;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryViewHolder;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReplicatedMapIterationService {
    /**
     * ReplicatedMap EntryView iteration creates resources in the server via creating snapshots. These snapshots are kept until
     * the timeout expires or the iteration is complete. This property configures the period of the task that checks for stale
     * iterators. The timeout for a iterator to be considered stale is managed via another property:
     * {@link ReplicatedMapIterationService#ITERATOR_CLEANUP_TIMEOUT_MILLIS}.
     */
    public static final HazelcastProperty ITERATOR_CLEANUP_PERIOD_SECONDS = new HazelcastProperty(
            "hazelcast.replicatedmap.iterator.cleanup.period.seconds", 30, TimeUnit.SECONDS);
    /**
     * The timeout in milliseconds for cleaning up stale iterators. The task that checks for stale iterators runs every
     * {@link ReplicatedMapIterationService#ITERATOR_CLEANUP_PERIOD_SECONDS} seconds.
     */
    public static final HazelcastProperty ITERATOR_CLEANUP_TIMEOUT_MILLIS = new HazelcastProperty(
            "hazelcast.replicatedmap.iterator.cleanup.timeout.millis", 300_000, TimeUnit.MILLISECONDS);
    private final ReplicatedMapService replicatedMapService;
    private final SerializationService serializationService;
    private final IteratorWithCursorManager<ReplicatedRecord> iteratorManager;
    private final HazelcastProperties properties;
    private final NodeEngine nodeEngine;
    private final AtomicBoolean noIteratorCreated = new AtomicBoolean(true);
    private ScheduledFuture iteratorCleanupFuture;
    public ReplicatedMapIterationService(ReplicatedMapService replicatedMapService, SerializationService serializationService,
                                         NodeEngine nodeEngine) {
        this.replicatedMapService = replicatedMapService;
        this.serializationService = serializationService;
        this.properties = nodeEngine.getProperties();
        this.iteratorManager = new IteratorWithCursorManager<>(replicatedMapService.getNodeEngine());
        this.nodeEngine = nodeEngine;
    }

    /**
     * If the partition owner changes during iteration, the iteration will fail
     * with a {@link IllegalStateException} stating that there is no iteration with
     * the provided cursor id because this method will be called on a different member.
     */
    public void createIterator(String name, int partitionId, UUID cursorId) {
        if (noIteratorCreated.getAndSet(false)) {
            // little optimization to create the future upon first iterator creation.
            this.iteratorCleanupFuture = nodeEngine.getExecutionService().getGlobalTaskScheduler().scheduleWithRepetition(
                    this::removeStaleIterators, 0,
                    nodeEngine.getProperties().getInteger(ITERATOR_CLEANUP_PERIOD_SECONDS), TimeUnit.SECONDS);
        }
        ReplicatedRecordStore store = this.replicatedMapService.getReplicatedRecordStore(name, false, partitionId);
        if (store == null) {
            throw new IllegalStateException("There is no ReplicatedRecordStore for " + name + " on partitionId "
                    + partitionId + " on member " + replicatedMapService.getNodeEngine().getThisAddress() + ".");
        }
        iteratorManager.createIterator(store.recordIterator(), cursorId);
    }

    public IterationResult<ReplicatedMapEntryViewHolder> iterate(UUID cursorId, int maxCount) {
        IterationResult<ReplicatedRecord> result = iteratorManager.iterate(cursorId, maxCount);
        List<ReplicatedRecord> page = result.getPage();
        return new IterationResult<>(convertToHolder(page), result.getCursorId(), result.getCursorIdToForget());
    }

    private List<ReplicatedMapEntryViewHolder> convertToHolder(List<ReplicatedRecord> page) {
        if (page.isEmpty()) {
            return Collections.emptyList();
        }
        List<ReplicatedMapEntryViewHolder> result = new ArrayList<>(page.size());
        for (ReplicatedRecord record : page) {
            result.add(toEntryViewHolder(record));
        }
        return result;
    }

    public void removeStaleIterators() {
        ConcurrentHashMap.KeySetView<UUID, IteratorWithCursor<ReplicatedRecord>> keySetView = iteratorManager.getKeySet();
        for (UUID iteratorId : keySetView) {
            IteratorWithCursor<ReplicatedRecord> paginator = iteratorManager.getIterator(iteratorId);
            if (paginator.getLastAccessTime()
                    < System.currentTimeMillis() - properties.getLong(ITERATOR_CLEANUP_TIMEOUT_MILLIS)) {
                keySetView.remove(iteratorId);
            }
        }
    }

    public IteratorWithCursorManager<ReplicatedRecord> getIteratorManager() {
        return iteratorManager;
    }

    private ReplicatedMapEntryViewHolder toEntryViewHolder(ReplicatedRecord record) {
        return new ReplicatedMapEntryViewHolder(toData(record.getKey()), toData(record.getValue()), record.getCreationTime(),
                record.getHits(), record.getLastAccessTime(), record.getUpdateTime(), record.getTtlMillis());
    }

    private Data toData(Object object) {
        return serializationService.toData(object);
    }

    public void shutdown() {
        if (iteratorCleanupFuture != null) {
            iteratorCleanupFuture.cancel(true);
        }
    }
}
