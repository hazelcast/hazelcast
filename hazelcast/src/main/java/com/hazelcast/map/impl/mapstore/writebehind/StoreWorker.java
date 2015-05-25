/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;
import com.hazelcast.util.CollectionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.CollectionUtil.isEmpty;

/**
 * Used to process store operations in another thread.
 * Collects entries from write behind queues and passes them to {@link #writeBehindProcessor}.
 * Created per map.
 */
public class StoreWorker implements Runnable {

    private final String mapName;

    private final MapServiceContext mapServiceContext;

    private final WriteBehindProcessor writeBehindProcessor;

    /**
     * Run on backup nodes after this interval.
     */
    private final long backupRunIntervalTime;

    /**
     * Last run time of this processor.
     */
    private long lastRunTime;


    public StoreWorker(MapStoreContext mapStoreContext, WriteBehindProcessor writeBehindProcessor) {
        this.mapName = mapStoreContext.getMapName();
        this.mapServiceContext = mapStoreContext.getMapServiceContext();
        this.writeBehindProcessor = writeBehindProcessor;
        this.backupRunIntervalTime = getReplicaWaitTime();
        this.lastRunTime = Clock.currentTimeMillis();
    }


    @Override
    public void run() {
        long now = Clock.currentTimeMillis();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        List<DelayedEntry> entries = new ArrayList<DelayedEntry>();

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            InternalPartition partition = partitionService.getPartition(partitionId, false);
            Address owner = partition.getOwnerOrNull();
            if (owner == null) {
                continue;
            }

            RecordStore recordStore = getRecordStoreOrNull(mapName, partitionId);
            if (recordStore == null) {
                continue;
            }

            List<DelayedEntry> entriesToStore = getEntriesToStore(now, recordStore);
            if (!partition.isLocal()) {
                if (now > lastRunTime + backupRunIntervalTime) {
                    doInBackup(entriesToStore, partitionId);
                }
            } else {
                entries.addAll(entriesToStore);
            }
        }

        if (entries.isEmpty()) {
            return;
        }

        Map<Integer, List<DelayedEntry>> failuresPerPartition = writeBehindProcessor.process(entries);
        removeFinishedStoreOperationsFromQueues(mapName, entries);
        readdFailedStoreOperationsToQueues(mapName, failuresPerPartition);
        lastRunTime = now;
    }

    private List<DelayedEntry> getEntriesToStore(long now, RecordStore recordStore) {
        int flushCount = getNumberOfFlushedEntries(recordStore);
        WriteBehindQueue<DelayedEntry> queue = getWriteBehindQueue(recordStore);

        List<DelayedEntry> entries = new ArrayList<DelayedEntry>();
        filterWriteBehindQueue(now, flushCount, entries, queue);

        return entries;
    }

    private void filterWriteBehindQueue(long now, int count, Collection<DelayedEntry> collection,
                                        WriteBehindQueue<DelayedEntry> queue) {
        if (count > 0) {
            queue.getFrontByNumber(count, collection);
        } else {
            queue.getFrontByTime(now, collection);
        }
    }

    private void removeFinishedStoreOperationsFromQueues(String mapName, List<DelayedEntry> entries) {
        for (DelayedEntry entry : entries) {
            final int partitionId = entry.getPartitionId();
            final RecordStore recordStore = getRecordStoreOrNull(mapName, partitionId);
            if (recordStore == null) {
                continue;
            }
            final WriteBehindQueue<DelayedEntry> queue = getWriteBehindQueue(recordStore);
            queue.removeFirstOccurrence(entry);

            final AtomicInteger flushCounter = getFlushCounter(recordStore);
            final int flushCount = flushCounter.get();
            if (flushCount > 0) {
                flushCounter.addAndGet(-1);
            }
        }
    }

    private void readdFailedStoreOperationsToQueues(String mapName, Map<Integer, List<DelayedEntry>> failuresPerPartition) {
        if (failuresPerPartition.isEmpty()) {
            return;
        }
        for (Map.Entry<Integer, List<DelayedEntry>> entry : failuresPerPartition.entrySet()) {
            final Integer partitionId = entry.getKey();
            final List<DelayedEntry> failures = failuresPerPartition.get(partitionId);
            if (isEmpty(failures)) {
                continue;
            }
            final RecordStore recordStore = getRecordStoreOrNull(mapName, partitionId);
            if (recordStore == null) {
                continue;
            }
            final WriteBehindQueue<DelayedEntry> queue = getWriteBehindQueue(recordStore);
            queue.addFirst(failures);
        }
    }

    /**
     * Process write-behind queues on backup partitions. It is a fake processing and
     * it only removes entries from queues and does not persist any of them.
     *
     * @param delayedEntries entries to be processed.
     * @param partitionId    corresponding partition id.
     */
    private void doInBackup(final List<DelayedEntry> delayedEntries, final int partitionId) {
        if (CollectionUtil.isEmpty(delayedEntries)) {
            return;
        }
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final ClusterService clusterService = nodeEngine.getClusterService();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        final Address thisAddress = clusterService.getThisAddress();
        final InternalPartition partition = partitionService.getPartition(partitionId, false);
        final Address owner = partition.getOwnerOrNull();
        if (owner != null && !owner.equals(thisAddress)) {
            writeBehindProcessor.callBeforeStoreListeners(delayedEntries);
            removeFinishedStoreOperationsFromQueues(mapName, delayedEntries);
            writeBehindProcessor.callAfterStoreListeners(delayedEntries);
        }
    }

    private long getReplicaWaitTime() {
        return TimeUnit.SECONDS.toMillis(mapServiceContext.getNodeEngine().getGroupProperties()
                .MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS.getInteger());
    }

    private RecordStore getRecordStoreOrNull(String mapName, int partitionId) {
        final PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        return partitionContainer.getExistingRecordStore(mapName);
    }

    private static WriteBehindQueue<DelayedEntry> getWriteBehindQueue(RecordStore recordStore) {
        WriteBehindStore writeBehindStore = (WriteBehindStore) recordStore.getMapDataStore();
        return writeBehindStore.getWriteBehindQueue();
    }

    private static AtomicInteger getFlushCounter(RecordStore recordStore) {
        WriteBehindStore writeBehindStore = (WriteBehindStore) recordStore.getMapDataStore();
        return writeBehindStore.getFlushCounter();
    }

    private static int getNumberOfFlushedEntries(RecordStore recordStore) {
        AtomicInteger flushCounter = getFlushCounter(recordStore);
        return flushCounter.get();
    }
}

