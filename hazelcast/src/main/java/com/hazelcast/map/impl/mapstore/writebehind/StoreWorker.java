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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.internal.instance.GroupProperties;
import com.hazelcast.internal.instance.GroupProperty;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;
import com.hazelcast.util.CollectionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.CollectionUtil.isEmpty;

/**
 * Used to process store operations in another thread.
 * Collects entries from write behind queues and passes them to {@link #writeBehindProcessor}.
 * Created per map.
 */
public class StoreWorker implements Runnable {

    private static final int ARRAY_LIST_DEFAULT_CAPACITY = 16;

    /**
     * Write-behind-queues of backup partitions are processed after this delay
     */
    private final long backupRunIntervalMillis;
    private final int partitionCount;
    private final String mapName;
    private final MapServiceContext mapServiceContext;
    private final InternalPartitionService partitionService;
    private final WriteBehindProcessor writeBehindProcessor;

    private long lastRunTimeMillis;

    public StoreWorker(MapStoreContext mapStoreContext, WriteBehindProcessor writeBehindProcessor) {
        this.mapName = mapStoreContext.getMapName();
        this.mapServiceContext = mapStoreContext.getMapServiceContext();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.partitionService = nodeEngine.getPartitionService();
        this.writeBehindProcessor = writeBehindProcessor;
        this.backupRunIntervalMillis = getReplicaWaitTimeMillis();
        this.lastRunTimeMillis = Clock.currentTimeMillis();
        this.partitionCount = partitionService.getPartitionCount();
    }


    @Override
    public void run() {
        long now = Clock.currentTimeMillis();

        List<DelayedEntry> entries = new ArrayList<DelayedEntry>(partitionCount);

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            RecordStore recordStore = getRecordStoreOrNull(mapName, partitionId);
            if (recordStore == null) {
                continue;
            }

            List<DelayedEntry> entriesToStore = getEntriesToStore(recordStore, now);
            if (!isPartitionLocal(partitionId)) {
                if (now > lastRunTimeMillis + backupRunIntervalMillis) {
                    doInBackup(entriesToStore, partitionId);
                }
            } else {
                entries.addAll(entriesToStore);
            }
        }

        if (!entries.isEmpty()) {
            Map<Integer, List<DelayedEntry>> failuresPerPartition = writeBehindProcessor.process(entries);
            removeFinishedStoreOperationsFromQueues(mapName, entries);
            readdFailedStoreOperationsToQueues(mapName, failuresPerPartition);
            lastRunTimeMillis = now;
        }

    }

    private boolean isPartitionLocal(int partitionId) {
        InternalPartition partition = partitionService.getPartition(partitionId, false);
        return partition.isLocal();
    }

    private List<DelayedEntry> getEntriesToStore(RecordStore recordStore, long now) {
        int numberOfEntriesToFlush = getNumberOfEntriesToFlush(recordStore);
        int initialCapacity = Math.max(numberOfEntriesToFlush, ARRAY_LIST_DEFAULT_CAPACITY);

        List<DelayedEntry> entries = new ArrayList<DelayedEntry>(initialCapacity);

        WriteBehindQueue<DelayedEntry> queue = getWriteBehindQueue(recordStore);
        filterWriteBehindQueue(now, numberOfEntriesToFlush, entries, queue);

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
            RecordStore recordStore = getRecordStoreOrNull(mapName, entry.getPartitionId());
            if (recordStore != null) {
                getWriteBehindQueue(recordStore).removeFirstOccurrence(entry);
                decrementFlushCounter(recordStore);
            }
        }
    }

    private void readdFailedStoreOperationsToQueues(String mapName, Map<Integer, List<DelayedEntry>> failuresPerPartition) {
        if (failuresPerPartition.isEmpty()) {
            return;
        }
        for (Map.Entry<Integer, List<DelayedEntry>> entry : failuresPerPartition.entrySet()) {
            Integer partitionId = entry.getKey();
            List<DelayedEntry> failures = failuresPerPartition.get(partitionId);
            if (isEmpty(failures)) {
                continue;
            }
            RecordStore recordStore = getRecordStoreOrNull(mapName, partitionId);
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
    private void doInBackup(List<DelayedEntry> delayedEntries, int partitionId) {
        if (CollectionUtil.isEmpty(delayedEntries) || isPartitionLocal(partitionId)) {
            return;
        }

        writeBehindProcessor.callBeforeStoreListeners(delayedEntries);
        removeFinishedStoreOperationsFromQueues(mapName, delayedEntries);
        writeBehindProcessor.callAfterStoreListeners(delayedEntries);
    }

    private long getReplicaWaitTimeMillis() {
        GroupProperties groupProperties = mapServiceContext.getNodeEngine().getGroupProperties();
        return groupProperties.getMillis(GroupProperty.MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS);
    }

    private RecordStore getRecordStoreOrNull(String mapName, int partitionId) {
        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        return partitionContainer.getExistingRecordStore(mapName);
    }

    private WriteBehindQueue<DelayedEntry> getWriteBehindQueue(RecordStore recordStore) {
        WriteBehindStore writeBehindStore = (WriteBehindStore) recordStore.getMapDataStore();
        return writeBehindStore.getWriteBehindQueue();
    }

    private int getNumberOfEntriesToFlush(RecordStore recordStore) {
        WriteBehindStore writeBehindStore = (WriteBehindStore) recordStore.getMapDataStore();
        return writeBehindStore.getNumberOfEntriesToFlush();
    }

    private void decrementFlushCounter(RecordStore recordStore) {
        WriteBehindStore writeBehindStore = (WriteBehindStore) recordStore.getMapDataStore();
        writeBehindStore.decrementFlushCounter();
    }
}

