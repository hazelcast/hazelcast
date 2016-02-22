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

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.partition.IPartition;
import com.hazelcast.partition.IPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;
import com.hazelcast.util.CollectionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.CollectionUtil.isEmpty;

/**
 * When write-behind is enabled the work is offloaded to another thread than partition-operation-thread.
 * That thread uses this runnable task to process write-behind-queues. This task collects entries from
 * write behind queues and passes them to {@link #writeBehindProcessor}.
 * <p/>
 * Only one {@link StoreWorker} task is created for a map on a member.
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
    private final IPartitionService partitionService;
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
            if (!hasEntryInWriteBehindQueue(recordStore)) {
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
            reAddFailedStoreOperationsToQueues(mapName, failuresPerPartition);
            lastRunTimeMillis = now;
        }

        notifyFlush();
    }

    protected boolean hasEntryInWriteBehindQueue(RecordStore recordStore) {
        if (recordStore == null) {
            return false;
        }

        MapDataStore mapDataStore = recordStore.getMapDataStore();
        WriteBehindStore dataStore = (WriteBehindStore) mapDataStore;
        WriteBehindQueue<DelayedEntry> writeBehindQueue = dataStore.getWriteBehindQueue();
        return writeBehindQueue.size() != 0;
    }

    protected void notifyFlush() {
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            RecordStore recordStore = getRecordStoreOrNull(mapName, partitionId);
            if (recordStore != null) {
                WriteBehindStore mapDataStore = ((WriteBehindStore) recordStore.getMapDataStore());
                mapDataStore.notifyFlush();
            }
        }
    }

    private boolean isPartitionLocal(int partitionId) {
        IPartition partition = partitionService.getPartition(partitionId, false);
        return partition.isLocal();
    }

    private List<DelayedEntry> getEntriesToStore(RecordStore recordStore, long now) {
        WriteBehindQueue<DelayedEntry> queue = getWriteBehindQueue(recordStore);
        long nextSequenceToFlush = getSequenceToFlush(recordStore);

        int capacity = findListCapacity(queue, nextSequenceToFlush);
        List<DelayedEntry> entries = new ArrayList<DelayedEntry>(capacity);

        filterWriteBehindQueue(now, nextSequenceToFlush, entries, queue);

        return entries;
    }

    private int findListCapacity(WriteBehindQueue<DelayedEntry> queue, long nextSequenceToFlush) {
        DelayedEntry peek = queue.peek();

        assert peek != null : "NPE should not happen, previous lines should prevent it";

        long firstSequenceInQueue = peek.getSequence();
        return Math.max((int) (nextSequenceToFlush - firstSequenceInQueue), ARRAY_LIST_DEFAULT_CAPACITY);
    }

    private void filterWriteBehindQueue(final long now, final long sequence, Collection<DelayedEntry> collection,
                                        WriteBehindQueue<DelayedEntry> queue) {
        if (sequence > 0) {

            queue.filter(new IPredicate<DelayedEntry>() {
                @Override
                public boolean test(DelayedEntry delayedEntry) {
                    return delayedEntry.getSequence() <= sequence;
                }
            }, collection);

        } else {

            queue.filter(new IPredicate<DelayedEntry>() {
                @Override
                public boolean test(DelayedEntry delayedEntry) {
                    return delayedEntry.getStoreTime() <= now;
                }
            }, collection);
        }
    }

    private void removeFinishedStoreOperationsFromQueues(String mapName, List<DelayedEntry> entries) {
        for (DelayedEntry entry : entries) {
            RecordStore recordStore = getRecordStoreOrNull(mapName, entry.getPartitionId());
            if (recordStore != null) {
                getWriteBehindQueue(recordStore).removeFirstOccurrence(entry);
            }
        }
    }

    private void reAddFailedStoreOperationsToQueues(String mapName, Map<Integer, List<DelayedEntry>> failuresPerPartition) {
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

    private long getSequenceToFlush(RecordStore recordStore) {
        WriteBehindStore writeBehindStore = (WriteBehindStore) recordStore.getMapDataStore();
        return writeBehindStore.getSequenceToFlush();
    }
}

