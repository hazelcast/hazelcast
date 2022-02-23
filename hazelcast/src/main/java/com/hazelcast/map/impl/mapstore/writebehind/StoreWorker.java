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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.internal.util.Clock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * When write-behind is enabled the work is offloaded to another thread than partition-operation-thread.
 * That thread uses this runnable task to process write-behind-queues. This task collects entries from
 * write behind queues and passes them to {@link #writeBehindProcessor}.
 * <p>
 * Only one {@link StoreWorker} task is created for a map on a member.
 */
public class StoreWorker implements Runnable {
    private final String mapName;
    private final MapServiceContext mapServiceContext;
    private final IPartitionService partitionService;
    private final ExecutionService executionService;
    private final WriteBehindProcessor writeBehindProcessor;
    /**
     * Run on backup nodes after this interval.
     */
    private final long backupDelayMillis;
    private final long writeDelayMillis;
    private final int partitionCount;
    /**
     * Entries are fetched from write-behind-queues according to highestStoreTime. If an entry
     * has a store-time which is smaller than or equal to the highestStoreTime, it will be processed.
     *
     * @see #calculateHighestStoreTime
     */
    private long lastHighestStoreTime;
    private volatile boolean running;

    public StoreWorker(MapStoreContext mapStoreContext, WriteBehindProcessor writeBehindProcessor) {
        this.mapName = mapStoreContext.getMapName();
        this.mapServiceContext = mapStoreContext.getMapServiceContext();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        this.partitionService = nodeEngine.getPartitionService();
        this.executionService = nodeEngine.getExecutionService();
        this.writeBehindProcessor = writeBehindProcessor;
        this.backupDelayMillis = getReplicaWaitTimeMillis();
        this.lastHighestStoreTime = Clock.currentTimeMillis();
        this.writeDelayMillis = SECONDS.toMillis(getWriteDelaySeconds(mapStoreContext));
        this.partitionCount = partitionService.getPartitionCount();
    }


    public synchronized void start() {
        if (running) {
            return;
        }

        running = true;
        schedule();
    }

    public synchronized void stop() {
        running = false;
    }

    @Override
    public void run() {
        try {
            runInternal();
        } finally {
            if (running) {
                schedule();
            }
        }
    }

    private void schedule() {
        executionService.schedule(this, 1, SECONDS);
    }

    private void runInternal() {
        final long now = Clock.currentTimeMillis();
        // if this node is the owner of a partition, we use this criteria time.
        final long ownerHighestStoreTime = calculateHighestStoreTime(lastHighestStoreTime, now);
        // if this node is the backup of a partition, we use this criteria time because backups are processed after delay.
        final long backupHighestStoreTime = ownerHighestStoreTime - backupDelayMillis;

        lastHighestStoreTime = ownerHighestStoreTime;

        List<DelayedEntry> ownersList = null;
        List<DelayedEntry> backupsList = null;

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            if (currentThread().isInterrupted()) {
                break;
            }

            RecordStore recordStore = getRecordStoreOrNull(mapName, partitionId);
            if (!hasEntryInWriteBehindQueue(recordStore)) {
                continue;
            }

            boolean localPartition = isPartitionLocal(partitionId);

            if (!localPartition) {
                backupsList = initListIfNull(backupsList, partitionCount);
                selectEntriesToStore(recordStore, backupsList, backupHighestStoreTime);
            } else {
                ownersList = initListIfNull(ownersList, partitionCount);
                selectEntriesToStore(recordStore, ownersList, ownerHighestStoreTime);
            }
        }

        if (!isEmpty(ownersList)) {
            Map<Integer, List<DelayedEntry>> failuresPerPartition = writeBehindProcessor.process(ownersList);
            removeFinishedStoreOperationsFromQueues(mapName, ownersList);
            reAddFailedStoreOperationsToQueues(mapName, failuresPerPartition);
        }

        if (!isEmpty(backupsList)) {
            doInBackup(backupsList);
        }

        notifyFlush();

    }

    private static List<DelayedEntry> initListIfNull(List<DelayedEntry> list, int capacity) {
        if (list == null) {
            list = new ArrayList<>(capacity);
        }
        return list;
    }

    /**
     * Calculates highestStoreTime which is used to select processable entries from write-behind-queues.
     * Entries which have smaller storeTimes than highestStoreTime will be processed.
     *
     * @param lastHighestStoreTime last calculated highest store time.
     * @param now                  now in millis
     * @return highestStoreTime in millis.
     */
    private long calculateHighestStoreTime(long lastHighestStoreTime, long now) {
        return now >= lastHighestStoreTime + writeDelayMillis ? now : lastHighestStoreTime;
    }

    private boolean hasEntryInWriteBehindQueue(RecordStore recordStore) {
        if (recordStore == null) {
            return false;
        }

        MapDataStore mapDataStore = recordStore.getMapDataStore();
        WriteBehindStore dataStore = (WriteBehindStore) mapDataStore;
        WriteBehindQueue<DelayedEntry> writeBehindQueue = dataStore.getWriteBehindQueue();
        return writeBehindQueue.size() != 0;
    }

    private void notifyFlush() {
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

    private void selectEntriesToStore(RecordStore recordStore, List<DelayedEntry> entries, long highestStoreTime) {
        WriteBehindQueue<DelayedEntry> queue = getWriteBehindQueue(recordStore);
        long nextSequenceToFlush = getSequenceToFlush(recordStore);

        filterWriteBehindQueue(highestStoreTime, nextSequenceToFlush, entries, queue);
    }

    private void filterWriteBehindQueue(final long highestStoreTime, final long sequence, Collection<DelayedEntry> collection,
                                        WriteBehindQueue<DelayedEntry> queue) {
        if (sequence > 0) {
            queue.filter(delayedEntry -> delayedEntry.getSequence() <= sequence, collection);
        } else {
            queue.filter(delayedEntry -> delayedEntry.getStoreTime() <= highestStoreTime, collection);
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
            // TODO what if capacity is exceeded during addFirst
            queue.addFirst(failures);
        }
    }

    /**
     * Process write-behind queues on backup partitions. It is a fake processing and
     * it only removes entries from queues and does not persist any of them.
     *
     * @param delayedEntries entries to be processed.
     */
    private void doInBackup(List<DelayedEntry> delayedEntries) {
        writeBehindProcessor.callBeforeStoreListeners(delayedEntries);
        removeFinishedStoreOperationsFromQueues(mapName, delayedEntries);
        writeBehindProcessor.callAfterStoreListeners(delayedEntries);
    }

    private long getReplicaWaitTimeMillis() {
        HazelcastProperties hazelcastProperties = mapServiceContext.getNodeEngine().getProperties();
        return hazelcastProperties.getMillis(ClusterProperty.MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS);
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

    private static int getWriteDelaySeconds(MapStoreContext mapStoreContext) {
        MapStoreConfig mapStoreConfig = mapStoreContext.getMapStoreConfig();
        return mapStoreConfig.getWriteDelaySeconds();
    }

    @Override
    public String toString() {
        return "StoreWorker{" + "mapName='" + mapName + "'}";
    }
}
