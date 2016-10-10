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

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.CollectionUtil.isEmpty;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Used to process store operations in another thread.
 * Collects entries from write behind queues and passes them to {@link #writeBehindProcessor}.
 * Created per map.
 */
public class StoreWorker implements Runnable {

    private final String mapName;
    private final MapServiceContext mapServiceContext;
    private final WriteBehindProcessor writeBehindProcessor;
    private final ExecutionService executionService;
    /**
     * Run on backup nodes after this interval.
     */
    private final long backupDelayMillis;
    private final long writeDelayMillis;
    private final int partitionCount;
    /**
     * Entries are fetched from write-behind-queues according to this highestStoreTime. If an entry
     * has a store-time which is smaller than or equal to this highestStoreTime, it will be processed.
     * <p/>
     * Next highestStoreTime will be calculated by adding writeDelayMillis to highestStoreTime, so
     * next can be found with the equation `highestStoreTime = highestStoreTime + writeDelayMillis`.
     *
     * @see #calculateHighestStoreTime
     */
    private long lastHighestStoreTime;
    private volatile boolean running;


    public StoreWorker(MapStoreContext mapStoreContext, WriteBehindProcessor writeBehindProcessor) {
        this.mapName = mapStoreContext.getMapName();
        this.mapServiceContext = mapStoreContext.getMapServiceContext();
        this.writeBehindProcessor = writeBehindProcessor;
        this.backupDelayMillis = getReplicaWaitTimeMillis();
        this.lastHighestStoreTime = Clock.currentTimeMillis();
        this.writeDelayMillis = SECONDS.toMillis(mapStoreContext.getMapStoreConfig().getWriteDelaySeconds());
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        this.partitionCount = partitionService.getPartitionCount();
        this.executionService = nodeEngine.getExecutionService();
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
        final long ownerHighestStoreTime = calculateHighestStoreTime(lastHighestStoreTime, now);
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
    }

    private boolean isPartitionLocal(int partitionId) {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        ClusterService clusterService = nodeEngine.getClusterService();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        Address thisAddress = clusterService.getThisAddress();
        InternalPartition partition = partitionService.getPartition(partitionId, false);
        Address owner = partition.getOwnerOrNull();
        return owner != null && owner.equals(thisAddress);
    }

    private static List<DelayedEntry> initListIfNull(List<DelayedEntry> list, int capacity) {
        if (list == null) {
            list = new ArrayList<DelayedEntry>(capacity);
        }
        return list;
    }

    /**
     * Calculates highestStoreTime which is used to select processable entries from write-behind-queues.
     * Entries which have smaller storeTimes than highestStoreTime will be processed.
     *
     * @param lastHighestStoreTime last calculated highest store time in millis.
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

    private void selectEntriesToStore(RecordStore recordStore, List<DelayedEntry> entries, long now) {
        int flushCount = getNumberOfFlushedEntries(recordStore);
        WriteBehindQueue<DelayedEntry> queue = getWriteBehindQueue(recordStore);

        filterWriteBehindQueue(now, flushCount, entries, queue);
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

    private void reAddFailedStoreOperationsToQueues(String mapName, Map<Integer, List<DelayedEntry>> failuresPerPartition) {
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
     */
    private void doInBackup(final List<DelayedEntry> delayedEntries) {
        writeBehindProcessor.callBeforeStoreListeners(delayedEntries);
        removeFinishedStoreOperationsFromQueues(mapName, delayedEntries);
        writeBehindProcessor.callAfterStoreListeners(delayedEntries);
    }

    private long getReplicaWaitTimeMillis() {
        GroupProperties groupProperties = mapServiceContext.getNodeEngine().getGroupProperties();
        return groupProperties.getMillis(GroupProperty.MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS);
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

    @Override
    public String toString() {
        return "StoreWorker{" + "mapName='" + mapName + "'}";
    }
}

