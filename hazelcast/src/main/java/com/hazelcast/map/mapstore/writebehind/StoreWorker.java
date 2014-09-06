/*
* Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.map.MapContainer;
import com.hazelcast.map.MapServiceContext;
import com.hazelcast.map.PartitionContainer;
import com.hazelcast.map.RecordStore;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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


    public StoreWorker(MapContainer mapContainer, WriteBehindProcessor writeBehindProcessor) {
        this.mapName = mapContainer.getName();
        this.mapServiceContext = mapContainer.getMapServiceContext();
        this.writeBehindProcessor = writeBehindProcessor;
        this.backupRunIntervalTime = getReplicaWaitTime();
        this.lastRunTime = Clock.currentTimeMillis();
    }


    private long getReplicaWaitTime() {
        return TimeUnit.SECONDS.toMillis(mapServiceContext.getNodeEngine().getGroupProperties()
                .MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS.getInteger());
    }

    @Override
    public void run() {
        final long now = Clock.currentTimeMillis();
        final MapServiceContext mapServiceContext = this.mapServiceContext;
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final ClusterService clusterService = nodeEngine.getClusterService();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        final Address thisAddress = clusterService.getThisAddress();
        final int partitionCount = partitionService.getPartitionCount();
        Map<Integer, Integer> partitionToEntryCountHolder = Collections.emptyMap();
        List<DelayedEntry> entries = Collections.emptyList();
        boolean createLazy = true;
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            final InternalPartition partition = partitionService.getPartition(partitionId, false);
            final Address owner = partition.getOwnerOrNull();
            final RecordStore recordStore = getRecordStoreOrNull(mapName, partitionId);
            if (owner == null || recordStore == null) {
                // no-op because no owner is set yet.
                // Therefore we don't know anything about the map
                continue;
            }
            final WriteBehindQueue<DelayedEntry> queue = getWriteBehindQueue(recordStore);
            final List<DelayedEntry> delayedEntries = filterItemsLessThanOrEqualToTime(queue, now);
            if (delayedEntries.isEmpty()) {
                continue;
            }
            if (!owner.equals(thisAddress)) {
                if (now < lastRunTime + backupRunIntervalTime) {
                    doInBackup(queue, delayedEntries, partitionId);
                }
                continue;
            }
            // initialize when needed, we do not want
            // to create these on backups for every second.
            if (createLazy) {
                partitionToEntryCountHolder = new HashMap<Integer, Integer>();
                entries = new ArrayList<DelayedEntry>();
                createLazy = false;
            }
            partitionToEntryCountHolder.put(partitionId, delayedEntries.size());
            entries.addAll(delayedEntries);
        }
        if (!entries.isEmpty()) {
            final Map<Integer, List<DelayedEntry>> failsPerPartition = writeBehindProcessor.process(entries);
            removeProcessed(mapName, getEntryPerPartitionMap(entries));
            addFailsToQueue(mapName, failsPerPartition);
            lastRunTime = now;
        }
    }

    private void removeProcessed(String mapName, Map<Integer, List<DelayedEntry>> entryListPerPartition) {
        for (Map.Entry<Integer, List<DelayedEntry>> entry : entryListPerPartition.entrySet()) {
            final int partitionId = entry.getKey();
            final RecordStore recordStore = getRecordStoreOrNull(mapName, partitionId);
            if (recordStore == null) {
                continue;
            }
            final WriteBehindQueue<DelayedEntry> queue = getWriteBehindQueue(recordStore);
            final List<DelayedEntry> entries = entry.getValue();
            queue.removeAll(entries);
        }
    }


    private Map<Integer, List<DelayedEntry>> getEntryPerPartitionMap(List<DelayedEntry> entries) {
        final Map<Integer, List<DelayedEntry>> entryListPerPartition = new HashMap<Integer, List<DelayedEntry>>();
        for (DelayedEntry entry : entries) {
            final int partitionId = entry.getPartitionId();
            List<DelayedEntry> delayedEntries = entryListPerPartition.get(partitionId);
            if (delayedEntries == null) {
                delayedEntries = new ArrayList<DelayedEntry>();
                entryListPerPartition.put(partitionId, delayedEntries);
            }
            delayedEntries.add(entry);
        }
        return entryListPerPartition;
    }

    /**
     * @param queue          write behind queue.
     * @param delayedEntries entries to be processed.
     * @param partitionId    corresponding partition id.
     */
    private void doInBackup(final WriteBehindQueue queue, final List<DelayedEntry> delayedEntries, final int partitionId) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final ClusterService clusterService = nodeEngine.getClusterService();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        final Address thisAddress = clusterService.getThisAddress();
        final InternalPartition partition = partitionService.getPartition(partitionId, false);
        final Address owner = partition.getOwnerOrNull();
        if (owner != null && !owner.equals(thisAddress)) {
            writeBehindProcessor.callBeforeStoreListeners(delayedEntries);
            removeProcessed(mapName, getEntryPerPartitionMap(delayedEntries));
            writeBehindProcessor.callAfterStoreListeners(delayedEntries);
        }
    }

    private RecordStore getRecordStoreOrNull(String mapName, int partitionId) {
        final PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        return partitionContainer.getExistingRecordStore(mapName);
    }

    private void addFailsToQueue(String mapName, Map<Integer, List<DelayedEntry>> failsPerPartition) {
        if (failsPerPartition.isEmpty()) {
            return;
        }
        for (Map.Entry<Integer, List<DelayedEntry>> entry : failsPerPartition.entrySet()) {
            final Integer partitionId = entry.getKey();
            final List<DelayedEntry> fails = failsPerPartition.get(partitionId);
            if (fails == null || fails.isEmpty()) {
                continue;
            }
            final RecordStore recordStore = getRecordStoreOrNull(mapName, partitionId);
            if (recordStore == null) {
                continue;
            }
            final WriteBehindQueue<DelayedEntry> queue = getWriteBehindQueue(recordStore);
            queue.addFront(fails);
        }
    }

    private static List<DelayedEntry> filterItemsLessThanOrEqualToTime(WriteBehindQueue<DelayedEntry> queue,
                                                                       long now) {
        if (queue == null || queue.size() == 0) {
            return Collections.emptyList();
        }

        return queue.filterItems(now);
    }

    private WriteBehindQueue<DelayedEntry> getWriteBehindQueue(RecordStore recordStore) {
        final WriteBehindStore storeManager = (WriteBehindStore) recordStore.getMapDataStore();
        return storeManager.getWriteBehindQueue();
    }
}
