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

package com.hazelcast.map.writebehind;


import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.MapStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapService;
import com.hazelcast.map.PartitionContainer;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.writebehind.store.MapStoreManager;
import com.hazelcast.map.writebehind.store.MapStoreManagers;
import com.hazelcast.map.writebehind.store.StoreListener;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;
import com.hazelcast.util.executor.ExecutorType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Write behind queue(WBQ) manager which is used by
 * {@link com.hazelcast.map.MapContainer} to control
 * write behind queues. Provides co-operation between
 * WBQ and {@link com.hazelcast.map.writebehind.store.MapStoreManager}
 */
class WriteBehindQueueManager implements WriteBehindManager {

    private static final String EXECUTOR_NAME_PREFIX = "hz:scheduled:mapstore:";

    private static final int EXECUTOR_DEFAULT_QUEUE_CAPACITY = 10000;

    private static final Comparator<DelayedEntry> DELAYED_ENTRY_COMPARATOR = new Comparator<DelayedEntry>() {
        @Override
        public int compare(DelayedEntry o1, DelayedEntry o2) {
            final long s1 = o1.getStoreTime();
            final long s2 = o2.getStoreTime();
            return (s1 < s2) ? -1 : ((s1 == s2) ? 0 : 1);
        }
    };

    private final ScheduledExecutorService scheduledExecutor;

    private final StoreProcessor processor;

    private final MapService mapService;

    private final MapStoreManager<DelayedEntry> mapStoreManager;

    private final List<StoreListener> listeners;

    private final ILogger logger;

    WriteBehindQueueManager(String mapName, MapService mapService, MapStore mapStore, MapStoreConfig mapStoreConfig) {
        this.scheduledExecutor = getScheduledExecutorService(mapName, mapService);
        this.mapService = mapService;
        this.logger = mapService.getNodeEngine().getLogger(WriteBehindQueueManager.class);
        this.listeners = new ArrayList<StoreListener>(2);
        this.mapStoreManager = MapStoreManagers.newMapStoreManager(mapService, mapStore, listeners);
        this.processor = new StoreProcessor(mapName, mapService, mapStoreManager, mapStoreConfig);
    }

    @Override
    public void start() {
        scheduledExecutor.scheduleAtFixedRate(processor, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        scheduledExecutor.shutdown();
    }

    @Override
    public void addStoreListener(StoreListener storeListener) {
        listeners.add(storeListener);
    }

    @Override
    public Collection<Data> flush(WriteBehindQueue<DelayedEntry> queue) {
        if (queue.size() == 0) {
            return Collections.emptyList();
        }
        final List<DelayedEntry> sortedDelayedEntries = queue.removeAll();
        return flush0(sortedDelayedEntries);
    }

    private Collection<Data> flush0(List<DelayedEntry> delayedEntries) {
        Collections.sort(delayedEntries, DELAYED_ENTRY_COMPARATOR);
        final Map<Integer, List<DelayedEntry>> failedStoreOpPerPartition = mapStoreManager.process(delayedEntries);
        if (failedStoreOpPerPartition.size() > 0) {
            printErrorLog(failedStoreOpPerPartition);
        }
        return getDataKeys(delayedEntries);
    }

    private List<Data> getDataKeys(final List<DelayedEntry> sortedDelayedEntries) {
        if (sortedDelayedEntries == null || sortedDelayedEntries.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Data> keys = new ArrayList<Data>(sortedDelayedEntries.size());
        for (DelayedEntry entry : sortedDelayedEntries) {
            // TODO Key type always should be Data. No need to call mapService.toData but check first if it is.
            keys.add(mapService.toData(entry.getKey()));
        }
        return keys;
    }

    @Override
    public ScheduledExecutorService getScheduler() {
        return scheduledExecutor;
    }

    private void printErrorLog(Map<Integer, List<DelayedEntry>> failsPerPartition) {
        int size = 0;
        final Collection<List<DelayedEntry>> values = failsPerPartition.values();
        for (Collection<DelayedEntry> value : values) {
            size += value.size();
        }
        final String logMessage = String.format("Map store flush operation can not be done for %d entries", size);
        logger.severe(logMessage);
    }

    private static List<DelayedEntry> filterItemsLessThanOrEqualToTime(WriteBehindQueue<DelayedEntry> queue,
                                                                       long now) {
        if (queue == null || queue.size() == 0) {
            return Collections.emptyList();
        }
        List<DelayedEntry> delayedEntries = Collections.emptyList();
        DelayedEntry e;
        int i = 0;
        while ((e = queue.get(i)) != null) {
            if (i == 0) {
                // init when needed.
                delayedEntries = new ArrayList<DelayedEntry>();
            }
            if (e.getStoreTime() <= now) {
                delayedEntries.add(e);
            }
            i++;
        }
        return delayedEntries;
    }

    private ScheduledExecutorService getScheduledExecutorService(String mapName, MapService mapService) {
        final NodeEngine nodeEngine = mapService.getNodeEngine();
        final ExecutionService executionService = nodeEngine.getExecutionService();
        final String executorName = EXECUTOR_NAME_PREFIX + mapName;
        executionService.register(executorName, 1, EXECUTOR_DEFAULT_QUEUE_CAPACITY, ExecutorType.CACHED);
        return executionService.getScheduledExecutor(executorName);
    }

    /**
     * Process store operations.
     */
    private static final class StoreProcessor implements Runnable {

        private final String mapName;

        private final MapService mapService;

        private final MapStoreManager mapStoreManager;

        /**
         * Run on backup nodes after this interval.
         */
        private final long backupRunIntervalTime;

        /**
         * Last run time of this processor.
         */
        private long lastRunTime;

        /**
         * The number of operations to be included in each batch processing round.
         */
        private final int writeBatchSize;


        private StoreProcessor(String mapName, MapService mapService,
                               MapStoreManager mapStoreManager, MapStoreConfig mapStoreConfig) {
            this.mapName = mapName;
            this.mapService = mapService;
            this.mapStoreManager = mapStoreManager;
            this.backupRunIntervalTime = getReplicaWaitTime();
            this.lastRunTime = Clock.currentTimeMillis();
            this.writeBatchSize = mapStoreConfig.getWriteBatchSize();

        }

        private long getReplicaWaitTime() {
            return TimeUnit.SECONDS.toMillis(mapService.getNodeEngine().getGroupProperties()
                    .MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS.getInteger());
        }

        @Override
        public void run() {
            final MapService mapService = this.mapService;
            final long now = Clock.currentTimeMillis();
            final NodeEngine nodeEngine = mapService.getNodeEngine();
            final ClusterService clusterService = nodeEngine.getClusterService();
            final InternalPartitionService partitionService = nodeEngine.getPartitionService();
            final Address thisAddress = clusterService.getThisAddress();
            final int partitionCount = partitionService.getPartitionCount();
            Map<Integer, Integer> partitionToEntryCountHolder = Collections.emptyMap();
            List<DelayedEntry> sortedDelayedEntries = Collections.emptyList();
            boolean createLazy = true;
            for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
                final InternalPartition partition = partitionService.getPartition(partitionId);
                final Address owner = partition.getOwnerOrNull();
                RecordStore recordStore;
                if (owner == null || (recordStore = getRecordStoreOrNull(mapName, partitionId)) == null) {
                    // no-op because no owner is set yet.
                    // Therefore we don't know anything about the map
                    continue;
                }
                final WriteBehindQueue<DelayedEntry> queue = recordStore.getWriteBehindQueue();
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
                    sortedDelayedEntries = new ArrayList<DelayedEntry>();
                    createLazy = false;
                }
                partitionToEntryCountHolder.put(partitionId, delayedEntries.size());
                sortedDelayedEntries.addAll(delayedEntries);
            }
            if (sortedDelayedEntries.isEmpty()) {
                return;
            }
            Collections.sort(sortedDelayedEntries, DELAYED_ENTRY_COMPARATOR);
            if (writeBatchSize > 1) {
                doStoreUsingBatchSize(sortedDelayedEntries, partitionToEntryCountHolder);
            } else {
                doStore(sortedDelayedEntries, partitionToEntryCountHolder);
            }
            lastRunTime = now;
        }

        /**
         * Store chunk by chunk using write batch size {@link StoreProcessor#writeBatchSize}
         *
         * @param sortedDelayedEntries entries to be stored.
         * @param countHolder          holds per partition count of candidate entries to be processed.
         */
        private void doStoreUsingBatchSize(List<DelayedEntry> sortedDelayedEntries, Map<Integer, Integer> countHolder) {
            int page = 0;
            List<DelayedEntry> delayedEntryList;
            while ((delayedEntryList = getBatchChunk(sortedDelayedEntries, writeBatchSize, page++)) != null) {
                doStore(delayedEntryList, countHolder);
            }
        }

        /**
         * Store dummy. Tries to store in one processing round.
         *
         * @param sortedDelayedEntries entries to be stored.
         * @param countHolder          holds per partition count of candidate entries to be processed.
         */
        private void doStore(List<DelayedEntry> sortedDelayedEntries, Map<Integer, Integer> countHolder) {
            final Map<Integer, List<DelayedEntry>> failsPerPartition = mapStoreManager.process(sortedDelayedEntries);
            removeProcessedEntries(mapName, countHolder);
            addFailsToQueue(mapName, failsPerPartition);
        }

        /**
         * Used to partition the list to chunks.
         *
         * @param list        to be paged.
         * @param batchSize   batch operation size.
         * @param chunkNumber batch chunk number.
         * @return sub-list of list if any or null.
         */
        private List<DelayedEntry> getBatchChunk(List<DelayedEntry> list, int batchSize, int chunkNumber) {
            if (list == null || list.isEmpty()) {
                return null;
            }
            final int start = chunkNumber * batchSize;
            final int end = Math.min(start + batchSize, list.size());
            if (start >= end) {
                return null;
            }
            return list.subList(start, end);
        }

        /**
         * @param queue          write behind queue.
         * @param delayedEntries entries to be processed.
         * @param partitionId    corresponding partition id.
         */
        private void doInBackup(final WriteBehindQueue queue, final List<DelayedEntry> delayedEntries, final int partitionId) {
            final NodeEngine nodeEngine = mapService.getNodeEngine();
            final ClusterService clusterService = nodeEngine.getClusterService();
            final InternalPartitionService partitionService = nodeEngine.getPartitionService();
            final Address thisAddress = clusterService.getThisAddress();
            final InternalPartition partition = partitionService.getPartition(partitionId);
            final Address owner = partition.getOwnerOrNull();
            if (owner != null && !owner.equals(thisAddress)) {
                mapStoreManager.callBeforeStoreListeners(delayedEntries);
                removeProcessed(queue, delayedEntries.size());
                mapStoreManager.callAfterStoreListeners(delayedEntries);
            }
        }

        private void removeProcessedEntries(String mapName, Map<Integer, Integer> partitionToEntryCountHolder) {
            for (Map.Entry<Integer, Integer> entry : partitionToEntryCountHolder.entrySet()) {
                final Integer partitionId = entry.getKey();
                final RecordStore recordStore = getRecordStoreOrNull(mapName, partitionId);
                if (recordStore == null) {
                    continue;
                }
                final WriteBehindQueue<DelayedEntry> queue = recordStore.getWriteBehindQueue();
                removeProcessed(queue, partitionToEntryCountHolder.get(partitionId));
            }
        }

        private RecordStore getRecordStoreOrNull(String mapName, int partitionId) {
            final PartitionContainer partitionContainer = mapService.getPartitionContainer(partitionId);
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
                final WriteBehindQueue<DelayedEntry> queue = recordStore.getWriteBehindQueue();
                Collections.sort(fails, DELAYED_ENTRY_COMPARATOR);
                queue.addFront(fails);
            }
        }

        private void removeProcessed(WriteBehindQueue<DelayedEntry> queue, int numberOfEntriesProcessed) {
            if (queue == null || queue.size() == 0 || numberOfEntriesProcessed < 1) {
                return;
            }

            for (int j = 0; j < numberOfEntriesProcessed; j++) {
                queue.removeFirst();
            }
        }

    }

}



