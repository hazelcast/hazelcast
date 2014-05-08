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
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapService;
import com.hazelcast.map.MapStoreWrapper;
import com.hazelcast.map.PartitionContainer;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.writebehind.store.StoreHandlerChain;
import com.hazelcast.map.writebehind.store.StoreHandlers;
import com.hazelcast.map.writebehind.store.StoreListener;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
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
 * TODO add parallel sort.
 * <p/>
 * Write behind queue manager.
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

    private final String mapName;

    private final MapService mapService;

    private final String executorName;

    private final StoreHandlerChain storeHandlerChain;

    private final List<StoreListener> listeners;

    private final ILogger logger;

    WriteBehindQueueManager(String mapName, MapService mapService, MapStoreWrapper storeWrapper) {
        this.scheduledExecutor = getScheduledExecutorService(mapService);
        this.mapName = mapName;
        this.mapService = mapService;
        this.logger = mapService.getNodeEngine().getLogger(WriteBehindQueueManager.class);
        this.executorName = EXECUTOR_NAME_PREFIX + mapName;
        this.listeners = new ArrayList<StoreListener>(2);
        this.storeHandlerChain = StoreHandlers.createHandlers(mapService, storeWrapper, listeners);
        this.processor = new StoreProcessor(mapName, mapService, storeHandlerChain, this);
    }

    @Override
    public void start() {
        scheduledExecutor.scheduleAtFixedRate(processor, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void stop() {
        mapService.getNodeEngine().getExecutionService().shutdownExecutor(executorName);
    }

    @Override
    public void reset() {
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
        final List<DelayedEntry> sortedDelayedEntries = queue.fetchAndRemoveAll();
        Collections.sort(sortedDelayedEntries, DELAYED_ENTRY_COMPARATOR);
        final Map<Integer, Collection<DelayedEntry>> failedStoreOpPerPartition
                = new HashMap<Integer, Collection<DelayedEntry>>();
        storeHandlerChain.process(sortedDelayedEntries, failedStoreOpPerPartition);
        if (failedStoreOpPerPartition.size() > 0) {
            printErrorLog(failedStoreOpPerPartition);
        }
        queue.clear();
        return getDataKeys(sortedDelayedEntries);
    }

    @Override
    public ScheduledExecutorService getScheduler() {
        return scheduledExecutor;
    }

    private void printErrorLog(Map<Integer, Collection<DelayedEntry>> failedsPerPartition) {
        int size = 0;
        final Collection<Collection<DelayedEntry>> values = failedsPerPartition.values();
        for (Collection<DelayedEntry> value : values) {
            size += value.size();
        }
        final String logMessage = String.format("Map store flush operation can not be done for %d entries", size);
        logger.severe(logMessage);
    }

    private static List<DelayedEntry> filterLessThanOrEqualToTime(WriteBehindQueue<DelayedEntry> queue,
                                                                  long time, TimeUnit unit) {
        if (queue == null || queue.size() == 0) {
            return Collections.emptyList();
        }
        final long timeInNanos = unit.toNanos(time);
        List<DelayedEntry> delayedEntries = Collections.emptyList();
        DelayedEntry e;
        int i = 0;
        while ((e = queue.get(i)) != null) {
            if (i == 0) {
                // init when needed.
                delayedEntries = new ArrayList<DelayedEntry>();
            }
            if (e.getStoreTime() <= timeInNanos) {
                delayedEntries.add(e);
            }
            i++;
        }
        return delayedEntries;
    }

    private static void removeProcessed(WriteBehindQueue<DelayedEntry> queue, int numberOfEntriesProcessed) {
        if (queue == null || queue.size() == 0 || numberOfEntriesProcessed < 1) {
            return;
        }
        for (int j = 0; j < numberOfEntriesProcessed; j++) {
            queue.removeFirst();
        }
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

    private ScheduledExecutorService getScheduledExecutorService(MapService mapService) {
        final NodeEngine nodeEngine = mapService.getNodeEngine();
        final ExecutionService executionService = nodeEngine.getExecutionService();
        final String executorName = EXECUTOR_NAME_PREFIX + mapName;
        executionService.register(executorName, 1, EXECUTOR_DEFAULT_QUEUE_CAPACITY, ExecutorType.CACHED);
        return executionService.getScheduledExecutor(executorName);
    }

    private static void removeProcessedEntries(MapService mapService, String mapName,
                                               Map<Integer, Integer> partitionToEntryCountHolder, Map<Integer,
            Collection<DelayedEntry>> failedsPerPartition) {
        for (Map.Entry<Integer, Integer> entry : partitionToEntryCountHolder.entrySet()) {
            final Integer partitionId = entry.getKey();
            final PartitionContainer partitionContainer = mapService.getPartitionContainer(partitionId);
            final RecordStore recordStore = partitionContainer.getExistingRecordStore(mapName);
            if (recordStore == null) {
                continue;
            }
            final WriteBehindQueue<DelayedEntry> queue = recordStore.getWriteBehindQueue();
            removeProcessed(queue, partitionToEntryCountHolder.get(partitionId));
            final Collection<DelayedEntry> faileds = failedsPerPartition.get(partitionId);
            if (faileds == null || faileds.isEmpty()) {
                continue;
            }
            queue.addFront(faileds);

        }
    }

    /**
     * Process store operations.
     */
    private static final class StoreProcessor implements Runnable {

        private long lastRunTimeInNanos = nanoNow();

        private final String mapName;

        private final MapService mapService;

        private final StoreHandlerChain storeHandlerChain;

        private final WriteBehindManager writeBehindManager;

        private final long backupWorkIntervalTimeInNanos;

        private StoreProcessor(String mapName, MapService mapService,
                               StoreHandlerChain storeHandlerChain, WriteBehindManager manager) {
            this.mapName = mapName;
            this.mapService = mapService;
            this.storeHandlerChain = storeHandlerChain;
            this.writeBehindManager = manager;
            this.backupWorkIntervalTimeInNanos = getReplicaWaitTimeInNanaos();

        }

        private long getReplicaWaitTimeInNanaos(){
            return TimeUnit.SECONDS.toNanos(mapService.getNodeEngine().getGroupProperties()
                    .MAP_REPLICA_WAIT_SECONDS_FOR_SCHEDULED_TASKS.getInteger());
        }

        @Override
        public void run() {
            final long now = nanoNow();
            final MapService mapService = this.mapService;
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
                if (owner == null) {
                    // no-op because no owner is set yet.
                    // Therefore we don't know anything about the map
                    continue;
                }
                final PartitionContainer partitionContainer = mapService.getPartitionContainer(partitionId);
                final RecordStore recordStore = partitionContainer.getExistingRecordStore(mapName);
                if (recordStore == null) {
                    continue;
                }
                final WriteBehindQueue<DelayedEntry> queue = recordStore.getWriteBehindQueue();
                final List<DelayedEntry> delayedEntries = filterLessThanOrEqualToTime(queue,
                        now, TimeUnit.NANOSECONDS);
                if (!owner.equals(thisAddress) && now > lastRunTimeInNanos + backupWorkIntervalTimeInNanos) {
                    doInBackup(queue, delayedEntries, partitionId);
                    lastRunTimeInNanos = now;
                }
                if (delayedEntries.size() == 0) {
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
            final Map<Integer, Collection<DelayedEntry>> failedsPerPartition = new HashMap<Integer, Collection<DelayedEntry>>();
            storeHandlerChain.process(sortedDelayedEntries, failedsPerPartition);
            removeProcessedEntries(mapService, mapName, partitionToEntryCountHolder, failedsPerPartition);
            writeBehindManager.reset();
        }

        /**
         * @param queue
         * @param delayedEntries
         * @param partitionId
         */
        private void doInBackup(final WriteBehindQueue queue, final List<DelayedEntry> delayedEntries, final int partitionId) {

            final NodeEngine nodeEngine = mapService.getNodeEngine();
            final ClusterService clusterService = nodeEngine.getClusterService();
            final InternalPartitionService partitionService = nodeEngine.getPartitionService();
            final Address thisAddress = clusterService.getThisAddress();
            final InternalPartition partition = partitionService.getPartition(partitionId);
            final Address owner = partition.getOwnerOrNull();
            if (!owner.equals(thisAddress)) {
                storeHandlerChain.callBeforeStoreListeners(delayedEntries);
                removeProcessed(queue, delayedEntries.size());
                storeHandlerChain.callAfterStoreListeners(delayedEntries);
            }

        }

        private static long nanoNow() {
            return System.nanoTime();
        }
    }

}



