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

import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.operation.WriteBehindCleanBackupOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Responsible for sending operations which will remove stored entries from backups.
 * Operations will be delayed according to two criterias and will be send when one of the criteria below is met:
 * <ul>
 * <li>If the total operation count is reached to {@value #STORE_OPERATION_COUNT}</li>
 * <li>If {@link #getReplicaWaitTime()} is elapsed</li>
 * </ul>
 */
class WriteBehindBackupPartitionCleaner {

    private static final int MAX_RETRY_COUNT = 10;
    private static final long STORE_OPERATION_COUNT = 32;

    private final AtomicLong totalStoreOperationCount;
    private final MapStoreContext mapStoreContext;
    private volatile Set<Integer> partitionIds;
    private volatile long lastRuntime;

    WriteBehindBackupPartitionCleaner(MapStoreContext mapStoreContext) {
        this.mapStoreContext = mapStoreContext;
        this.partitionIds = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
        this.totalStoreOperationCount = new AtomicLong(0);
    }

    void add(int partitionId) {
        partitionIds.add(partitionId);
        totalStoreOperationCount.incrementAndGet();
    }


    void removeFromBackups() {
        Set<Integer> partitionIds = this.partitionIds;
        if (partitionIds.isEmpty()) {
            return;
        }

        boolean sendable = false;
        int tryCount = 0;
        while (true) {
            if (tryCount >= MAX_RETRY_COUNT) {
                break;
            }

            tryCount++;
            long operationCount = totalStoreOperationCount.get();
            if (operationCount >= STORE_OPERATION_COUNT
                    || isInSendableTimeWindow()) {
                sendable = totalStoreOperationCount.compareAndSet(operationCount, 0);
            } else {
                break;
            }

            if (sendable) {
                break;
            }
        }

        if (!sendable) {
            return;
        }

        sendReplicaWriteBehindQueueClearOperation(partitionIds);
        lastRuntime = System.nanoTime();
    }


    private boolean isInSendableTimeWindow() {
        long nanoNow = System.nanoTime();
        long elapsedNanoTime = nanoNow - lastRuntime;
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(elapsedNanoTime);
        long replicaWaitTimeInMillis = getReplicaWaitTime();
        return elapsedMillis >= replicaWaitTimeInMillis;
    }

    private long getReplicaWaitTime() {
        MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
        return TimeUnit.SECONDS.toMillis(mapServiceContext.getNodeEngine().getGroupProperties()
                .MAP_REPLICA_SCHEDULED_TASK_DELAY_SECONDS.getInteger());
    }

    private void sendReplicaWriteBehindQueueClearOperation(Set<Integer> partitionIds) {
        int backupCount = getBackupCount();
        if (backupCount < 1) {
            return;
        }

        for (Iterator<Integer> iterator = partitionIds.iterator(); iterator.hasNext(); ) {
            Integer partitionId = iterator.next();
            iterator.remove();

            MapDataStore mapDataStore = mapStoreContext.getMapStoreManager().getMapDataStore(partitionId);
            WriteBehindStore store = (WriteBehindStore) mapDataStore;
            Sequencer sequencer = store.getWriteBehindQueue().getSequencer();

            NodeEngine nodeEngine = mapStoreContext.getMapServiceContext().getNodeEngine();
            OperationService operationService = nodeEngine.getOperationService();

            InternalPartitionService partitionService = nodeEngine.getPartitionService();
            InternalPartition partition = partitionService.getPartition(partitionId);

            for (int i = 1; i <= backupCount; i++) {
                Address target = partition.getReplicaAddress(i);
                if (target == null) {
                    continue;
                }

                Operation operation = createOperation(sequencer);
                operationService.createInvocationBuilder(MapService.SERVICE_NAME,
                        operation, partitionId)
                        .setReplicaIndex(i)
                        .invoke();
            }
        }
    }

    private int getBackupCount() {
        MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
        String mapName = mapStoreContext.getMapName();
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        return mapContainer.getBackupCount() + mapContainer.getAsyncBackupCount();
    }

    private Operation createOperation(Sequencer sequencer) {
        String mapName = mapStoreContext.getMapName();
        return new WriteBehindCleanBackupOperation(mapName, sequencer);
    }
}

