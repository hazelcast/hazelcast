/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.operation;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.stream.IntStream;

import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.config.InMemoryFormat.NATIVE;

/**
 * An {@link Eviction} operation that attempts to evict all an {@link com.hazelcast.map.IMap} partition entries
 * of current thread
 */
class PartitionAllEntriesEviction implements Eviction {
    private final ILogger logger;
    private final MapOperation mapOperation;
    private boolean successful;

    PartitionAllEntriesEviction(ILogger logger, MapOperation mapOperation) {
        this.logger = logger;
        this.mapOperation = mapOperation;
    }

    @Override
    public void execute() {
        successful = false;
        if (logger.isInfoEnabled()) {
            logger.info("Evicting all entries in other RecordStores owned by the same partition thread"
                            + " because forced eviction was not enough!");
        }

        boolean isBackup = mapOperation instanceof BackupOperation;
        NodeEngine nodeEngine = mapOperation.getNodeEngine();
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        OperationService operationService = nodeEngine.getOperationService();
        int threadCount = operationService.getPartitionThreadCount();
        int mod = mapOperation.getPartitionId() % threadCount;

        IntStream.range(0, partitionCount)
            .filter(partitionId -> partitionId % threadCount == mod)
            .mapToObj(partitionId ->
                          mapOperation.mapServiceContext.getPartitionContainer(partitionId).getMaps())
            .flatMap(map -> map.values().stream())
            .filter(recordStore -> recordStore.getInMemoryFormat() == NATIVE
                    && recordStore.getEvictionPolicy() != NONE)
            .forEach(recordStore -> {
                recordStore.evictAll(isBackup);
                recordStore.disposeDeferredBlocks();
            });
        mapOperation.runInternal();
        successful = true;
    }

    @Override
    public boolean isSuccessful() {
        return successful;
    }
}
