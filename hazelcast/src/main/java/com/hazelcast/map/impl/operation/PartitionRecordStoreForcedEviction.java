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
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.eviction.Evictor;
import com.hazelcast.memory.NativeOutOfMemoryError;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.Objects;
import java.util.stream.IntStream;

import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static java.lang.String.format;

/**
 * An {@link Eviction} operation that attempts to evict entries from current thread partition
 * of an {@link com.hazelcast.map.impl.recordstore.RecordStore}
 */
class PartitionRecordStoreForcedEviction implements Eviction {
    private final int retries;
    private final ILogger logger;
    private final MapOperation mapOperation;
    private boolean successful;

    PartitionRecordStoreForcedEviction(int retries, ILogger logger, MapOperation mapOperation) {
        this.retries = retries;
        this.logger = logger;
        this.mapOperation = mapOperation;
    }

    @Override
    public void execute() {
        for (int i = 0; i < retries; i++) {
            try {
                if (logger.isFineEnabled()) {
                    logger.fine(format("Applying forced eviction on other RecordStores owned by the same partition thread"
                                           + " (map %s, partitionId: %d", mapOperation.getName(), mapOperation.getPartitionId()));
                }
                NodeEngine nodeEngine = mapOperation.getNodeEngine();
                int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
                OperationService operationService = nodeEngine.getOperationService();
                int threadCount = operationService.getPartitionThreadCount();
                int mod = mapOperation.getPartitionId() % threadCount;

                IntStream.range(0, partitionCount)
                    .filter(partitionId -> partitionId % threadCount == mod)
                    .mapToObj(partitionId -> mapOperation.mapServiceContext.getPartitionContainer(
                            partitionId).getMaps())
                    .flatMap(maps -> maps.values().stream())
                    .filter(Objects::nonNull)
                    .filter(recordStore -> recordStore.getInMemoryFormat() == NATIVE
                        && recordStore.getEvictionPolicy() != NONE)
                    .forEach(recordStore -> {
                        MapContainer mapContainer = recordStore.getMapContainer();
                        Evictor evictor = mapContainer.getEvictor();
                        evictor.forceEvict(recordStore);
                    });
                mapOperation.runInternal();
                successful = true;
                return;
            } catch (NativeOutOfMemoryError e) {
                ignore(e);
            }
        }
    }

    @Override
    public boolean isSuccessful() {
        return successful;
    }
}
