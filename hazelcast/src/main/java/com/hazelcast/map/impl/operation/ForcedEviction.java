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

package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.config.InMemoryFormat.NATIVE;

/**
 * Forced eviction is done per record store basis. This interface is
 * the main contract to implement various forced eviction strategies.
 *
 * @see SingleRecordStoreForcedEviction
 * @see MultipleRecordStoreForcedEviction
 */
interface ForcedEviction {

    int EVICTION_RETRY_COUNT = 5;

    double TWENTY_PERCENT = 0.2D;

    double HUNDRED_PERCENT = 1D;

    double[] EVICTION_PERCENTAGES = {TWENTY_PERCENT, HUNDRED_PERCENT};

    ForcedEviction[] EVICTION_STRATEGIES = {new SingleRecordStoreForcedEviction(),
            new MultipleRecordStoreForcedEviction()};

    static void runWithForcedEvictionStrategies(MapOperation operation) {
        for (double evictionPercentage : EVICTION_PERCENTAGES) {
            for (ForcedEviction evictionStrategy : EVICTION_STRATEGIES) {
                if (evictionStrategy.forceEvictAndRun(operation, evictionPercentage)) {
                    return;
                }
            }
        }
    }

    /**
     * First does forced eviction by deleting a percentage
     * of entries then tries to run provided map operation.
     *
     * @param mapOperation       the map operation which got Native OOME during its run
     * @param evictionPercentage percentage of the entries to evict from record store.
     * @return {@code true} if run is succeeded after forced eviction,
     * otherwise return {@code false}
     * @throws com.hazelcast.memory.NativeOutOfMemoryError
     */
    boolean forceEvictAndRun(MapOperation mapOperation,
                             double evictionPercentage);

    /**
     * @return {@code true} if supplied record store is valid
     * to apply forced eviction, otherwise return {@code false}
     */
    static boolean isValid(RecordStore recordStore) {
        return recordStore != null && recordStore.getInMemoryFormat() == NATIVE
                && recordStore.getEvictionPolicy() != NONE && recordStore.size() > 0;
    }

    default int mod(MapOperation mapOperation, int threadCount) {
        return mapOperation.getPartitionId() % threadCount;
    }

    default int threadCount(MapOperation mapOperation) {
        NodeEngine nodeEngine = mapOperation.getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();
        return operationService.getPartitionThreadCount();
    }

    default int numberOfPartitions(MapOperation mapOperation) {
        NodeEngine nodeEngine = mapOperation.getNodeEngine();
        return nodeEngine.getPartitionService().getPartitionCount();
    }

    default ConcurrentMap<String, RecordStore> partitionMaps(MapOperation mapOperation, int partitionId) {
        return mapOperation.mapServiceContext.getPartitionContainer(partitionId).getMaps();
    }

    /**
     * @return 1 if evictionPercentage is 1. 1 means we are evicting
     * all data in record store and further retrying has no point, otherwise
     * return {@link #EVICTION_RETRY_COUNT}
     */
    default int retryCount(double evictionPercentage) {
        return evictionPercentage == 1D ? 1 : EVICTION_RETRY_COUNT;
    }
}
