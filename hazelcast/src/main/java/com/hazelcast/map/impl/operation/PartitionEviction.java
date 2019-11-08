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

import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.config.EvictionPolicy.NONE;
import static com.hazelcast.config.InMemoryFormat.NATIVE;

abstract class PartitionEviction implements Eviction {

    boolean nativeFormatWithEvictionPolicy(RecordStore recordStore) {
        return recordStore.getInMemoryFormat() == NATIVE
            && recordStore.getEvictionPolicy() != NONE;
    }

    int mod(MapOperation mapOperation, int threadCount) {
        return mapOperation.getPartitionId() % threadCount;
    }

    int threadCount(MapOperation mapOperation) {
        NodeEngine nodeEngine = mapOperation.getNodeEngine();
        OperationService operationService = nodeEngine.getOperationService();
        return operationService.getPartitionThreadCount();
    }

    int numberOfPartitions(MapOperation mapOperation) {
        NodeEngine nodeEngine = mapOperation.getNodeEngine();
        return nodeEngine.getPartitionService().getPartitionCount();
    }

    ConcurrentMap<String, RecordStore> partitionMaps(MapOperation mapOperation, int partitionId) {
        return mapOperation.mapServiceContext.getPartitionContainer(partitionId).getMaps();
    }
}
