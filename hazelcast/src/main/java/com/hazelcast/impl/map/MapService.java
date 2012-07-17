/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.map;

import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.impl.spi.NodeService;
import com.hazelcast.nio.Address;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class MapService {
    public final static String MAP_SERVICE_NAME = "mapService";

    final PartitionContainer[] partitionContainers;
    private NodeService nodeService;

    public MapService(NodeService nodeService, PartitionInfo[] partitions) {
        this.nodeService = nodeService;
        int partitionCount = nodeService.getNode().groupProperties.CONCURRENT_MAP_PARTITION_COUNT.getInteger();
        partitionContainers = new PartitionContainer[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            partitionContainers[i] = new PartitionContainer(nodeService.getNode(), partitions[i]);
        }
    }

    public Future backup(PutBackupOperation putBackupOperation, Address target) throws Exception {
        return nodeService.invoke(MAP_SERVICE_NAME, putBackupOperation, target);
    }

    public PartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    public MapPartition getMapPartition(int partitionId, String mapName) {
        return getPartitionContainer(partitionId).getMapPartition(mapName);
    }

    final AtomicLong counter = new AtomicLong();

    public long nextId() {
        return counter.incrementAndGet();
    }
}
