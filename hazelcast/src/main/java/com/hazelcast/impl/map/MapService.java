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
import com.hazelcast.impl.spi.ServiceLifecycle;
import com.hazelcast.impl.spi.ServiceMigrationOperation;

import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

public class MapService implements ServiceLifecycle {
    public final static String MAP_SERVICE_NAME = "mapService";

    private final AtomicLong counter = new AtomicLong();
    private final PartitionContainer[] partitionContainers;
    private final NodeService nodeService;

    public MapService(NodeService nodeService, PartitionInfo[] partitions) {
        this.nodeService = nodeService;
        int partitionCount = nodeService.getPartitionCount();
        partitionContainers = new PartitionContainer[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            partitionContainers[i] = new PartitionContainer(nodeService.getNode(), partitions[i]);
        }

        new Thread() {
            public void run() {
                while (true) {
                    int k = 0;
                    for (PartitionContainer partitionContainer : partitionContainers) {
                        if (!partitionContainer.partitionInfo.isOwnerOrBackup(
                                MapService.this.nodeService.getThisAddress(), 1)) {
                            partitionContainer.maps.clear();
                            continue;
                        }
                        for (Entry<String, MapPartition> entry : partitionContainer.maps.entrySet()) {
                            k += entry.getValue().records.size();
                        }
                    }
                    System.err.println("Total: " + k);
                    try {
                        sleep(5000);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }.start();

    }

    public PartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    public MapPartition getMapPartition(int partitionId, String mapName) {
        return getPartitionContainer(partitionId).getMapPartition(mapName);
    }

    public long nextId() {
        return counter.incrementAndGet();
    }

    public ServiceMigrationOperation getMigrationTask(final int partitionId, final int replicaIndex, boolean diffOnly) {
        if (partitionId < 0 || partitionId >= nodeService.getPartitionCount()) {
            return null;
        }
        final PartitionContainer container = partitionContainers[partitionId];
        return new MapMigrationOperation(container, partitionId, replicaIndex, diffOnly);
    }

}
