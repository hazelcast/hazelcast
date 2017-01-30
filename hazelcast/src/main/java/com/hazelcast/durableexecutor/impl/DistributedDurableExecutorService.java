/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.durableexecutor.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.partition.MigrationEndpoint;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DistributedDurableExecutorService implements ManagedService, RemoteService, MigrationAwareService {

    public static final String SERVICE_NAME = "hz:impl:durableExecutorService";

    private final NodeEngineImpl nodeEngine;
    private final DurableExecutorPartitionContainer[] partitionContainers;

    private final Set<String> shutdownExecutors
            = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    public DistributedDurableExecutorService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        partitionContainers = new DurableExecutorPartitionContainer[partitionCount];
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            partitionContainers[partitionId] = new DurableExecutorPartitionContainer(nodeEngine, partitionId);
        }
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    public DurableExecutorPartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public void reset() {
        shutdownExecutors.clear();
        for (int partitionId = 0; partitionId < partitionContainers.length; partitionId++) {
            partitionContainers[partitionId] = new DurableExecutorPartitionContainer(nodeEngine, partitionId);
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        return new DurableExecutorServiceProxy(nodeEngine, this, name);
    }

    @Override
    public void destroyDistributedObject(String name) {
        shutdownExecutors.remove(name);
        nodeEngine.getExecutionService().shutdownDurableExecutor(name);
    }

    public void shutdownExecutor(String name) {
        nodeEngine.getExecutionService().shutdownDurableExecutor(name);
        shutdownExecutors.add(name);
    }

    public boolean isShutdown(String name) {
        return shutdownExecutors.contains(name);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        DurableExecutorPartitionContainer partitionContainer = partitionContainers[partitionId];
        return partitionContainer.prepareReplicationOperation(event.getReplicaIndex());
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearRingBuffersHavingLesserBackupCountThan(partitionId, event.getNewReplicaIndex());
        } else if (event.getNewReplicaIndex() == 0) {
            DurableExecutorPartitionContainer partitionContainer = partitionContainers[partitionId];
            partitionContainer.executeAll();
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearRingBuffersHavingLesserBackupCountThan(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
    }

    private void clearRingBuffersHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        DurableExecutorPartitionContainer partitionContainer = partitionContainers[partitionId];
        partitionContainer.clearRingBuffersHavingLesserBackupCountThan(thresholdReplicaIndex);
    }
}
