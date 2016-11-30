/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.partition.MigrationEndpoint;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DistributedScheduledExecutorService
        implements ManagedService, RemoteService, MigrationAwareService {

    public static final String SERVICE_NAME = "hz:impl:scheduledExecutorService";

    private static final int MEMBER_BIN = -1;

    private NodeEngine nodeEngine;

    private ScheduledExecutorPartition[] partitions;

    private ScheduledExecutorMemberBin memberBin;

    private final ConcurrentMap<String, Boolean> shutdownExecutors
            = new ConcurrentHashMap<String, Boolean>();

    public DistributedScheduledExecutorService() {
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.nodeEngine = nodeEngine;
        this.partitions = new ScheduledExecutorPartition[partitionCount];
        reset();
    }

    public ScheduledExecutorPartition getPartition(int partitionId) {
        return partitions[partitionId];
    }

    public ScheduledExecutorContainerHolder getPartitionOrMemberBin(int id) {
        if (id == MEMBER_BIN) {
            return memberBin;
        }

        return getPartition(id);
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public void reset() {
        shutdown(true);

        memberBin = new ScheduledExecutorMemberBin(nodeEngine);

        for (int partitionId = 0; partitionId < partitions.length; partitionId++) {
            if (partitions[partitionId] != null) {
                partitions[partitionId].destroy();
            }
            partitions[partitionId] = new ScheduledExecutorPartition(nodeEngine, partitionId);
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        shutdownExecutors.clear();

        if (memberBin != null) {
            memberBin.destroy();
         }

        for (int partitionId = 0; partitionId < partitions.length; partitionId++) {
            if (partitions[partitionId] != null) {
                partitions[partitionId].destroy();
            }
        }
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        return new ScheduledExecutorServiceProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name) {
        if (shutdownExecutors.remove(name) != null) {
            ((InternalExecutionService) nodeEngine.getExecutionService()).shutdownDurableExecutor(name);
        }
    }

    public void shutdownExecutor(String name) {
        if (shutdownExecutors.putIfAbsent(name, Boolean.TRUE) == null) {
            ((InternalExecutionService) nodeEngine.getExecutionService()).shutdownDurableExecutor(name);
        }
    }

    public boolean isShutdown(String name) {
        return shutdownExecutors.containsKey(name);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        ScheduledExecutorPartition partition = partitions[partitionId];
        return partition.prepareReplicationOperation(event.getReplicaIndex());
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            discardStash(partitionId, event.getNewReplicaIndex());
        } else if (event.getNewReplicaIndex() == 0) {
            ScheduledExecutorPartition partition = partitions[partitionId];
            partition.promoteStash();
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            discardStash(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
    }

    private void discardStash(int partitionId, int thresholdReplicaIndex) {
        ScheduledExecutorPartition partition = partitions[partitionId];
        partition.disposeObsoleteReplicas(thresholdReplicaIndex);
    }
}
