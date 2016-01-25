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

package com.hazelcast.concurrent.semaphore;

import com.hazelcast.concurrent.semaphore.operations.SemaphoreDeadMemberOperation;
import com.hazelcast.concurrent.semaphore.operations.SemaphoreReplicationOperation;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.ClientAwareService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.util.ConstructorFunction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getPartitionKey;
import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createEmptyResponseHandler;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

public class SemaphoreService implements ManagedService, MigrationAwareService, MembershipAwareService, RemoteService,
        ClientAwareService {

    public static final String SERVICE_NAME = "hz:impl:semaphoreService";

    private final ConcurrentMap<String, SemaphoreContainer> containers = new ConcurrentHashMap<String, SemaphoreContainer>();

    private final ConstructorFunction<String, SemaphoreContainer> containerConstructor
            = new ConstructorFunction<String, SemaphoreContainer>() {
        @Override
        public SemaphoreContainer createNew(String name) {
            SemaphoreConfig config = nodeEngine.getConfig().findSemaphoreConfig(name);
            InternalPartitionService partitionService = nodeEngine.getPartitionService();
            int partitionId = partitionService.getPartitionId(getPartitionKey(name));
            return new SemaphoreContainer(partitionId, new SemaphoreConfig(config));
        }
    };
    private final NodeEngine nodeEngine;

    public SemaphoreService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public SemaphoreContainer getSemaphoreContainer(String name) {
        return getOrPutIfAbsent(containers, name, containerConstructor);
    }

    // just for testing
    public boolean containsSemaphore(String name) {
        return containers.containsKey(name);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
        containers.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        containers.clear();
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        String caller = event.getMember().getUuid();
        onOwnerDisconnected(caller);
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
    }

    private void onOwnerDisconnected(final String caller) {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        OperationService operationService = nodeEngine.getOperationService();

        for (String name : containers.keySet()) {
            int partitionId = partitionService.getPartitionId(getPartitionKey(name));
            InternalPartition partition = partitionService.getPartition(partitionId);
            if (partition.isLocal()) {
                Operation op = new SemaphoreDeadMemberOperation(name, caller)
                        .setPartitionId(partitionId)
                        .setOperationResponseHandler(createEmptyResponseHandler())
                        .setService(this)
                        .setNodeEngine(nodeEngine)
                        .setServiceName(SERVICE_NAME);
                operationService.executeOperation(op);
            }
        }
    }

    @Override
    public SemaphoreProxy createDistributedObject(String objectId) {
        return new SemaphoreProxy(objectId, this, nodeEngine);
    }

    @Override
    public void destroyDistributedObject(String objectId) {
        containers.remove(objectId);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        Map<String, SemaphoreContainer> migrationData = new HashMap<String, SemaphoreContainer>();
        for (Map.Entry<String, SemaphoreContainer> entry : containers.entrySet()) {
            String name = entry.getKey();
            SemaphoreContainer semaphoreContainer = entry.getValue();
            if (semaphoreContainer.getPartitionId() == event.getPartitionId()
                    && semaphoreContainer.getTotalBackupCount() >= event.getReplicaIndex()) {
                migrationData.put(name, semaphoreContainer);
            }
        }

        if (migrationData.isEmpty()) {
            return null;
        }

        return new SemaphoreReplicationOperation(migrationData);
    }

    public void insertMigrationData(Map<String, SemaphoreContainer> migrationData) {
        containers.putAll(migrationData);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearMigrationData(event.getPartitionId());
        }
    }

    private void clearMigrationData(int partitionId) {
        Iterator<Map.Entry<String, SemaphoreContainer>> it = containers.entrySet().iterator();
        while (it.hasNext()) {
            SemaphoreContainer semaphoreContainer = it.next().getValue();
            if (semaphoreContainer.getPartitionId() == partitionId) {
                it.remove();
            }
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearMigrationData(event.getPartitionId());
        }
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        clearMigrationData(partitionId);
    }

    @Override
    public void clientDisconnected(String clientUuid) {
        onOwnerDisconnected(clientUuid);
    }
}
