/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.concurrent.semaphore.operations.SemaphoreDetachMemberOperation;
import com.hazelcast.concurrent.semaphore.operations.SemaphoreReplicationOperation;
import com.hazelcast.config.SemaphoreConfig;
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
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ContextMutexFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getPartitionKey;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;

public class SemaphoreService implements ManagedService, MigrationAwareService, MembershipAwareService, RemoteService,
        ClientAwareService, QuorumAwareService {

    public static final String SERVICE_NAME = "hz:impl:semaphoreService";

    private static final Object NULL_OBJECT = new Object();

    private final ConcurrentMap<String, SemaphoreContainer> containers = new ConcurrentHashMap<String, SemaphoreContainer>();

    private final ConcurrentMap<String, Object> quorumConfigCache = new ConcurrentHashMap<String, Object>();
    private final ContextMutexFactory quorumConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> quorumConfigConstructor = new ConstructorFunction<String, Object>() {
        @Override
        public Object createNew(String name) {
            SemaphoreConfig semaphoreConfig = nodeEngine.getConfig().findSemaphoreConfig(name);
            String quorumName = semaphoreConfig.getQuorumName();
            return quorumName == null ? NULL_OBJECT : quorumName;
        }
    };

    private final ConstructorFunction<String, SemaphoreContainer> containerConstructor
            = new ConstructorFunction<String, SemaphoreContainer>() {
        @Override
        public SemaphoreContainer createNew(String name) {
            SemaphoreConfig config = nodeEngine.getConfig().findSemaphoreConfig(name);
            IPartitionService partitionService = nodeEngine.getPartitionService();
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
        onOwnerDisconnected(event.getMember().getUuid());
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
    }

    private void onOwnerDisconnected(final String owner) {
        OperationService operationService = nodeEngine.getOperationService();

        for (Map.Entry<String, SemaphoreContainer> entry : containers.entrySet()) {
            String name = entry.getKey();
            SemaphoreContainer container = entry.getValue();

            Operation op = new SemaphoreDetachMemberOperation(name, owner)
                    .setPartitionId(container.getPartitionId())
                    .setValidateTarget(false)
                    .setService(this)
                    .setNodeEngine(nodeEngine)
                    .setServiceName(SERVICE_NAME);

            // op will be executed on partition thread locally.
            // Invocation is to handle retries (if partition is being migrated).
            operationService.invokeOnTarget(SERVICE_NAME, op, nodeEngine.getThisAddress());
        }
    }

    @Override
    public SemaphoreProxy createDistributedObject(String objectId) {
        return new SemaphoreProxy(objectId, this, nodeEngine);
    }

    @Override
    public void destroyDistributedObject(String objectId) {
        containers.remove(objectId);
        quorumConfigCache.remove(objectId);
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
            clearSemaphoresHavingLesserBackupCountThan(event.getPartitionId(), event.getNewReplicaIndex());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearSemaphoresHavingLesserBackupCountThan(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
    }

    private void clearSemaphoresHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        Iterator<SemaphoreContainer> it = containers.values().iterator();
        while (it.hasNext()) {
            SemaphoreContainer semaphoreContainer = it.next();
            if (semaphoreContainer.getPartitionId() != partitionId) {
                continue;
            }

            if (thresholdReplicaIndex < 0 || thresholdReplicaIndex > semaphoreContainer.getTotalBackupCount()) {
                it.remove();
            }
        }
    }

    @Override
    public void clientDisconnected(String clientUuid) {
        onOwnerDisconnected(clientUuid);
    }

    @Override
    public String getQuorumName(final String name) {
        Object quorumName = getOrPutSynchronized(quorumConfigCache, name, quorumConfigCacheMutexFactory, quorumConfigConstructor);
        return quorumName == NULL_OBJECT ? null : (String) quorumName;
    }

}
