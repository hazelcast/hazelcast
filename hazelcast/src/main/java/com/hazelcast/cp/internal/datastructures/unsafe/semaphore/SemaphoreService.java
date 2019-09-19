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

package com.hazelcast.cp.internal.datastructures.unsafe.semaphore;

import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.SemaphoreDetachMemberOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.semaphore.operations.SemaphoreReplicationOperation;
import com.hazelcast.internal.services.ClientAwareService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.MemberAttributeServiceEvent;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.SplitBrainProtectionAwareService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.partition.MigrationAwareService;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.spi.partition.PartitionMigrationEvent;
import com.hazelcast.spi.partition.PartitionReplicationEvent;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ContextMutexFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getPartitionKey;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutSynchronized;

public class SemaphoreService implements ManagedService, MigrationAwareService, MembershipAwareService, RemoteService,
        ClientAwareService, SplitBrainProtectionAwareService {

    public static final String SERVICE_NAME = "hz:impl:semaphoreService";

    private static final Object NULL_OBJECT = new Object();

    private final ConcurrentMap<String, SemaphoreContainer> containers = new ConcurrentHashMap<String, SemaphoreContainer>();

    private final ConcurrentMap<String, Object> splitBrainProtectionConfigCache = new ConcurrentHashMap<String, Object>();
    private final ContextMutexFactory splitBrainProtectionConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> splitBrainProtectionConfigConstructor =
            new ConstructorFunction<String, Object>() {
        @Override
        public Object createNew(String name) {
            SemaphoreConfig semaphoreConfig = nodeEngine.getConfig().findSemaphoreConfig(name);
            String splitBrainProtectionName = semaphoreConfig.getSplitBrainProtectionName();
            return splitBrainProtectionName == null ? NULL_OBJECT : splitBrainProtectionName;
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
        splitBrainProtectionConfigCache.remove(objectId);
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
    public String getSplitBrainProtectionName(final String name) {
        Object splitBrainProtectionName = getOrPutSynchronized(splitBrainProtectionConfigCache, name,
                splitBrainProtectionConfigCacheMutexFactory, splitBrainProtectionConfigConstructor);
        return splitBrainProtectionName == NULL_OBJECT ? null : (String) splitBrainProtectionName;
    }

}
