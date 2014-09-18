/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.InternalPartitionService;
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
import static com.hazelcast.spi.impl.ResponseHandlerFactory.createEmptyResponseHandler;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

public class SemaphoreService implements ManagedService, MigrationAwareService, MembershipAwareService,
        RemoteService, ClientAwareService {

    public static final String SERVICE_NAME = "hz:impl:semaphoreService";

    private final ConcurrentMap<String, Permit> permitMap = new ConcurrentHashMap<String, Permit>();

    private final ConstructorFunction<String, Permit> permitConstructor = new ConstructorFunction<String, Permit>() {
        public Permit createNew(String name) {
            SemaphoreConfig config = nodeEngine.getConfig().findSemaphoreConfig(name);
            InternalPartitionService partitionService = nodeEngine.getPartitionService();
            int partitionId = partitionService.getPartitionId(getPartitionKey(name));
            return new Permit(partitionId, new SemaphoreConfig(config));
        }
    };
    private final NodeEngine nodeEngine;

    public SemaphoreService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public Permit getOrCreatePermit(String name) {
        return getOrPutIfAbsent(permitMap, name, permitConstructor);
    }

    // need for testing..
    public boolean containsSemaphore(String name) {
        return permitMap.containsKey(name);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
        permitMap.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        permitMap.clear();
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
        Address thisAddress = nodeEngine.getThisAddress();

        for (String name : permitMap.keySet()) {
            int partitionId = partitionService.getPartitionId(getPartitionKey(name));
            InternalPartition partition = partitionService.getPartition(partitionId);

            if (thisAddress.equals(partition.getOwnerOrNull())) {
                Operation op = new SemaphoreDeadMemberOperation(name, caller)
                        .setPartitionId(partitionId)
                        .setResponseHandler(createEmptyResponseHandler())
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
        permitMap.remove(objectId);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        Map<String, Permit> migrationData = new HashMap<String, Permit>();
        for (Map.Entry<String, Permit> entry : permitMap.entrySet()) {
            String name = entry.getKey();
            Permit permit = entry.getValue();
            if (permit.getPartitionId() == event.getPartitionId() && permit.getTotalBackupCount() >= event.getReplicaIndex()) {
                migrationData.put(name, permit);
            }
        }

        if (migrationData.isEmpty()) {
            return null;
        }

        return new SemaphoreReplicationOperation(migrationData);
    }

    public void insertMigrationData(Map<String, Permit> migrationData) {
        permitMap.putAll(migrationData);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearMigrationData(event.getPartitionId());
        }
    }

    private void clearMigrationData(int partitionId) {
        Iterator<Map.Entry<String, Permit>> it = permitMap.entrySet().iterator();
        while (it.hasNext()) {
            Permit permit = it.next().getValue();
            if (permit.getPartitionId() == partitionId) {
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
