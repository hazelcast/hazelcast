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

package com.hazelcast.concurrent.lock;

import com.hazelcast.client.ClientCommandHandler;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationType;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.ResponseHandlerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * @mdogan 2/12/13
 */
public class LockService implements ManagedService, RemoteService, MembershipAwareService,
        MigrationAwareService, ClientProtocolService, SharedLockService {

    private final NodeEngine nodeEngine;
    private final LockStoreContainer[] containers;

    public LockService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.containers = new LockStoreContainer[nodeEngine.getPartitionService().getPartitionCount()];
        for (int i = 0; i < containers.length; i++) {
            containers[i] = new LockStoreContainer(this, i);
        }
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    public void shutdown() {
        for (LockStoreContainer container : containers) {
            container.clear();
        }
    }

    public LockStoreView createLockStore(int partitionId, ILockNamespace namespace, int backupCount, int asyncBackupCount) {
        final LockStoreContainer container = getLockContainer(partitionId);
        return container.createLockStore(namespace, backupCount, asyncBackupCount);
    }

    public void destroyLockStore(int partitionId, ILockNamespace namespace) {
        final LockStoreContainer container = getLockContainer(partitionId);
        container.destroyLockStore(namespace);
    }

    LockStoreContainer getLockContainer(int partitionId) {
        return containers[partitionId];
    }

    public void memberAdded(MembershipServiceEvent event) {
    }

    public void memberRemoved(MembershipServiceEvent event) {
        // TODO: release lock on member remove
        final MemberImpl member = event.getMember();
        final String uuid = member.getUuid();
        releaseLocksOf(uuid);
    }

    private void releaseLocksOf(String uuid) {
        for (LockStoreContainer container : containers) {
            for (LockStore lockStore : container.getLockStores()) {
                Map<Data, LockInfo> locks = lockStore.getLocks();
                for (Map.Entry<Data, LockInfo> entry : locks.entrySet()) {
                    final Data key = entry.getKey();
                    final LockInfo lock = entry.getValue();
                    if (lock.getOwner().equals(uuid)) {
                        UnlockOperation op = new UnlockOperation(lockStore.getNamespace(), key, -1, true);
                        op.setNodeEngine(nodeEngine);
                        op.setServiceName(SERVICE_NAME);
                        op.setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler());
                        op.setPartitionId(container.getPartitionId());
                        nodeEngine.getOperationService().runOperation(op);
                    }
                }
            }
        }
    }

    public void beforeMigration(MigrationServiceEvent migrationServiceEvent) {
    }

    public Operation prepareMigrationOperation(MigrationServiceEvent event) {
        LockStoreContainer container = containers[event.getPartitionId()];
        LockMigrationOperation op = new LockMigrationOperation(container, event.getPartitionId(), event.getReplicaIndex());
        return op.isEmpty() ? null : op;
    }

    public void commitMigration(MigrationServiceEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            final LockStoreContainer container = containers[event.getPartitionId()];
            if (event.getMigrationType() == MigrationType.MOVE) {
                container.clear();
            } else if (event.getMigrationType() == MigrationType.MOVE_COPY_BACK) {
                for (LockStore ls : container.getLockStores()) {
                    if (ls.getTotalBackupCount() < event.getCopyBackReplicaIndex()) {
                        ls.clear();
                    }
                }
            }
        }
    }

    public void rollbackMigration(MigrationServiceEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            final LockStoreContainer container = containers[event.getPartitionId()];
            container.clear();
        }
    }

    public int getMaxBackupCount() {
        int max = 0;
        for (LockStoreContainer container : containers) {
            max = Math.max(container.getMaxBackupCount(), max);
        }
        return 0;
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public DistributedObject createDistributedObject(Object objectId) {
        return new LockProxy(nodeEngine, this, nodeEngine.getSerializationService().toData(objectId));
    }

    public DistributedObject createDistributedObjectForClient(Object objectId) {
        return new LockProxy(nodeEngine, this, nodeEngine.getSerializationService().toData(objectId));
    }

    public void destroyDistributedObject(Object objectId) {
        final Data key = nodeEngine.getSerializationService().toData(objectId);
        for (LockStoreContainer container : containers) {
            final LockStore lockStore = container.getLockStore(new InternalLockNamespace());
            lockStore.forceUnlock(key);
        }
    }

    public Map<Command, ClientCommandHandler> getCommandsAsMap() {
        return null;
    }

    public void onClientDisconnect(String clientUuid) {
        releaseLocksOf(clientUuid);
    }

    public LockStore getLockStore(int partitionId, ILockNamespace namespace) {
        return getLockContainer(partitionId).getLockStore(namespace);
    }
}
