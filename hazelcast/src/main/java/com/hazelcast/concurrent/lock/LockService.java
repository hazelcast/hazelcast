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
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.ResponseHandlerFactory;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.EntryTaskSchedulerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @mdogan 2/12/13
 */
public class LockService implements ManagedService, RemoteService, MembershipAwareService,
        MigrationAwareService, ClientProtocolService, SharedLockService {

    private final NodeEngine nodeEngine;
    private final LockStoreContainer[] containers;
    private final ConcurrentHashMap<ObjectNamespace, EntryTaskScheduler> evictionProcessors = new ConcurrentHashMap<ObjectNamespace, EntryTaskScheduler>();

    public LockService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.containers = new LockStoreContainer[nodeEngine.getPartitionService().getPartitionCount()];
        for (int i = 0; i < containers.length; i++) {
            containers[i] = new LockStoreContainer(this, i);
        }
    }

    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    public void reset() {
        for (LockStoreContainer container : containers) {
            for (LockStoreImpl lockStore : container.getLockStores()) {
                lockStore.clear();
            }
        }
    }

    public void shutdown() {
        for (LockStoreContainer container : containers) {
            container.clear();
        }
    }

    public LockStore createLockStore(int partitionId, ObjectNamespace namespace, int backupCount, int asyncBackupCount) {
        final LockStoreContainer container = getLockContainer(partitionId);
        container.createLockStore(namespace, backupCount, asyncBackupCount);
        return new LockStoreProxy(container, namespace);
    }

    public void clearLockStore(int partitionId, ObjectNamespace namespace) {
        final LockStoreContainer container = getLockContainer(partitionId);
        container.clearLockStore(namespace);
    }

    private final ConcurrencyUtil.ConstructorFunction<ObjectNamespace, EntryTaskScheduler> schedulerConstructor = new ConcurrencyUtil.ConstructorFunction<ObjectNamespace, EntryTaskScheduler>() {
        public EntryTaskScheduler createNew(ObjectNamespace namespace) {
            return EntryTaskSchedulerFactory.newScheduler(nodeEngine.getExecutionService().getScheduledExecutor(), new LockEvictionProcessor(nodeEngine, namespace), true);
        }
    };

    public void scheduleEviction(ObjectNamespace namespace, Data key, long delay) {
        EntryTaskScheduler scheduler = ConcurrencyUtil.getOrPutSynchronized(evictionProcessors, namespace, evictionProcessors, schedulerConstructor);
        scheduler.schedule(delay, key, null);
    }

    public void cancelEviction(ObjectNamespace namespace, Data key) {
        EntryTaskScheduler scheduler = ConcurrencyUtil.getOrPutSynchronized(evictionProcessors, namespace, evictionProcessors, schedulerConstructor);
        scheduler.cancel(key);
    }

    LockStoreContainer getLockContainer(int partitionId) {
        return containers[partitionId];
    }

    public LockStoreContainer[] getLockContainers() {
        return containers;
    }

    LockStoreImpl getLockStore(int partitionId, ObjectNamespace namespace) {
        return getLockContainer(partitionId).getOrCreateDefaultLockStore(namespace);
    }

    public void memberAdded(MembershipServiceEvent event) {
    }

    public void memberRemoved(MembershipServiceEvent event) {
        final MemberImpl member = event.getMember();
        final String uuid = member.getUuid();
        releaseLocksOf(uuid);
    }

    private void releaseLocksOf(final String uuid) {
        for (LockStoreContainer container : containers) {
            for (LockStoreImpl lockStore : container.getLockStores()) {
                Map<Data, DistributedLock> locks = lockStore.getLocks();
                for (Map.Entry<Data, DistributedLock> entry : locks.entrySet()) {
                    final Data key = entry.getKey();
                    final DistributedLock lock = entry.getValue();
                    if (uuid.equals(lock.getOwner()) && !lock.isTransactional()) {
                        UnlockOperation op = new UnlockOperation(lockStore.getNamespace(), key, -1, true);
                        op.setNodeEngine(nodeEngine);
                        op.setServiceName(SERVICE_NAME);
                        op.setService(LockService.this);
                        op.setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler());
                        op.setPartitionId(container.getPartitionId());
                        nodeEngine.getOperationService().executeOperation(op);
                    }
                }
            }
        }
    }

    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        LockStoreContainer container = containers[event.getPartitionId()];
        LockReplicationOperation op = new LockReplicationOperation(container, event.getPartitionId(), event.getReplicaIndex());
        return op.isEmpty() ? null : op;
    }

    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearPartition(event.getPartitionId());
        }
    }

    private void clearPartition(int partitionId) {
        final LockStoreContainer container = containers[partitionId];
        for (LockStoreImpl ls : container.getLockStores()) {
            ls.clear();
        }
    }

    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearPartition(event.getPartitionId());
        }
    }

    public void clearPartitionReplica(int partitionId) {
        clearPartition(partitionId);
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
            final LockStoreImpl lockStore = container.getOrCreateDefaultLockStore(new InternalLockNamespace());
            lockStore.forceUnlock(key);
        }
    }

    public Map<Command, ClientCommandHandler> getCommandsAsMap() {
        return null;
    }

    public void clientDisconnected(String clientUuid) {
        releaseLocksOf(clientUuid);
    }
}
