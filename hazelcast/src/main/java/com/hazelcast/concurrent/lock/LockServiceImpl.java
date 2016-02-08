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

package com.hazelcast.concurrent.lock;

import com.hazelcast.concurrent.lock.operations.LocalLockCleanupOperation;
import com.hazelcast.concurrent.lock.operations.LockReplicationOperation;
import com.hazelcast.concurrent.lock.operations.UnlockOperation;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.instance.GroupProperties;
import com.hazelcast.internal.instance.GroupProperty;
import com.hazelcast.internal.instance.MemberImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.ClientAwareService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConstructorFunction;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createEmptyResponseHandler;

public final class LockServiceImpl implements LockService, ManagedService, RemoteService, MembershipAwareService,
        MigrationAwareService, ClientAwareService {

    private final NodeEngine nodeEngine;
    private final LockStoreContainer[] containers;
    private final ConcurrentMap<String, ConstructorFunction<ObjectNamespace, LockStoreInfo>> constructors
            = new ConcurrentHashMap<String, ConstructorFunction<ObjectNamespace, LockStoreInfo>>();

    private final long maxLeaseTimeInMillis;

    public LockServiceImpl(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.containers = new LockStoreContainer[nodeEngine.getPartitionService().getPartitionCount()];
        for (int i = 0; i < containers.length; i++) {
            containers[i] = new LockStoreContainer(this, i);
        }

        maxLeaseTimeInMillis = getMaxLeaseTimeInMillis(nodeEngine.getGroupProperties());
    }

    NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        registerLockStoreConstructor(SERVICE_NAME, new ConstructorFunction<ObjectNamespace, LockStoreInfo>() {
            public LockStoreInfo createNew(ObjectNamespace key) {
                return new LockStoreInfo() {
                    @Override
                    public int getBackupCount() {
                        return 1;
                    }

                    @Override
                    public int getAsyncBackupCount() {
                        return 0;
                    }
                };
            }
        });
    }

    @Override
    public void reset() {
        for (LockStoreContainer container : containers) {
            for (LockStoreImpl lockStore : container.getLockStores()) {
                lockStore.clear();
            }
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        for (LockStoreContainer container : containers) {
            container.clear();
        }
    }

    @Override
    public long getMaxLeaseTimeInMillis() {
        return maxLeaseTimeInMillis;
    }

    @Override
    public void registerLockStoreConstructor(String serviceName,
                                             ConstructorFunction<ObjectNamespace, LockStoreInfo> constructorFunction) {
        boolean put = constructors.putIfAbsent(serviceName, constructorFunction) == null;
        if (!put) {
            throw new IllegalArgumentException("LockStore constructor for service[" + serviceName + "] "
                    + "is already registered!");
        }
    }

    /**
     * Gets the constructor for the given service, or null if the constructor doesn't exist.
     *
     * @param serviceName the name of the constructor to look up.
     * @return the found ConstructorFunction.
     */
    ConstructorFunction<ObjectNamespace, LockStoreInfo> getConstructor(String serviceName) {
        return constructors.get(serviceName);
    }

    @Override
    public LockStore createLockStore(int partitionId, ObjectNamespace namespace) {
        final LockStoreContainer container = getLockContainer(partitionId);
        container.getOrCreateLockStore(namespace);
        return new LockStoreProxy(container, namespace);
    }

    @Override
    public void clearLockStore(int partitionId, ObjectNamespace namespace) {
        LockStoreContainer container = getLockContainer(partitionId);
        container.clearLockStore(namespace);
    }


    public LockStoreContainer getLockContainer(int partitionId) {
        return containers[partitionId];
    }

    public LockStoreImpl getLockStore(int partitionId, ObjectNamespace namespace) {
        return getLockContainer(partitionId).getOrCreateLockStore(namespace);
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        final MemberImpl member = event.getMember();
        final String uuid = member.getUuid();
        releaseLocksOwnedBy(uuid);
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
    }

    private void releaseLocksOwnedBy(final String uuid) {
        final InternalOperationService operationService = (InternalOperationService) nodeEngine.getOperationService();
        for (final LockStoreContainer container : containers) {
            operationService.execute(new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    for (LockStoreImpl lockStore : container.getLockStores()) {
                        releaseLock(operationService, uuid, container.getPartitionId(), lockStore);
                    }
                }

                @Override
                public int getPartitionId() {
                    return container.getPartitionId();
                }
            });

        }
    }

    private void releaseLock(OperationService operationService, String uuid, int partitionId, LockStoreImpl lockStore) {
        Collection<LockResource> locks = lockStore.getLocks();
        for (LockResource lock : locks) {
            Data key = lock.getKey();
            if (uuid.equals(lock.getOwner()) && !lock.isTransactional()) {
                UnlockOperation op = createUnlockOperation(partitionId, lockStore.getNamespace(), key, uuid);
                operationService.runOperationOnCallingThread(op);
            }
        }
    }

    private UnlockOperation createUnlockOperation(int partitionId, ObjectNamespace namespace, Data key, String uuid) {
        UnlockOperation op = new LocalLockCleanupOperation(namespace, key, uuid);
        op.setAsyncBackup(true);
        op.setNodeEngine(nodeEngine);
        op.setServiceName(SERVICE_NAME);
        op.setService(LockServiceImpl.this);
        op.setOperationResponseHandler(createEmptyResponseHandler());
        op.setPartitionId(partitionId);
        op.setValidateTarget(false);
        return op;
    }

    @Override
    public Collection<LockResource> getAllLocks() {
        final Collection<LockResource> locks = new LinkedList<LockResource>();
        for (LockStoreContainer container : containers) {
            for (LockStoreImpl lockStore : container.getLockStores()) {
                locks.addAll(lockStore.getLocks());
            }
        }
        return locks;
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        LockStoreContainer container = containers[partitionId];
        int replicaIndex = event.getReplicaIndex();
        LockReplicationOperation op = new LockReplicationOperation(container, partitionId, replicaIndex);
        if (op.isEmpty()) {
            return null;
        } else {
            return op;
        }
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearPartition(event.getPartitionId());
        } else {
            int partitionId = event.getPartitionId();
            scheduleEvictions(partitionId);
        }
    }

    private void scheduleEvictions(int partitionId) {
        long now = Clock.currentTimeMillis();
        LockStoreContainer container = containers[partitionId];
        for (LockStoreImpl ls : container.getLockStores()) {
            for (LockResource lock : ls.getLocks()) {
                long expirationTime = lock.getExpirationTime();
                if (expirationTime == Long.MAX_VALUE || expirationTime < 0) {
                    continue;
                }

                long leaseTime = expirationTime - now;
                ls.scheduleEviction(lock.getKey(), lock.getVersion(), leaseTime);
            }
        }
    }

    private void clearPartition(int partitionId) {
        final LockStoreContainer container = containers[partitionId];
        for (LockStoreImpl ls : container.getLockStores()) {
            ls.clear();
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearPartition(event.getPartitionId());
        }
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        clearPartition(partitionId);
    }

    @Override
    public DistributedObject createDistributedObject(String objectId) {
        return new LockProxy(nodeEngine, this, objectId);
    }

    @Override
    public void destroyDistributedObject(String objectId) {
        Data key = nodeEngine.getSerializationService().toData(objectId);
        for (LockStoreContainer container : containers) {
            InternalLockNamespace namespace = new InternalLockNamespace(objectId);
            LockStoreImpl lockStore = container.getOrCreateLockStore(namespace);
            lockStore.forceUnlock(key);
        }
    }

    @Override
    public void clientDisconnected(String clientUuid) {
        releaseLocksOwnedBy(clientUuid);
    }

    public static long getMaxLeaseTimeInMillis(GroupProperties groupProperties) {
        return groupProperties.getMillis(GroupProperty.LOCK_MAX_LEASE_TIME_SECONDS);
    }
}
