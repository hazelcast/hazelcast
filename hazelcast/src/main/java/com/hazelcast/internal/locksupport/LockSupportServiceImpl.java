/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.locksupport;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.locksupport.operations.LocalLockCleanupOperation;
import com.hazelcast.internal.locksupport.operations.LockReplicationOperation;
import com.hazelcast.internal.locksupport.operations.UnlockOperation;
import com.hazelcast.internal.partition.ChunkedMigrationAwareService;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.ClientAwareService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class LockSupportServiceImpl implements LockSupportService, ManagedService, MembershipAwareService,
        ChunkedMigrationAwareService, ClientAwareService {

    private final NodeEngine nodeEngine;
    private final LockStoreContainer[] containers;
    private final ConcurrentMap<String, ConstructorFunction<ObjectNamespace, LockStoreInfo>> constructors
            = new ConcurrentHashMap<>();

    private final long maxLeaseTimeInMillis;

    public LockSupportServiceImpl(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.containers = new LockStoreContainer[nodeEngine.getPartitionService().getPartitionCount()];
        for (int i = 0; i < containers.length; i++) {
            containers[i] = new LockStoreContainer(this, i);
        }

        maxLeaseTimeInMillis = getMaxLeaseTimeInMillis(nodeEngine.getProperties());
    }

    NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
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
        final UUID uuid = member.getUuid();
        releaseLocksOwnedBy(uuid);
    }

    private void releaseLocksOwnedBy(final UUID uuid) {
        final OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        for (final LockStoreContainer container : containers) {
            operationService.execute(new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    for (LockStoreImpl lockStore : container.getLockStores()) {
                        cleanUpLock(operationService, uuid, container.getPartitionId(), lockStore);
                    }
                }

                @Override
                public int getPartitionId() {
                    return container.getPartitionId();
                }
            });

        }
    }

    private void cleanUpLock(OperationService operationService, UUID uuid, int partitionId, LockStoreImpl lockStore) {
        Collection<LockResource> locks = lockStore.getLocks();
        for (LockResource lock : locks) {
            Data key = lock.getKey();
            if (uuid.equals(lock.getOwner()) && !lock.isTransactional()) {
                UnlockOperation op = createLockCleanupOperation(partitionId, lockStore.getNamespace(), key, uuid);
                // op will be executed on partition thread locally. Invocation is to handle retries.
                operationService.invokeOnTarget(SERVICE_NAME, op, nodeEngine.getThisAddress());
            }
        }
    }

    private UnlockOperation createLockCleanupOperation(int partitionId, ObjectNamespace namespace, Data key, UUID uuid) {
        UnlockOperation op = new LocalLockCleanupOperation(namespace, key, uuid);
        op.setAsyncBackup(true);
        op.setNodeEngine(nodeEngine);
        op.setServiceName(SERVICE_NAME);
        op.setService(LockSupportServiceImpl.this);
        op.setPartitionId(partitionId);
        op.setValidateTarget(false);
        return op;
    }

    @Override
    public Collection<LockResource> getAllLocks() {
        final Collection<LockResource> locks = new LinkedList<>();
        for (LockStoreContainer container : containers) {
            for (LockStoreImpl lockStore : container.getLockStores()) {
                locks.addAll(lockStore.getLocks());
            }
        }
        return locks;
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        LockStoreContainer container = containers[partitionId];
        return container.getAllNamespaces(event.getReplicaIndex());
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return namespace instanceof ObjectNamespace;
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return prepareReplicationOperation(event,
                containers[event.getPartitionId()].getAllNamespaces(event.getReplicaIndex()));
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        int partitionId = event.getPartitionId();
        LockStoreContainer container = containers[partitionId];
        int replicaIndex = event.getReplicaIndex();
        LockReplicationOperation op = new LockReplicationOperation(container, partitionId, replicaIndex, namespaces);
        return op.isEmpty() ? null : op;
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearLockStoresHavingLesserBackupCountThan(event.getPartitionId(), event.getNewReplicaIndex());
        } else {
            scheduleEvictions(event.getPartitionId());
        }
        // Local locks are local to the partition and replicaIndex where they have been acquired.
        // That is the reason they are removed on any partition event on the destination.
        removeLocalLocks(event.getPartitionId());
    }

    private void removeLocalLocks(int partitionId) {
        LockStoreContainer container = containers[partitionId];
        for (LockStoreImpl lockStore : container.getLockStores()) {
            lockStore.removeLocalLocks();
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
                if (leaseTime <= 0) {
                    ls.forceUnlock(lock.getKey());
                } else {
                    ls.scheduleEviction(lock.getKey(), lock.getVersion(), leaseTime);
                }
            }
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearLockStoresHavingLesserBackupCountThan(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
    }

    private void clearLockStoresHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        LockStoreContainer container = containers[partitionId];
        for (LockStoreImpl lockStore : container.getLockStores()) {
            if (thresholdReplicaIndex < 0 || thresholdReplicaIndex > lockStore.getTotalBackupCount()) {
                lockStore.clear();
            }
        }
    }

    @Override
    public void clientDisconnected(UUID clientUuid) {
        releaseLocksOwnedBy(clientUuid);
    }

    public static long getMaxLeaseTimeInMillis(HazelcastProperties hazelcastProperties) {
        return hazelcastProperties.getMillis(ClusterProperty.LOCK_MAX_LEASE_TIME_SECONDS);
    }
}
