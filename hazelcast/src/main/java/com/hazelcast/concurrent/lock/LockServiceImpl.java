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

package com.hazelcast.concurrent.lock;

import com.hazelcast.concurrent.lock.operations.LocalLockCleanupOperation;
import com.hazelcast.concurrent.lock.operations.LockReplicationOperation;
import com.hazelcast.concurrent.lock.operations.MergeOperation;
import com.hazelcast.concurrent.lock.operations.UnlockOperation;
import com.hazelcast.config.LockConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.ClientAwareService;
import com.hazelcast.spi.FragmentedMigrationAwareService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ContextMutexFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.MapUtil.createHashMap;

@SuppressWarnings("checkstyle:methodcount")
public final class LockServiceImpl implements LockService, ManagedService, RemoteService, MembershipAwareService,
        FragmentedMigrationAwareService, ClientAwareService, QuorumAwareService, SplitBrainHandlerService {

    private static final Object NULL_OBJECT = new Object();

    private final NodeEngine nodeEngine;
    private final LockStoreContainer[] containers;
    private final ConcurrentMap<String, ConstructorFunction<ObjectNamespace, LockStoreInfo>> constructors
            = new ConcurrentHashMap<String, ConstructorFunction<ObjectNamespace, LockStoreInfo>>();
    private final ConcurrentMap<String, Object> quorumConfigCache = new ConcurrentHashMap<String, Object>();
    private final ContextMutexFactory quorumConfigCacheMutexFactory = new ContextMutexFactory();
    private final long maxLeaseTimeInMillis;

    public LockServiceImpl(NodeEngine nodeEngine) {
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

    private void cleanUpLock(OperationService operationService, String uuid, int partitionId, LockStoreImpl lockStore) {
        Collection<LockResource> locks = lockStore.getLocks();
        for (LockResource lock : locks) {
            Data key = lock.getKey();
            if (uuid.equals(lock.getOwner()) && !lock.isTransactional()) {
                UnlockOperation op = createLockCleanupOperation(partitionId, lockStore.getNamespace(), key, uuid);
                // op will be executed on partition thread locally. Invocation is to handle retries.
                operationService.invokeOnTarget(SERVICE_NAME, op, nodeEngine.getThisAddress());
            }
            lockStore.cleanWaitersAndSignalsFor(key, uuid);
        }
    }

    private UnlockOperation createLockCleanupOperation(int partitionId, ObjectNamespace namespace, Data key, String uuid) {
        UnlockOperation op = new LocalLockCleanupOperation(namespace, key, uuid);
        op.setAsyncBackup(true);
        op.setNodeEngine(nodeEngine);
        op.setServiceName(SERVICE_NAME);
        op.setService(LockServiceImpl.this);
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
        int partitionId = event.getPartitionId();
        LockStoreContainer container = containers[partitionId];
        int replicaIndex = event.getReplicaIndex();
        LockReplicationOperation op = new LockReplicationOperation(container, partitionId, replicaIndex);
        return op.isEmpty() ? null : op;
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
    public DistributedObject createDistributedObject(String objectId) {
        return new LockProxy(nodeEngine, this, objectId);
    }

    @Override
    public void destroyDistributedObject(String objectId) {
        final Data key = nodeEngine.getSerializationService().toData(objectId, StringPartitioningStrategy.INSTANCE);
        final int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        final LockStoreImpl lockStore = containers[partitionId].getLockStore(new InternalLockNamespace(objectId));

        if (lockStore != null) {
            InternalOperationService operationService = (InternalOperationService) nodeEngine.getOperationService();
            operationService.execute(new PartitionSpecificRunnable() {
                @Override
                public void run() {
                    lockStore.forceUnlock(key);
                }

                @Override
                public int getPartitionId() {
                    return partitionId;
                }
            });
        }
    }

    @Override
    public void clientDisconnected(String clientUuid) {
        releaseLocksOwnedBy(clientUuid);
    }

    public static long getMaxLeaseTimeInMillis(HazelcastProperties hazelcastProperties) {
        return hazelcastProperties.getMillis(GroupProperty.LOCK_MAX_LEASE_TIME_SECONDS);
    }

    @Override
    public String getQuorumName(final String name) {
        // we use caching here because lock operations are often and we should avoid lock config lookup
        Object quorumName = getOrPutSynchronized(quorumConfigCache, name, quorumConfigCacheMutexFactory,
                new ConstructorFunction<String, Object>() {
                    @Override
                    public Object createNew(String arg) {
                        LockConfig lockConfig = nodeEngine.getConfig().findLockConfig(name);
                        String quorumName = lockConfig.getQuorumName();
                        // The quorumName will be null if there is no quorum defined for this data structure,
                        // but the QuorumService is active, due to another data structure with a quorum configuration
                        return quorumName == null ? NULL_OBJECT : quorumName;
                    }
                });
        return quorumName == NULL_OBJECT ? null : (String) quorumName;
    }

    @Override
    public Runnable prepareMergeRunnable() {
        Map<Integer, Map<ObjectNamespace, Map<Data, LockResourceImpl>>> lockStoreMap = createHashMap(containers.length);
        IPartitionService partitionService = nodeEngine.getPartitionService();
        Address thisAddress = nodeEngine.getClusterService().getThisAddress();

        for (LockStoreContainer container : containers) {
            int partitionId = container.getPartitionId();
            if (thisAddress.equals(partitionService.getPartitionOwner(partitionId))) {
                Map<ObjectNamespace, Map<Data, LockResourceImpl>> map = lockStoreMap.get(partitionId);
                Collection<LockStoreImpl> lockStores = container.getLockStores();
                if (map == null) {
                    map = createHashMap(lockStores.size());
                    lockStoreMap.put(partitionId, map);
                }
                // add your owned entries to the map so they will be merged
                for (LockStoreImpl lockStore : lockStores) {
                    map.put(lockStore.getNamespace(), lockStore.getLocksCopy());
                }
            }

            // clear all items either owned or backup
            container.clear();
        }

        return new Merger(lockStoreMap);
    }

    private class Merger implements Runnable {

        private static final int TIMEOUT_FACTOR = 500;

        private final Map<Integer, Map<ObjectNamespace, Map<Data, LockResourceImpl>>> lockStoreMap;

        Merger(Map<Integer, Map<ObjectNamespace, Map<Data, LockResourceImpl>>> lockStoreMap) {
            this.lockStoreMap = lockStoreMap;
        }

        @Override
        public void run() {
            // we cannot merge into a 3.9 cluster, since not all members may understand the MergeOperation
            if (nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_10)) {
                return;
            }

            final ILogger logger = nodeEngine.getLogger(LockService.class);
            final Semaphore semaphore = new Semaphore(0);

            ExecutionCallback<Object> mergeCallback = new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                    semaphore.release(1);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.warning("Error while running merge operation: " + t.getMessage());
                    semaphore.release(1);
                }
            };

            int itemCount = 0;
            for (Map.Entry<Integer, Map<ObjectNamespace, Map<Data, LockResourceImpl>>> entry : lockStoreMap.entrySet()) {
                int partitionId = entry.getKey();
                Map<ObjectNamespace, Map<Data, LockResourceImpl>> lockResourceMap = entry.getValue();

                for (Map.Entry<ObjectNamespace, Map<Data, LockResourceImpl>> lockEntry : lockResourceMap.entrySet()) {
                    ObjectNamespace namespace = lockEntry.getKey();
                    Map<Data, LockResourceImpl> locks = lockEntry.getValue();

                    for (Map.Entry<Data, LockResourceImpl> lockResourceEntry : locks.entrySet()) {
                        Data key = lockResourceEntry.getKey();
                        LockResourceImpl lockResource = lockResourceEntry.getValue();
                        if (lockResource.isLocal()) {
                            continue;
                        }
                        itemCount++;

                        MergeOperation operation = new MergeOperation(namespace, key, lockResource);
                        try {
                            nodeEngine.getOperationService()
                                    .invokeOnPartition(SERVICE_NAME, operation, partitionId)
                                    .andThen(mergeCallback);
                        } catch (Throwable t) {
                            throw rethrow(t);
                        }
                    }
                }
            }
            lockStoreMap.clear();

            try {
                semaphore.tryAcquire(itemCount, itemCount * TIMEOUT_FACTOR, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.finest("Interrupted while waiting for merge operation...");
            }
        }
    }
}
