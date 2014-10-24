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

package com.hazelcast.multimap.impl;

import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockStoreInfo;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.EventData;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.impl.LocalMultiMapStatsImpl;
import com.hazelcast.multimap.impl.operations.MultiMapMigrationOperation;
import com.hazelcast.multimap.impl.txn.TransactionalMultiMapProxy;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;

import java.util.EventListener;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MultiMapService implements ManagedService, RemoteService, MigrationAwareService,
        EventPublishingService<EventData, EntryListener>, TransactionalService {

    public static final String SERVICE_NAME = "hz:impl:multiMapService";
    private static final int STATS_MAP_INITIAL_CAPACITY = 1000;
    private static final int REPLICA_ADDRESS_TRY_COUNT = 3;
    private static final int REPLICA_ADDRESS_SLEEP_WAIT_MILLIS = 1000;
    private final NodeEngine nodeEngine;
    private final MultiMapPartitionContainer[] partitionContainers;
    private final ConcurrentMap<String, LocalMultiMapStatsImpl> statsMap =
            new ConcurrentHashMap<String, LocalMultiMapStatsImpl>(STATS_MAP_INITIAL_CAPACITY);
    private final ConstructorFunction<String, LocalMultiMapStatsImpl> localMultiMapStatsConstructorFunction =
            new ConstructorFunction<String, LocalMultiMapStatsImpl>() {

                public LocalMultiMapStatsImpl createNew(String key) {
                    return new LocalMultiMapStatsImpl();
                }
            };
    private final ILogger logger;
    private final MultiMapEventsDispatcher dispatcher;
    private final MultiMapEventsPublisher publisher;

    public MultiMapService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        partitionContainers = new MultiMapPartitionContainer[partitionCount];
        this.logger = nodeEngine.getLogger(MultiMapService.class);
        dispatcher = new MultiMapEventsDispatcher(this, nodeEngine.getClusterService());
        publisher = new MultiMapEventsPublisher(nodeEngine);
    }

    public void init(final NodeEngine nodeEngine, Properties properties) {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int partition = 0; partition < partitionCount; partition++) {
            partitionContainers[partition] = new MultiMapPartitionContainer(this, partition);
        }
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            lockService.registerLockStoreConstructor(SERVICE_NAME,
                    new ConstructorFunction<ObjectNamespace, LockStoreInfo>() {
                        public LockStoreInfo createNew(final ObjectNamespace key) {
                            String name = key.getObjectName();
                            final MultiMapConfig multiMapConfig = nodeEngine.getConfig().findMultiMapConfig(name);

                            return new LockStoreInfo() {
                                public int getBackupCount() {
                                    return multiMapConfig.getSyncBackupCount();
                                }

                                public int getAsyncBackupCount() {
                                    return multiMapConfig.getAsyncBackupCount();
                                }
                            };
                        }
                    });
        }
    }

    public void reset() {
        for (MultiMapPartitionContainer container : partitionContainers) {
            if (container != null) {
                container.destroy();
            }
        }
    }

    public void shutdown(boolean terminate) {
        reset();
        for (int i = 0; i < partitionContainers.length; i++) {
            partitionContainers[i] = null;
        }
    }

    public MultiMapContainer getOrCreateCollectionContainer(int partitionId, String name) {
        return partitionContainers[partitionId].getOrCreateMultiMapContainer(name);
    }

    public MultiMapPartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    public DistributedObject createDistributedObject(String name) {
        return new ObjectMultiMapProxy(this, nodeEngine, name);
    }

    public void destroyDistributedObject(String name) {
        for (MultiMapPartitionContainer container : partitionContainers) {
            if (container != null) {
                container.destroyCollection(name);
            }
        }
        nodeEngine.getEventService().deregisterAllListeners(SERVICE_NAME, name);
    }

    public Set<Data> localKeySet(String name) {
        Set<Data> keySet = new HashSet<Data>();
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
        Address thisAddress = clusterService.getThisAddress();
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            InternalPartition partition = nodeEngine.getPartitionService().getPartition(i);
            MultiMapPartitionContainer partitionContainer = getPartitionContainer(i);
            MultiMapContainer multiMapContainer = partitionContainer.getCollectionContainer(name);
            if (multiMapContainer == null) {
                continue;
            }
            if (thisAddress.equals(partition.getOwnerOrNull())) {
                keySet.addAll(multiMapContainer.keySet());
            }
        }
        getLocalMultiMapStatsImpl(name).incrementOtherOperations();
        return keySet;
    }

    public SerializationService getSerializationService() {
        return nodeEngine.getSerializationService();
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public void publishMultiMapEvent(String mapName, EntryEventType eventType,
                                     int numberOfEntriesAffected) {
        publisher.publishMultiMapEvent(mapName, eventType, numberOfEntriesAffected);

    }

    public final void publishEntryEvent(String multiMapName, EntryEventType eventType, Data key, Object value) {
        publisher.publishEntryEvent(multiMapName, eventType, key, value);
    }

    public String addListener(String name, EventListener listener, Data key, boolean includeValue, boolean local) {
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration;
        final MultiMapEventFilter filter = new MultiMapEventFilter(includeValue, key);
        if (local) {
            registration = eventService.registerLocalListener(SERVICE_NAME, name, filter, listener);
        } else {
            registration = eventService.registerListener(SERVICE_NAME, name, filter, listener);
        }
        return registration.getId();
    }

    public boolean removeListener(String name, String registrationId) {
        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(SERVICE_NAME, name, registrationId);
    }

    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        int replicaIndex = event.getReplicaIndex();
        final MultiMapPartitionContainer partitionContainer = partitionContainers[event.getPartitionId()];
        if (partitionContainer == null) {
            return null;
        }
        Map<String, Map> map = new HashMap<String, Map>(partitionContainer.containerMap.size());
        for (Map.Entry<String, MultiMapContainer> entry : partitionContainer.containerMap.entrySet()) {
            String name = entry.getKey();
            MultiMapContainer container = entry.getValue();
            if (container.getConfig().getTotalBackupCount() < replicaIndex) {
                continue;
            }
            map.put(name, container.getMultiMapWrappers());
        }
        if (map.isEmpty()) {
            return null;
        }
        return new MultiMapMigrationOperation(map);
    }

    public void insertMigratedData(int partitionId, Map<String, Map> map) {
        for (Map.Entry<String, Map> entry : map.entrySet()) {
            String name = entry.getKey();
            MultiMapContainer container = getOrCreateCollectionContainer(partitionId, name);
            Map<Data, MultiMapWrapper> collections = entry.getValue();
            container.getMultiMapWrappers().putAll(collections);
        }
    }

    private void clearMigrationData(int partitionId) {
        final MultiMapPartitionContainer partitionContainer = partitionContainers[partitionId];
        if (partitionContainer != null) {
            partitionContainer.containerMap.clear();
        }
    }

    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearMigrationData(event.getPartitionId());
        }
    }

    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearMigrationData(event.getPartitionId());
        }
    }

    public void clearPartitionReplica(int partitionId) {
        clearMigrationData(partitionId);
    }

    public LocalMapStats createStats(String name) {
        LocalMultiMapStatsImpl stats = getLocalMultiMapStatsImpl(name);
        long ownedEntryCount = 0;
        long backupEntryCount = 0;
        long hits = 0;
        long lockedEntryCount = 0;
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();

        Address thisAddress = clusterService.getThisAddress();
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            InternalPartition partition = nodeEngine.getPartitionService().getPartition(i);
            MultiMapPartitionContainer partitionContainer = getPartitionContainer(i);
            MultiMapContainer multiMapContainer = partitionContainer.getCollectionContainer(name);
            if (multiMapContainer == null) {
                continue;
            }
            Address owner = partition.getOwnerOrNull();
            if (owner != null) {
                if (owner.equals(thisAddress)) {
                    lockedEntryCount += multiMapContainer.getLockedCount();
                    for (MultiMapWrapper wrapper : multiMapContainer.getMultiMapWrappers().values()) {
                        hits += wrapper.getHits();
                        ownedEntryCount += wrapper.getCollection(false).size();
                    }
                } else {
                    int backupCount = multiMapContainer.getConfig().getTotalBackupCount();
                    for (int j = 1; j <= backupCount; j++) {
                        Address replicaAddress = partition.getReplicaAddress(j);
                        int memberSize = nodeEngine.getClusterService().getMembers().size();

                        int tryCount = REPLICA_ADDRESS_TRY_COUNT;
                        // wait if the partition table is not updated yet
                        while (memberSize > backupCount && replicaAddress == null && tryCount-- > 0) {
                            try {
                                Thread.sleep(REPLICA_ADDRESS_SLEEP_WAIT_MILLIS);
                            } catch (InterruptedException e) {
                                throw ExceptionUtil.rethrow(e);
                            }
                            replicaAddress = partition.getReplicaAddress(j);
                        }

                        if (replicaAddress != null && replicaAddress.equals(thisAddress)) {
                            for (MultiMapWrapper wrapper : multiMapContainer.getMultiMapWrappers().values()) {
                                backupEntryCount += wrapper.getCollection(false).size();
                            }
                        }
                    }
                }
            }
        }
        stats.setOwnedEntryCount(ownedEntryCount);
        stats.setBackupEntryCount(backupEntryCount);
        stats.setHits(hits);
        stats.setLockedEntryCount(lockedEntryCount);
        return stats;
    }

    public LocalMultiMapStatsImpl getLocalMultiMapStatsImpl(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, localMultiMapStatsConstructorFunction);
    }

    public <T extends TransactionalObject> T createTransactionalObject(String name, TransactionSupport transaction) {
        return (T) new TransactionalMultiMapProxy(nodeEngine, this, name, transaction);
    }

    public void rollbackTransaction(String transactionId) {

    }

    @Override
    public void dispatchEvent(EventData event, EntryListener listener) {
        dispatcher.dispatchEvent(event, listener);
    }
}
