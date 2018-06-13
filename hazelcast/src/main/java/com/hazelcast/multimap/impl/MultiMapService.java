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

package com.hazelcast.multimap.impl;

import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockStoreInfo;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.map.impl.event.EventData;
import com.hazelcast.monitor.LocalMultiMapStats;
import com.hazelcast.monitor.impl.LocalMultiMapStatsImpl;
import com.hazelcast.multimap.impl.operations.MergeOperation;
import com.hazelcast.multimap.impl.operations.MultiMapReplicationOperation;
import com.hazelcast.multimap.impl.txn.TransactionalMultiMapProxy;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.quorum.QuorumService;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.FragmentedMigrationAwareService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.spi.impl.merge.AbstractContainerMerger;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MultiMapMergeTypes;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ContextMutexFactory;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EventListener;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.config.ConfigValidator.checkMultiMapConfig;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.util.MapUtil.createConcurrentHashMap;
import static com.hazelcast.util.MapUtil.createHashMap;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Thread.currentThread;

@SuppressWarnings({"checkstyle:classfanoutcomplexity", "checkstyle:methodcount"})
public class MultiMapService implements ManagedService, RemoteService, FragmentedMigrationAwareService,
        EventPublishingService<EventData, EntryListener>, TransactionalService, StatisticsAwareService<LocalMultiMapStats>,
        QuorumAwareService, SplitBrainHandlerService {

    public static final String SERVICE_NAME = "hz:impl:multiMapService";

    private static final Object NULL_OBJECT = new Object();

    private static final int STATS_MAP_INITIAL_CAPACITY = 1000;
    private static final int REPLICA_ADDRESS_TRY_COUNT = 3;
    private static final int REPLICA_ADDRESS_SLEEP_WAIT_MILLIS = 1000;

    private final NodeEngine nodeEngine;
    private final MultiMapPartitionContainer[] partitionContainers;
    private final ConcurrentMap<String, LocalMultiMapStatsImpl> statsMap = createConcurrentHashMap(STATS_MAP_INITIAL_CAPACITY);
    private final ConstructorFunction<String, LocalMultiMapStatsImpl> localMultiMapStatsConstructorFunction
            = new ConstructorFunction<String, LocalMultiMapStatsImpl>() {
        public LocalMultiMapStatsImpl createNew(String key) {
            return new LocalMultiMapStatsImpl();
        }
    };
    private final MultiMapEventsDispatcher dispatcher;
    private final MultiMapEventsPublisher publisher;
    private final QuorumService quorumService;

    private final ConcurrentMap<String, Object> quorumConfigCache = new ConcurrentHashMap<String, Object>();
    private final ContextMutexFactory quorumConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> quorumConfigConstructor = new ConstructorFunction<String, Object>() {
        @Override
        public Object createNew(String name) {
            MultiMapConfig multiMapConfig = nodeEngine.getConfig().findMultiMapConfig(name);
            String quorumName = multiMapConfig.getQuorumName();
            return quorumName == null ? NULL_OBJECT : quorumName;
        }
    };

    public MultiMapService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.partitionContainers = new MultiMapPartitionContainer[partitionCount];
        this.dispatcher = new MultiMapEventsDispatcher(this, nodeEngine.getClusterService());
        this.publisher = new MultiMapEventsPublisher(nodeEngine);
        this.quorumService = nodeEngine.getQuorumService();
    }

    @Override
    public void init(final NodeEngine nodeEngine, Properties properties) {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int partition = 0; partition < partitionCount; partition++) {
            partitionContainers[partition] = new MultiMapPartitionContainer(this, partition);
        }
        LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            lockService.registerLockStoreConstructor(SERVICE_NAME, new ConstructorFunction<ObjectNamespace, LockStoreInfo>() {
                @Override
                public LockStoreInfo createNew(ObjectNamespace key) {
                    String name = key.getObjectName();
                    final MultiMapConfig multiMapConfig = nodeEngine.getConfig().findMultiMapConfig(name);
                    return new LockStoreInfo() {
                        @Override
                        public int getBackupCount() {
                            return multiMapConfig.getBackupCount();
                        }

                        @Override
                        public int getAsyncBackupCount() {
                            return multiMapConfig.getAsyncBackupCount();
                        }
                    };
                }
            });
        }
    }

    @Override
    public void reset() {
        for (MultiMapPartitionContainer container : partitionContainers) {
            if (container != null) {
                container.destroy();
            }
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
        for (int i = 0; i < partitionContainers.length; i++) {
            partitionContainers[i] = null;
        }
    }

    public MultiMapContainer getOrCreateCollectionContainer(int partitionId, String name) {
        return partitionContainers[partitionId].getOrCreateMultiMapContainer(name);
    }

    public MultiMapContainer getOrCreateCollectionContainerWithoutAccess(int partitionId, String name) {
        return partitionContainers[partitionId].getOrCreateMultiMapContainer(name, false);
    }

    public MultiMapPartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        MultiMapConfig multiMapConfig = nodeEngine.getConfig().findMultiMapConfig(name);
        checkMultiMapConfig(multiMapConfig, nodeEngine.getSplitBrainMergePolicyProvider());

        return new ObjectMultiMapProxy(multiMapConfig, this, nodeEngine, name);
    }

    @Override
    public void destroyDistributedObject(String name) {
        for (MultiMapPartitionContainer container : partitionContainers) {
            if (container != null) {
                container.destroyMultiMap(name);
            }
        }
        nodeEngine.getEventService().deregisterAllListeners(SERVICE_NAME, name);
        quorumConfigCache.remove(name);
    }

    public Set<Data> localKeySet(String name) {
        Set<Data> keySet = new HashSet<Data>();
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            IPartition partition = nodeEngine.getPartitionService().getPartition(i);
            MultiMapPartitionContainer partitionContainer = getPartitionContainer(i);
            MultiMapContainer multiMapContainer = partitionContainer.getMultiMapContainer(name);
            if (multiMapContainer == null) {
                continue;
            }
            if (partition.isLocal()) {
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

    public void publishMultiMapEvent(String mapName, EntryEventType eventType, int numberOfEntriesAffected) {
        publisher.publishMultiMapEvent(mapName, eventType, numberOfEntriesAffected);

    }

    public final void publishEntryEvent(String multiMapName, EntryEventType eventType, Data key, Object newValue,
                                        Object oldValue) {
        publisher.publishEntryEvent(multiMapName, eventType, key, newValue, oldValue);
    }

    public String addListener(String name, EventListener listener, Data key, boolean includeValue, boolean local) {
        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration;
        MultiMapEventFilter filter = new MultiMapEventFilter(includeValue, key);
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

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        MultiMapPartitionContainer partitionContainer = partitionContainers[event.getPartitionId()];
        if (partitionContainer == null) {
            return null;
        }
        return partitionContainer.getAllNamespaces(event.getReplicaIndex());
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return namespace instanceof ObjectNamespace && SERVICE_NAME.equals(namespace.getServiceName());
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        MultiMapPartitionContainer partitionContainer = partitionContainers[event.getPartitionId()];
        if (partitionContainer == null) {
            return null;
        }
        return prepareReplicationOperation(event, partitionContainer.getAllNamespaces(event.getReplicaIndex()));
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event, Collection<ServiceNamespace> namespaces) {
        MultiMapPartitionContainer partitionContainer = partitionContainers[event.getPartitionId()];
        if (partitionContainer == null) {
            return null;
        }

        int replicaIndex = event.getReplicaIndex();
        Map<String, Map<Data, MultiMapValue>> map = createHashMap(namespaces.size());

        for (ServiceNamespace namespace : namespaces) {
            assert isKnownServiceNamespace(namespace) : namespace + " is not a MultiMapService namespace!";

            ObjectNamespace ns = (ObjectNamespace) namespace;
            MultiMapContainer container = partitionContainer.containerMap.get(ns.getObjectName());
            if (container == null) {
                continue;
            }
            if (container.getConfig().getTotalBackupCount() < replicaIndex) {
                continue;
            }
            map.put(ns.getObjectName(), container.getMultiMapValues());
        }

        return map.isEmpty() ? null : new MultiMapReplicationOperation(map);
    }

    public void insertMigratedData(int partitionId, Map<String, Map<Data, MultiMapValue>> map) {
        for (Map.Entry<String, Map<Data, MultiMapValue>> entry : map.entrySet()) {
            String name = entry.getKey();
            MultiMapContainer container = getOrCreateCollectionContainerWithoutAccess(partitionId, name);
            Map<Data, MultiMapValue> collections = entry.getValue();
            long maxRecordId = -1;

            for (Map.Entry<Data, MultiMapValue> multiMapValueEntry : collections.entrySet()) {
                MultiMapValue multiMapValue = multiMapValueEntry.getValue();
                container.getMultiMapValues().put(multiMapValueEntry.getKey(), multiMapValue);
                long recordId = getMaxRecordId(multiMapValue);
                maxRecordId = max(maxRecordId, recordId);
            }
            container.setId(maxRecordId);
        }
    }

    private long getMaxRecordId(MultiMapValue multiMapValue) {
        long maxRecordId = -1;
        for (MultiMapRecord record : multiMapValue.getCollection(false)) {
            maxRecordId = max(maxRecordId, record.getRecordId());
        }
        return maxRecordId;
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearMapsHavingLesserBackupCountThan(event.getPartitionId(), event.getNewReplicaIndex());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearMapsHavingLesserBackupCountThan(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
    }

    private void clearMapsHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        MultiMapPartitionContainer partitionContainer = partitionContainers[partitionId];
        if (partitionContainer == null) {
            return;
        }

        ConcurrentMap<String, MultiMapContainer> containerMap = partitionContainer.containerMap;
        if (thresholdReplicaIndex < 0) {
            for (MultiMapContainer container : containerMap.values()) {
                container.destroy();
            }
            containerMap.clear();
            return;
        }

        Iterator<MultiMapContainer> iterator = containerMap.values().iterator();
        while (iterator.hasNext()) {
            MultiMapContainer container = iterator.next();
            if (thresholdReplicaIndex > container.getConfig().getTotalBackupCount()) {
                container.destroy();
                iterator.remove();
            }
        }
    }

    public LocalMultiMapStats createStats(String name) {
        LocalMultiMapStatsImpl stats = getLocalMultiMapStatsImpl(name);
        long ownedEntryCount = 0;
        long backupEntryCount = 0;
        long hits = 0;
        long lockedEntryCount = 0;
        long lastAccessTime = 0;
        long lastUpdateTime = 0;
        ClusterService clusterService = nodeEngine.getClusterService();
        MultiMapConfig config = nodeEngine.getConfig().findMultiMapConfig(name);
        int backupCount = config.getTotalBackupCount();

        Address thisAddress = clusterService.getThisAddress();
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            IPartition partition = nodeEngine.getPartitionService().getPartition(i, false);
            MultiMapPartitionContainer partitionContainer = getPartitionContainer(i);
            MultiMapContainer multiMapContainer = partitionContainer.getMultiMapContainer(name);
            if (multiMapContainer == null) {
                continue;
            }
            Address owner = partition.getOwnerOrNull();
            if (owner != null) {
                if (owner.equals(thisAddress)) {
                    lockedEntryCount += multiMapContainer.getLockedCount();
                    lastAccessTime = max(lastAccessTime, multiMapContainer.getLastAccessTime());
                    lastUpdateTime = max(lastUpdateTime, multiMapContainer.getLastUpdateTime());
                    for (MultiMapValue multiMapValue : multiMapContainer.getMultiMapValues().values()) {
                        hits += multiMapValue.getHits();
                        ownedEntryCount += multiMapValue.getCollection(false).size();
                    }
                } else {
                    for (int j = 1; j <= backupCount; j++) {
                        // wait if the partition table is not updated yet
                        Address replicaAddress = getReplicaAddress(partition, backupCount, j);

                        if (replicaAddress != null && replicaAddress.equals(thisAddress)) {
                            for (MultiMapValue multiMapValue : multiMapContainer.getMultiMapValues().values()) {
                                backupEntryCount += multiMapValue.getCollection(false).size();
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
        stats.setBackupCount(backupCount);
        stats.setLastAccessTime(lastAccessTime);
        stats.setLastUpdateTime(lastUpdateTime);
        return stats;
    }

    public LocalMultiMapStatsImpl getLocalMultiMapStatsImpl(String name) {
        return getOrPutIfAbsent(statsMap, name, localMultiMapStatsConstructorFunction);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends TransactionalObject> T createTransactionalObject(String name, Transaction transaction) {
        return (T) new TransactionalMultiMapProxy(nodeEngine, this, name, transaction);
    }

    @Override
    public void rollbackTransaction(String transactionId) {

    }

    @Override
    public void dispatchEvent(EventData event, EntryListener listener) {
        dispatcher.dispatchEvent(event, listener);
    }

    @Override
    public Map<String, LocalMultiMapStats> getStats() {
        Map<String, LocalMultiMapStats> multiMapStats = new HashMap<String, LocalMultiMapStats>();
        for (MultiMapPartitionContainer partitionContainer : partitionContainers) {
            for (String name : partitionContainer.containerMap.keySet()) {
                if (!multiMapStats.containsKey(name)) {
                    multiMapStats.put(name, createStats(name));
                }
            }
        }
        return multiMapStats;
    }

    private Address getReplicaAddress(IPartition partition, int backupCount, int replicaIndex) {
        Address replicaAddress = partition.getReplicaAddress(replicaIndex);
        int tryCount = REPLICA_ADDRESS_TRY_COUNT;
        int maxAllowedBackupCount = min(backupCount, nodeEngine.getPartitionService().getMaxAllowedBackupCount());

        while (maxAllowedBackupCount > replicaIndex && replicaAddress == null && tryCount-- > 0) {
            try {
                Thread.sleep(REPLICA_ADDRESS_SLEEP_WAIT_MILLIS);
            } catch (InterruptedException e) {
                currentThread().interrupt();
                throw ExceptionUtil.rethrow(e);
            }
            replicaAddress = partition.getReplicaAddress(replicaIndex);
        }
        return replicaAddress;
    }

    @Override
    public String getQuorumName(String name) {
        // RU_COMPAT_3_9
        if (nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_10)) {
            return null;
        }
        Object quorumName = getOrPutSynchronized(quorumConfigCache, name, quorumConfigCacheMutexFactory,
                quorumConfigConstructor);
        return quorumName == NULL_OBJECT ? null : (String) quorumName;
    }

    public void ensureQuorumPresent(String distributedObjectName, QuorumType requiredQuorumPermissionType) {
        quorumService.ensureQuorumPresent(getQuorumName(distributedObjectName), requiredQuorumPermissionType);
    }

    @Override
    public Runnable prepareMergeRunnable() {
        MultiMapContainerCollector collector = new MultiMapContainerCollector(nodeEngine, partitionContainers);
        collector.run();
        return new Merger(collector);
    }

    private class Merger extends AbstractContainerMerger<MultiMapContainer, Collection<Object>, MultiMapMergeTypes> {

        Merger(MultiMapContainerCollector collector) {
            super(collector, nodeEngine);
        }

        @Override
        protected String getLabel() {
            return "MultiMap";
        }

        @Override
        public void runInternal() {
            for (Map.Entry<Integer, Collection<MultiMapContainer>> entry : collector.getCollectedContainers().entrySet()) {
                int partitionId = entry.getKey();
                Collection<MultiMapContainer> containers = entry.getValue();

                for (MultiMapContainer container : containers) {
                    String name = container.getObjectNamespace().getObjectName();
                    SplitBrainMergePolicy<Collection<Object>, MultiMapMergeTypes> mergePolicy
                            = getMergePolicy(container.getConfig().getMergePolicyConfig());
                    int batchSize = container.getConfig().getMergePolicyConfig().getBatchSize();

                    List<MultiMapMergeContainer> mergeContainers = new ArrayList<MultiMapMergeContainer>(batchSize);
                    for (Map.Entry<Data, MultiMapValue> multiMapValueEntry : container.getMultiMapValues().entrySet()) {
                        Data key = multiMapValueEntry.getKey();
                        MultiMapValue multiMapValue = multiMapValueEntry.getValue();
                        Collection<MultiMapRecord> records = multiMapValue.getCollection(false);

                        MultiMapMergeContainer mergeContainer = new MultiMapMergeContainer(key, records,
                                container.getCreationTime(), container.getLastAccessTime(), container.getLastUpdateTime(),
                                multiMapValue.getHits());
                        mergeContainers.add(mergeContainer);

                        if (mergeContainers.size() == batchSize) {
                            sendBatch(partitionId, name, mergePolicy, mergeContainers);
                            mergeContainers = new ArrayList<MultiMapMergeContainer>(batchSize);
                        }
                    }
                    if (mergeContainers.size() > 0) {
                        sendBatch(partitionId, name, mergePolicy, mergeContainers);
                    }
                }
            }
        }

        private void sendBatch(int partitionId, String name,
                               SplitBrainMergePolicy<Collection<Object>, MultiMapMergeTypes> mergePolicy,
                               List<MultiMapMergeContainer> mergeContainers) {
            MergeOperation operation = new MergeOperation(name, mergeContainers, mergePolicy);
            invoke(SERVICE_NAME, operation, partitionId);
        }
    }
}
