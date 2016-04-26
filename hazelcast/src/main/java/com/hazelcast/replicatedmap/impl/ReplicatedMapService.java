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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.monitor.LocalReplicatedMapStats;
import com.hazelcast.monitor.impl.LocalReplicatedMapStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import com.hazelcast.replicatedmap.impl.operation.CheckReplicaVersion;
import com.hazelcast.replicatedmap.impl.operation.ReplicationOperation;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.replicatedmap.merge.MergePolicyProvider;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;

/**
 * This is the main service implementation to handle proxy creation, event publishing, migration, anti-entropy and
 * manages the backing {@link PartitionContainer}s that actually hold the data
 */
public class ReplicatedMapService implements ManagedService, RemoteService, EventPublishingService<Object, Object>,
        MigrationAwareService, SplitBrainHandlerService, StatisticsAwareService {

    public static final String SERVICE_NAME = "hz:impl:replicatedMapService";
    public static final int INVOCATION_TRY_COUNT = 3;

    private static final int SYNC_INTERVAL_SECONDS = 30;

    private final Config config;
    private final NodeEngine nodeEngine;
    private final PartitionContainer[] partitionContainers;
    private final InternalPartitionServiceImpl partitionService;
    private final ClusterServiceImpl clusterService;
    private final OperationService operationService;
    private final ReplicatedMapEventPublishingService eventPublishingService;
    private final MergePolicyProvider mergePolicyProvider;
    private final ReplicatedMapSplitBrainHandlerService replicatedMapSplitBrainHandlerService;
    private ConcurrentHashMap<String, LocalReplicatedMapStatsImpl> statsMap =
            new ConcurrentHashMap<String, LocalReplicatedMapStatsImpl>();
    private ConstructorFunction<String, LocalReplicatedMapStatsImpl> constructorFunction =
            new ConstructorFunction<String, LocalReplicatedMapStatsImpl>() {
                @Override
                public LocalReplicatedMapStatsImpl createNew(String arg) {
                    return new LocalReplicatedMapStatsImpl();
                }
            };

    public ReplicatedMapService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.config = nodeEngine.getConfig();
        this.partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        this.clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
        this.operationService = nodeEngine.getOperationService();
        this.partitionContainers = new PartitionContainer[nodeEngine.getPartitionService().getPartitionCount()];
        this.eventPublishingService = new ReplicatedMapEventPublishingService(this);
        this.mergePolicyProvider = new MergePolicyProvider(nodeEngine);
        this.replicatedMapSplitBrainHandlerService = new ReplicatedMapSplitBrainHandlerService(this,
                mergePolicyProvider);
    }

    @Override
    public void init(final NodeEngine nodeEngine, Properties properties) {
        if (config.isLiteMember()) {
            return;
        }

        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            partitionContainers[i] = new PartitionContainer(this, i);
        }
        nodeEngine.getExecutionService().getGlobalTaskScheduler().scheduleWithRepetition(new Runnable() {
            @Override
            public void run() {
                if (clusterService.getSize() == 1) {
                    return;
                }
                Collection<Address> addresses = new ArrayList<Address>(getMemberAddresses(DATA_MEMBER_SELECTOR));
                addresses.remove(nodeEngine.getThisAddress());
                for (int i = 0; i < partitionContainers.length; i++) {
                    Address thisAddress = nodeEngine.getThisAddress();
                    InternalPartition partition = partitionService.getPartition(i, false);
                    Address ownerAddress = partition.getOwnerOrNull();
                    if (!thisAddress.equals(ownerAddress)) {
                        continue;
                    }
                    PartitionContainer partitionContainer = partitionContainers[i];
                    if (partitionContainer.isEmpty()) {
                        continue;
                    }
                    for (Address address : addresses) {
                        CheckReplicaVersion checkReplicaVersion = new CheckReplicaVersion(partitionContainer);
                        checkReplicaVersion.setPartitionId(i);
                        checkReplicaVersion.setValidateTarget(false);
                        operationService.createInvocationBuilder(SERVICE_NAME, checkReplicaVersion, address)
                                .setTryCount(INVOCATION_TRY_COUNT)
                                .invoke();
                    }
                }
            }
        }, 0, SYNC_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void reset() {
        if (config.isLiteMember()) {
            return;
        }

        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            ConcurrentMap<String, ReplicatedRecordStore> stores = partitionContainers[i].getStores();
            for (ReplicatedRecordStore store : stores.values()) {
                store.reset();
            }
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        if (config.isLiteMember()) {
            return;
        }

        for (PartitionContainer container : partitionContainers) {
            container.shutdown();
        }
    }

    public LocalReplicatedMapStatsImpl getLocalMapStatsImpl(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, constructorFunction);
    }

    public LocalReplicatedMapStatsImpl createReplicatedMapStats(String name) {
        LocalReplicatedMapStatsImpl stats = getLocalMapStatsImpl(name);
        long hits = 0;
        long count = 0;
        long memoryUsage = 0;
        boolean isBinary = (getReplicatedMapConfig(name).getInMemoryFormat() == InMemoryFormat.BINARY);
        for (PartitionContainer container : partitionContainers) {
            ReplicatedRecordStore store = container.getRecordStore(name);
            if (store == null) {
                continue;
            }
            Iterator<ReplicatedRecord> iterator = store.recordIterator();
            while (iterator.hasNext()) {
                ReplicatedRecord record = iterator.next();
                stats.setLastAccessTime(Math.max(stats.getLastAccessTime(), record.getLastAccessTime()));
                stats.setLastUpdateTime(Math.max(stats.getLastUpdateTime(), record.getUpdateTime()));
                hits += record.getHits();
                if (isBinary) {
                    memoryUsage += ((HeapData) record.getValueInternal()).getHeapCost();
                }
                count++;
            }
        }
        stats.setOwnedEntryCount(count);
        stats.setHits(hits);
        stats.setOwnedEntryMemoryCost(memoryUsage);
        return stats;
    }


    @Override
    public DistributedObject createDistributedObject(String objectName) {
        if (config.isLiteMember()) {
            throw new ReplicatedMapCantBeCreatedOnLiteMemberException(nodeEngine.getThisAddress());
        }
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            PartitionContainer partitionContainer = partitionContainers[i];
            if (partitionContainer == null) {
                continue;
            }
            partitionContainer.getOrCreateRecordStore(objectName);
        }
        return new ReplicatedMapProxy(nodeEngine, objectName, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        if (config.isLiteMember()) {
            return;
        }

        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            partitionContainers[i].destroy(objectName);
        }
    }

    @Override
    public void dispatchEvent(Object event, Object listener) {
        eventPublishingService.dispatchEvent(event, listener);
    }


    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        return config.getReplicatedMapConfig(name).getAsReadOnly();
    }

    public ReplicatedRecordStore getReplicatedRecordStore(String name, boolean create, Object key) {
        return getReplicatedRecordStore(name, create, partitionService.getPartitionId(key));
    }

    public ReplicatedRecordStore getReplicatedRecordStore(String name, boolean create, int partitionId) {
        if (config.isLiteMember()) {
            throw new ReplicatedMapCantBeCreatedOnLiteMemberException(nodeEngine.getThisAddress());
        }

        PartitionContainer partitionContainer = partitionContainers[partitionId];
        if (create) {
            return partitionContainer.getOrCreateRecordStore(name);
        }
        return partitionContainer.getRecordStore(name);
    }

    public Collection<ReplicatedRecordStore> getAllReplicatedRecordStores(String name) {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        ArrayList<ReplicatedRecordStore> stores = new ArrayList<ReplicatedRecordStore>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            PartitionContainer partitionContainer = partitionContainers[i];
            if (partitionContainer == null) {
                continue;
            }
            ReplicatedRecordStore recordStore = partitionContainer.getRecordStore(name);
            if (recordStore == null) {
                continue;
            }
            stores.add(recordStore);
        }
        return stores;
    }

    private Collection<Address> getMemberAddresses(MemberSelector memberSelector) {
        Collection<Member> members = clusterService.getMembers(memberSelector);
        Collection<Address> addresses = new ArrayList<Address>(members.size());
        for (Member member : members) {
            addresses.add(member.getAddress());
        }
        return addresses;
    }

    public void initializeListeners(String name) {
        List<ListenerConfig> listenerConfigs = config.getReplicatedMapConfig(name).getListenerConfigs();
        for (ListenerConfig listenerConfig : listenerConfigs) {
            EntryListener listener = null;
            if (listenerConfig.getImplementation() != null) {
                listener = (EntryListener) listenerConfig.getImplementation();
            } else if (listenerConfig.getClassName() != null) {
                try {
                    listener = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(),
                            listenerConfig.getClassName());
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }
            if (listener != null) {
                if (listener instanceof HazelcastInstanceAware) {
                    ((HazelcastInstanceAware) listener).setHazelcastInstance(nodeEngine.getHazelcastInstance());
                }
                eventPublishingService.addEventListener(listener, TrueEventFilter.INSTANCE, name);
            }
        }
    }

    public PartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public ReplicatedMapEventPublishingService getEventPublishingService() {
        return eventPublishingService;
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (config.isLiteMember()) {
            return null;
        }

        final PartitionContainer container = partitionContainers[event.getPartitionId()];
        final ReplicationOperation operation = new ReplicationOperation(nodeEngine.getSerializationService(),
                container, event.getPartitionId());
        operation.setService(this);
        return operation.isEmpty() ? null : operation;
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        // no-op
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        // no-op
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        // no-op
    }

    @Override
    public Runnable prepareMergeRunnable() {
        return replicatedMapSplitBrainHandlerService.prepareMergeRunnable();
    }

    @Override
    public Map<String, LocalReplicatedMapStats> getStats() {
        Collection<String> maps = getNodeEngine().getProxyService().getDistributedObjectNames(SERVICE_NAME);
        Map<String, LocalReplicatedMapStats> mapStats = new
                HashMap<String, LocalReplicatedMapStats>(maps.size());
        for (String map : maps) {
            mapStats.put(map, createReplicatedMapStats(map));
        }
        return mapStats;
    }

}
