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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.monitor.LocalReplicatedMapStats;
import com.hazelcast.monitor.impl.LocalReplicatedMapStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.quorum.QuorumService;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import com.hazelcast.replicatedmap.impl.operation.CheckReplicaVersionOperation;
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
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ContextMutexFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.internal.config.ConfigValidator.checkReplicatedMapConfig;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.Math.max;

/**
 * This is the main service implementation to handle proxy creation, event publishing, migration, anti-entropy and
 * manages the backing {@link PartitionContainer}s that actually hold the data
 */
public class ReplicatedMapService implements ManagedService, RemoteService, EventPublishingService<Object, Object>,
        MigrationAwareService, SplitBrainHandlerService, StatisticsAwareService<LocalReplicatedMapStats>, QuorumAwareService {

    public static final String SERVICE_NAME = "hz:impl:replicatedMapService";
    public static final int INVOCATION_TRY_COUNT = 3;

    private static final int SYNC_INTERVAL_SECONDS = 30;

    private static final Object NULL_OBJECT = new Object();

    private final AntiEntropyTask antiEntropyTask = new AntiEntropyTask();

    private final ConcurrentMap<String, Object> quorumConfigCache = new ConcurrentHashMap<String, Object>();
    private final ContextMutexFactory quorumConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> quorumConfigConstructor = new ConstructorFunction<String, Object>() {
        @Override
        public Object createNew(String name) {
            ReplicatedMapConfig lockConfig = nodeEngine.getConfig().findReplicatedMapConfig(name);
            String quorumName = lockConfig.getQuorumName();
            return quorumName == null ? NULL_OBJECT : quorumName;
        }
    };

    private final ConcurrentHashMap<String, LocalReplicatedMapStatsImpl> statsMap =
            new ConcurrentHashMap<String, LocalReplicatedMapStatsImpl>();
    private final ConstructorFunction<String, LocalReplicatedMapStatsImpl> statsConstructorFunction =
            new ConstructorFunction<String, LocalReplicatedMapStatsImpl>() {
                @Override
                public LocalReplicatedMapStatsImpl createNew(String arg) {
                    return new LocalReplicatedMapStatsImpl();
                }
            };

    private final Config config;
    private final NodeEngine nodeEngine;
    private final PartitionContainer[] partitionContainers;
    private final InternalPartitionServiceImpl partitionService;
    private final ClusterService clusterService;
    private final OperationService operationService;
    private final QuorumService quorumService;
    private final ReplicatedMapEventPublishingService eventPublishingService;
    private final ReplicatedMapSplitBrainHandlerService splitBrainHandlerService;

    private ScheduledFuture antiEntropyFuture;
    private MergePolicyProvider mergePolicyProvider;

    public ReplicatedMapService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.config = nodeEngine.getConfig();
        this.partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        this.clusterService = nodeEngine.getClusterService();
        this.operationService = nodeEngine.getOperationService();
        this.partitionContainers = new PartitionContainer[nodeEngine.getPartitionService().getPartitionCount()];
        this.eventPublishingService = new ReplicatedMapEventPublishingService(this);
        this.splitBrainHandlerService = new ReplicatedMapSplitBrainHandlerService(this);
        this.quorumService = nodeEngine.getQuorumService();
        this.mergePolicyProvider = new MergePolicyProvider(nodeEngine);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            partitionContainers[i] = new PartitionContainer(this, i);
        }
        antiEntropyFuture = nodeEngine.getExecutionService().getGlobalTaskScheduler()
                .scheduleWithRepetition(antiEntropyTask, 0, SYNC_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void reset() {
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            ConcurrentMap<String, ReplicatedRecordStore> stores = partitionContainers[i].getStores();
            for (ReplicatedRecordStore store : stores.values()) {
                store.reset();
            }
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        for (PartitionContainer container : partitionContainers) {
            if (container != null) {
                container.shutdown();
            }
        }
        if (antiEntropyFuture != null) {
            antiEntropyFuture.cancel(true);
        }
    }

    public LocalReplicatedMapStatsImpl getLocalMapStatsImpl(String name) {
        return getOrPutIfAbsent(statsMap, name, statsConstructorFunction);
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
                stats.setLastAccessTime(max(stats.getLastAccessTime(), record.getLastAccessTime()));
                stats.setLastUpdateTime(max(stats.getLastUpdateTime(), record.getUpdateTime()));
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
        ReplicatedMapConfig replicatedMapConfig = getReplicatedMapConfig(objectName);
        checkReplicatedMapConfig(replicatedMapConfig, mergePolicyProvider);
        if (nodeEngine.getLocalMember().isLiteMember()) {
            throw new ReplicatedMapCantBeCreatedOnLiteMemberException(nodeEngine.getThisAddress());
        }

        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            PartitionContainer partitionContainer = partitionContainers[i];
            if (partitionContainer == null) {
                continue;
            }
            partitionContainer.getOrCreateRecordStore(objectName);
        }
        return new ReplicatedMapProxy(nodeEngine, objectName, this, replicatedMapConfig);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        if (nodeEngine.getLocalMember().isLiteMember()) {
            return;
        }

        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            partitionContainers[i].destroy(objectName);
        }
        quorumConfigCache.remove(objectName);
    }

    @Override
    public void dispatchEvent(Object event, Object listener) {
        eventPublishingService.dispatchEvent(event, listener);
    }

    @SuppressWarnings("deprecation")
    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        return config.findReplicatedMapConfig(name);
    }

    public ReplicatedRecordStore getReplicatedRecordStore(String name, boolean create, Object key) {
        return getReplicatedRecordStore(name, create, partitionService.getPartitionId(key));
    }

    public ReplicatedRecordStore getReplicatedRecordStore(String name, boolean create, int partitionId) {
        if (nodeEngine.getLocalMember().isLiteMember()) {
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
        List<ListenerConfig> listenerConfigs = getReplicatedMapConfig(name).getListenerConfigs();
        for (ListenerConfig listenerConfig : listenerConfigs) {
            EntryListener listener = null;
            if (listenerConfig.getImplementation() != null) {
                listener = (EntryListener) listenerConfig.getImplementation();
            } else if (listenerConfig.getClassName() != null) {
                try {
                    listener = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(),
                            listenerConfig.getClassName());
                } catch (Exception e) {
                    throw rethrow(e);
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
        if (nodeEngine.getLocalMember().isLiteMember()) {
            return null;
        }
        if (event.getReplicaIndex() > 0) {
            return null;
        }

        PartitionContainer container = partitionContainers[event.getPartitionId()];
        SerializationService serializationService = nodeEngine.getSerializationService();
        ReplicationOperation operation = new ReplicationOperation(serializationService, container, event.getPartitionId());
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
        return splitBrainHandlerService.prepareMergeRunnable();
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

    @Override
    public String getQuorumName(String name) {
        Object quorumName = getOrPutSynchronized(quorumConfigCache, name, quorumConfigCacheMutexFactory, quorumConfigConstructor);
        return quorumName == NULL_OBJECT ? null : (String) quorumName;
    }

    public void ensureQuorumPresent(String distributedObjectName, QuorumType requiredQuorumPermissionType) {
        quorumService.ensureQuorumPresent(getQuorumName(distributedObjectName), requiredQuorumPermissionType);
    }

    // needed for a test
    public void triggerAntiEntropy() {
        antiEntropyTask.triggerAntiEntropy();
    }

    public Object getMergePolicy(String name) {
        MergePolicyConfig mergePolicyConfig = getReplicatedMapConfig(name).getMergePolicyConfig();
        return mergePolicyProvider.getMergePolicy(mergePolicyConfig.getPolicy());
    }

    private class AntiEntropyTask implements Runnable {

        @Override
        public void run() {
            triggerAntiEntropy();
        }

        /**
         * Sends an operation to all replicas to check their replica versions for all partitions for which this node is the owner.
         */
        void triggerAntiEntropy() {
            if (nodeEngine.getLocalMember().isLiteMember() || clusterService.getSize(DATA_MEMBER_SELECTOR) == 1) {
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
                    Operation operation = new CheckReplicaVersionOperation(partitionContainer)
                            .setPartitionId(i)
                            .setValidateTarget(false);
                    operationService.createInvocationBuilder(SERVICE_NAME, operation, address)
                            .setTryCount(INVOCATION_TRY_COUNT)
                            .invoke();
                }
            }
        }
    }
}
