/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.monitor.impl.LocalReplicatedMapStatsImpl;
import com.hazelcast.internal.namespace.NamespaceUtil;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.internal.services.SplitBrainProtectionAwareService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ContextMutexFactory;
import com.hazelcast.replicatedmap.LocalReplicatedMapStats;
import com.hazelcast.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;
import com.hazelcast.replicatedmap.impl.iterator.ReplicatedMapIterationService;
import com.hazelcast.replicatedmap.impl.operation.CheckReplicaVersionOperation;
import com.hazelcast.replicatedmap.impl.operation.ReplicationOperation;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.spi.impl.eventservice.impl.TrueEventFilter;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.internal.config.ConfigValidator.checkReplicatedMapConfig;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.REPLICATED_MAP_PREFIX;
import static com.hazelcast.internal.metrics.impl.ProviderHelper.provide;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * This is the main service implementation to handle proxy creation, event publishing, migration, anti-entropy and
 * manages the backing {@link PartitionContainer}s that actually hold the data
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
public class ReplicatedMapService implements ManagedService, RemoteService, EventPublishingService<Object, Object>,
                                             MigrationAwareService, SplitBrainHandlerService,
                                             StatisticsAwareService<LocalReplicatedMapStats>,
                                             SplitBrainProtectionAwareService, DynamicMetricsProvider {
    public static final String SERVICE_NAME = "hz:impl:replicatedMapService";
    public static final int INVOCATION_TRY_COUNT = 3;

    private static final int SYNC_INTERVAL_SECONDS = 30;

    private static final Object NULL_OBJECT = new Object();

    private final AntiEntropyTask antiEntropyTask = new AntiEntropyTask();

    private final ConcurrentMap<String, Object> splitBrainProtectionConfigCache = new ConcurrentHashMap<>();
    private final ContextMutexFactory splitBrainProtectionConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> splitBrainProtectionConfigConstructor =
            new ConstructorFunction<>() {
        @Override
        public Object createNew(String name) {
            ReplicatedMapConfig lockConfig = nodeEngine.getConfig().findReplicatedMapConfig(name);
            String splitBrainProtectionName = lockConfig.getSplitBrainProtectionName();
            return splitBrainProtectionName == null ? NULL_OBJECT : splitBrainProtectionName;
        }
    };

    private final Config config;
    private final NodeEngine nodeEngine;
    private final PartitionContainer[] partitionContainers;
    private final InternalPartitionServiceImpl partitionService;
    private final ClusterService clusterService;
    private final OperationService operationService;
    private final SplitBrainProtectionService splitBrainProtectionService;
    private final ReplicatedMapEventPublishingService eventPublishingService;
    private final ReplicatedMapSplitBrainHandlerService splitBrainHandlerService;
    private final ReplicatedMapIterationService iterationService;
    private final LocalReplicatedMapStatsProvider statsProvider;
    private final SplitBrainMergePolicyProvider mergePolicyProvider;

    private ScheduledFuture antiEntropyFuture;

    public ReplicatedMapService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.config = nodeEngine.getConfig();
        this.partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        this.clusterService = nodeEngine.getClusterService();
        this.operationService = nodeEngine.getOperationService();
        this.partitionContainers = new PartitionContainer[nodeEngine.getPartitionService().getPartitionCount()];
        this.eventPublishingService = new ReplicatedMapEventPublishingService(this);
        this.splitBrainHandlerService = new ReplicatedMapSplitBrainHandlerService(this);
        this.iterationService = new ReplicatedMapIterationService(this, nodeEngine.getSerializationService(), nodeEngine);
        this.splitBrainProtectionService = nodeEngine.getSplitBrainProtectionService();
        this.mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        this.statsProvider = new LocalReplicatedMapStatsProvider(config, partitionContainers);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            partitionContainers[i] = new PartitionContainer(this, i);
        }
        antiEntropyFuture = nodeEngine.getExecutionService().getGlobalTaskScheduler()
                .scheduleWithRepetition(antiEntropyTask, 0, SYNC_INTERVAL_SECONDS, TimeUnit.SECONDS);

        boolean dsMetricsEnabled = nodeEngine.getProperties().getBoolean(ClusterProperty.METRICS_DATASTRUCTURES);
        if (dsMetricsEnabled) {
            nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(this);
        }
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
        this.iterationService.shutdown();
    }

    /**
     * Gets the {@link LocalReplicatedMapStatsImpl} implementation of {@link LocalReplicatedMapStats} for the provided
     * {@code name} of the replicated map. This is used for operations that mutate replicated map's local statistics.
     *
     * @param name of the replicated map.
     * @return replicated map's local statistics object.
     */
    public LocalReplicatedMapStatsImpl getLocalReplicatedMapStatsImpl(String name) {
        return statsProvider.getLocalReplicatedMapStatsImpl(name);
    }

    /**
     * Gets the replicated map's local statistics. If the statistics is disabled so method returns always the same object which is
     * empty and immutable.
     *
     * @param name of the replicated map.
     * @return replicated map's local statistics object.
     */
    public LocalReplicatedMapStats getLocalReplicatedMapStats(String name) {
        return statsProvider.getLocalReplicatedMapStats(name);
    }

    @Override
    public DistributedObject createDistributedObject(String objectName, UUID source, boolean local) {
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
    public void destroyDistributedObject(String objectName, boolean local) {
        if (nodeEngine.getLocalMember().isLiteMember()) {
            return;
        }

        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            partitionContainers[i].destroy(objectName);
        }
        splitBrainProtectionConfigCache.remove(objectName);
    }

    @Override
    public void dispatchEvent(Object event, Object listener) {
        eventPublishingService.dispatchEvent(event, listener);
    }

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
        ArrayList<ReplicatedRecordStore> stores = new ArrayList<>(partitionCount);
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
        Collection<Address> addresses = new ArrayList<>(members.size());
        for (Member member : members) {
            addresses.add(member.getAddress());
        }
        return addresses;
    }

    public void initializeListeners(String name) {
        ReplicatedMapConfig mapConfig = getReplicatedMapConfig(name);
        List<ListenerConfig> listenerConfigs = mapConfig.getListenerConfigs();
        for (ListenerConfig listenerConfig : listenerConfigs) {
            EntryListener listener = null;
            if (listenerConfig.getImplementation() != null) {
                listener = (EntryListener) listenerConfig.getImplementation();
            } else if (listenerConfig.getClassName() != null) {
                try {
                    ClassLoader loader = NamespaceUtil.getClassLoaderForNamespace(nodeEngine, mapConfig.getUserCodeNamespace());
                    listener = ClassLoaderUtil.newInstance(loader, listenerConfig.getClassName());
                } catch (Exception e) {
                    throw rethrow(e);
                }
            }
            if (listener != null) {
                if (listener instanceof HazelcastInstanceAware aware) {
                    aware.setHazelcastInstance(nodeEngine.getHazelcastInstance());
                }
                eventPublishingService.addLocalEventListener(listener, TrueEventFilter.INSTANCE, name);
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

    public ReplicatedMapIterationService getIterationService() {
        return iterationService;
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
        Map<String, LocalReplicatedMapStats> mapStats = new HashMap<>(maps.size());
        for (String map : maps) {
            mapStats.put(map, getLocalReplicatedMapStats(map));
        }
        return mapStats;
    }

    @Override
    public String getSplitBrainProtectionName(String name) {
        Object splitBrainProtectionName = getOrPutSynchronized(splitBrainProtectionConfigCache, name,
                splitBrainProtectionConfigCacheMutexFactory, splitBrainProtectionConfigConstructor);
        return splitBrainProtectionName == NULL_OBJECT ? null : (String) splitBrainProtectionName;
    }

    public void ensureNoSplitBrain(String distributedObjectName,
                                   SplitBrainProtectionOn requiredSplitBrainProtectionPermissionType) {
        splitBrainProtectionService.ensureNoSplitBrain(getSplitBrainProtectionName(distributedObjectName),
                requiredSplitBrainProtectionPermissionType);
    }

    // needed for a test
    public void triggerAntiEntropy() {
        antiEntropyTask.triggerAntiEntropy();
    }

    public Object getMergePolicy(String name) {
        var replicatedMapConfig = getReplicatedMapConfig(name);
        return mergePolicyProvider.getMergePolicy(
                replicatedMapConfig.getMergePolicyConfig().getPolicy(),
                replicatedMapConfig.getUserCodeNamespace()
        );
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        provide(descriptor, context, REPLICATED_MAP_PREFIX, getStats());
    }

    /**
     * Looks up the User Code Namespace name associated with the specified replicated map name. This is done
     * by checking the Node's config tree directly.
     *
     * @param engine  {@link NodeEngine} implementation of this member for service and config lookups
     * @param mapName The name of the {@link com.hazelcast.replicatedmap.ReplicatedMap} to lookup for
     * @return the Namespace Name if found, or {@code null} otherwise.
     */
    public static String lookupNamespace(NodeEngine engine, String mapName) {
        if (engine.getNamespaceService().isEnabled()) {
            // No regular containers available, fallback to config
            ReplicatedMapConfig config = engine.getConfig().findReplicatedMapConfig(mapName);
            if (config != null) {
                return config.getUserCodeNamespace();
            }
        }
        return null;
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
            Collection<Address> addresses = new ArrayList<>(getMemberAddresses(DATA_MEMBER_SELECTOR));
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
