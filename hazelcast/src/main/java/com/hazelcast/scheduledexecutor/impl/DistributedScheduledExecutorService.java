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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.monitor.impl.LocalExecutorStatsImpl;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.internal.services.SplitBrainProtectionAwareService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ContextMutexFactory;
import com.hazelcast.map.impl.ExecutorStats;
import com.hazelcast.scheduledexecutor.impl.operations.MergeOperation;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.merge.AbstractContainerMerger;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.ScheduledExecutorMergeTypes;
import com.hazelcast.spi.properties.ClusterProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.config.ConfigValidator.checkScheduledExecutorConfig;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.SCHEDULED_EXECUTOR_PREFIX;
import static com.hazelcast.internal.metrics.impl.ProviderHelper.provide;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.internal.util.ExceptionUtil.peel;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static java.util.Collections.newSetFromMap;
import static java.util.Collections.synchronizedSet;

/**
 * Scheduled executor service, middle-man responsible for managing Scheduled Executor containers.
 */
public class DistributedScheduledExecutorService
        implements ManagedService, RemoteService, MigrationAwareService, SplitBrainProtectionAwareService,
        SplitBrainHandlerService, MembershipAwareService, StatisticsAwareService<LocalExecutorStatsImpl>,
        DynamicMetricsProvider {

    public static final int MEMBER_BIN = -1;
    public static final String SERVICE_NAME = "hz:impl:scheduledExecutorService";
    public static final CapacityPermit NOOP_PERMIT = new NoopCapacityPermit();

    //Testing only
    static final AtomicBoolean FAIL_MIGRATIONS = new AtomicBoolean(false);

    private static final Object NULL_OBJECT = new Object();

    private final ExecutorStats executorStats = new ExecutorStats();
    private final ConcurrentMap<String, Boolean> shutdownExecutors = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, CapacityPermit> permits = new ConcurrentHashMap<>();
    private final Set<ScheduledFutureProxy> lossListeners = synchronizedSet(newSetFromMap(new WeakHashMap<>()));
    private final ConcurrentMap<String, Object> splitBrainProtectionConfigCache = new ConcurrentHashMap<>();
    private final ContextMutexFactory splitBrainProtectionConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> splitBrainProtectionConfigConstructor =
            new ConstructorFunction<String, Object>() {
                @Override
                public Object createNew(String name) {
                    ScheduledExecutorConfig executorConfig = nodeEngine.getConfig().findScheduledExecutorConfig(name);
                    String splitBrainProtectionName = executorConfig.getSplitBrainProtectionName();
                    return splitBrainProtectionName == null ? NULL_OBJECT : splitBrainProtectionName;
                }
            };

    private NodeEngine nodeEngine;
    private ScheduledExecutorPartition[] partitions;
    private ScheduledExecutorMemberBin memberBin;
    private UUID partitionLostRegistration;

    public DistributedScheduledExecutorService() {
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.nodeEngine = nodeEngine;
        this.partitions = new ScheduledExecutorPartition[partitionCount];
        boolean dsMetricsEnabled = nodeEngine.getProperties().getBoolean(ClusterProperty.METRICS_DATASTRUCTURES);
        if (dsMetricsEnabled) {
            ((NodeEngineImpl) nodeEngine).getMetricsRegistry().registerDynamicMetricsProvider(this);
        }
        reset();
    }

    public ScheduledExecutorPartition getPartition(int partitionId) {
        return partitions[partitionId];
    }

    public ScheduledExecutorContainerHolder getPartitionOrMemberBin(int id) {
        if (id == MEMBER_BIN) {
            return memberBin;
        }

        return getPartition(id);
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public ExecutorStats getExecutorStats() {
        return executorStats;
    }

    @Override
    public void reset() {
        shutdown(true);

        memberBin = new ScheduledExecutorMemberBin(nodeEngine, this);

        // Keep using the public API due to the benefit of getting events on all partitions and not just local
        if (partitionLostRegistration == null) {
            registerPartitionListener();
        }

        for (int partitionId = 0; partitionId < partitions.length; partitionId++) {
            if (partitions[partitionId] != null) {
                partitions[partitionId].destroy();
            }
            partitions[partitionId] = new ScheduledExecutorPartition(nodeEngine, this, partitionId);
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        executorStats.clear();
        shutdownExecutors.clear();
        permits.clear();

        if (memberBin != null) {
            memberBin.destroy();
        }

        lossListeners.clear();

        unRegisterPartitionListenerIfExists();

        for (ScheduledExecutorPartition partition : partitions) {
            if (partition != null) {
                partition.destroy();
            }
        }
    }

    CapacityPermit permitFor(String name, ScheduledExecutorConfig config) {
        return permits.computeIfAbsent(name, n -> new MemberCapacityPermit(n, config.getCapacity()));
    }

    void addLossListener(ScheduledFutureProxy future) {
        this.lossListeners.add(future);
    }

    @Override
    public DistributedObject createDistributedObject(String name, UUID source, boolean local) {
        ScheduledExecutorConfig executorConfig = nodeEngine.getConfig().findScheduledExecutorConfig(name);
        checkScheduledExecutorConfig(executorConfig, nodeEngine.getSplitBrainMergePolicyProvider());

        return new ScheduledExecutorServiceProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name, boolean local) {
        if (shutdownExecutors.remove(name) == null) {
            nodeEngine.getExecutionService().shutdownScheduledDurableExecutor(name);
        }

        resetPartitionOrMemberBinContainer(name);
        splitBrainProtectionConfigCache.remove(name);
    }

    public void shutdownExecutor(String name) {
        if (shutdownExecutors.putIfAbsent(name, Boolean.TRUE) == null) {
            nodeEngine.getExecutionService().shutdownScheduledDurableExecutor(name);
        }
    }

    public boolean isShutdown(String name) {
        return shutdownExecutors.containsKey(name);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        ScheduledExecutorPartition partition = partitions[partitionId];
        return partition.prepareReplicationOperation(event.getReplicaIndex());
    }

    @Override
    public Runnable prepareMergeRunnable() {
        ScheduledExecutorContainerCollector collector = new ScheduledExecutorContainerCollector(nodeEngine, partitions);
        collector.run();
        return new Merger(collector);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        if (FAIL_MIGRATIONS.getAndSet(false)) {
            throw new RuntimeException();
        }

        ScheduledExecutorPartition partition = partitions[event.getPartitionId()];
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE && event.getCurrentReplicaIndex() == 0) {
            // this is the partition owner at the beginning of the migration
            // so we suspend tasks now and promote them back if the migration
            // is rolled back
            partition.suspendTasks();
        }
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            discardReserved(partitionId, event.getNewReplicaIndex());
        } else if (event.getNewReplicaIndex() == 0) {
            ScheduledExecutorPartition partition = partitions[partitionId];
            partition.promoteSuspended();
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            discardReserved(event.getPartitionId(), event.getCurrentReplicaIndex());
        } else if (event.getCurrentReplicaIndex() == 0) {
            ScheduledExecutorPartition partition = partitions[partitionId];
            partition.promoteSuspended();
        }
    }

    private void discardReserved(int partitionId, int thresholdReplicaIndex) {
        ScheduledExecutorPartition partition = partitions[partitionId];
        partition.disposeObsoleteReplicas(thresholdReplicaIndex);
    }

    private void resetPartitionOrMemberBinContainer(String name) {
        permits.remove(name);
        if (memberBin != null) {
            memberBin.destroyContainer(name);
        }

        for (ScheduledExecutorPartition partition : partitions) {
            partition.destroyContainer(name);
        }
    }

    private void registerPartitionListener() {
        this.partitionLostRegistration =
                getNodeEngine().getPartitionService().addPartitionLostListener(event -> {
                    // use toArray before iteration since it is done under mutex
                    ScheduledFutureProxy[] futures = lossListeners.toArray(new ScheduledFutureProxy[0]);
                    for (ScheduledFutureProxy future : futures) {
                        future.notifyPartitionLost(event);
                    }
                });
    }

    private void unRegisterPartitionListenerIfExists() {
        if (this.partitionLostRegistration == null) {
            return;
        }

        try {
            getNodeEngine().getPartitionService().removePartitionLostListener(this.partitionLostRegistration);
        } catch (Exception ex) {
            if (peel(ex, HazelcastInstanceNotActiveException.class, null) instanceof HazelcastInstanceNotActiveException) {
                throw rethrow(ex);
            }
        }

        this.partitionLostRegistration = null;
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
        // ignore
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        // use toArray before iteration since it is done under mutex
        ScheduledFutureProxy[] futures = lossListeners.toArray(new ScheduledFutureProxy[0]);
        for (ScheduledFutureProxy future : futures) {
            future.notifyMemberLost(event);
        }
    }

    @Override
    public String getSplitBrainProtectionName(final String name) {
        Object splitBrainProtectionName = getOrPutSynchronized(splitBrainProtectionConfigCache, name,
                splitBrainProtectionConfigCacheMutexFactory, splitBrainProtectionConfigConstructor);
        return splitBrainProtectionName == NULL_OBJECT ? null : (String) splitBrainProtectionName;
    }

    @Override
    public Map<String, LocalExecutorStatsImpl> getStats() {
        return executorStats.getStatsMap();
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        provide(descriptor, context, SCHEDULED_EXECUTOR_PREFIX, getStats());
    }

    private class Merger extends AbstractContainerMerger<ScheduledExecutorContainer,
            ScheduledTaskDescriptor, ScheduledExecutorMergeTypes> {

        Merger(ScheduledExecutorContainerCollector collector) {
            super(collector, nodeEngine);
        }

        @Override
        protected String getLabel() {
            return "scheduled executors";
        }

        @Override
        public void runInternal() {
            ScheduledExecutorContainerCollector collector = (ScheduledExecutorContainerCollector) this.collector;
            SerializationService serializationService = nodeEngine.getSerializationService();

            List<ScheduledExecutorMergeTypes> mergingEntries;
            Map<Integer, Collection<ScheduledExecutorContainer>> containerMap = collector.getCollectedContainers();
            for (Map.Entry<Integer, Collection<ScheduledExecutorContainer>> entry : containerMap.entrySet()) {
                int partitionId = entry.getKey();
                Collection<ScheduledExecutorContainer> containers = entry.getValue();

                for (ScheduledExecutorContainer container : containers) {
                    String name = container.getName();
                    MergePolicyConfig mergePolicyConfig = collector.getMergePolicyConfig(container);
                    SplitBrainMergePolicy<ScheduledTaskDescriptor, ScheduledExecutorMergeTypes,
                            ScheduledTaskDescriptor> mergePolicy = getMergePolicy(mergePolicyConfig);
                    int batchSize = mergePolicyConfig.getBatchSize();

                    mergingEntries = new ArrayList<>(batchSize);

                    container.suspendTasks();
                    Map<String, ScheduledTaskDescriptor> tasks = container.prepareForReplication();
                    for (ScheduledTaskDescriptor descriptor : tasks.values()) {
                        ScheduledExecutorMergeTypes mergingEntry = createMergingEntry(serializationService, descriptor);
                        mergingEntries.add(mergingEntry);
                    }
                    if (mergingEntries.size() == batchSize) {
                        sendBatch(partitionId, name, mergingEntries, mergePolicy);
                        mergingEntries = new ArrayList<>(batchSize);
                    }
                    if (!mergingEntries.isEmpty()) {
                        sendBatch(partitionId, name, mergingEntries, mergePolicy);
                    }
                }
            }
        }

        private void sendBatch(int partitionId, String name, List<ScheduledExecutorMergeTypes> mergingEntries,
                               SplitBrainMergePolicy<ScheduledTaskDescriptor, ScheduledExecutorMergeTypes,
                                       ScheduledTaskDescriptor> mergePolicy) {
            MergeOperation operation = new MergeOperation(name, mergingEntries, mergePolicy);
            invoke(SERVICE_NAME, operation, partitionId);
        }
    }
}
