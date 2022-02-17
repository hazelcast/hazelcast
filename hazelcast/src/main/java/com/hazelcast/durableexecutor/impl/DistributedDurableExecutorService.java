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

package com.hazelcast.durableexecutor.impl;

import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.monitor.impl.LocalExecutorStatsImpl;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.SplitBrainProtectionAwareService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ContextMutexFactory;
import com.hazelcast.map.impl.ExecutorStats;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.DURABLE_EXECUTOR_PREFIX;
import static com.hazelcast.internal.metrics.impl.ProviderHelper.provide;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutSynchronized;

public class DistributedDurableExecutorService implements ManagedService, RemoteService, MigrationAwareService,
        SplitBrainProtectionAwareService, StatisticsAwareService<LocalExecutorStatsImpl>, DynamicMetricsProvider {

    public static final String SERVICE_NAME = "hz:impl:durableExecutorService";

    private static final Object NULL_OBJECT = new Object();

    private final NodeEngineImpl nodeEngine;
    private final ExecutorStats executorStats = new ExecutorStats();
    private final DurableExecutorPartitionContainer[] partitionContainers;
    private final Set<String> shutdownExecutors = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ConcurrentMap<String, Object> splitBrainProtectionConfigCache = new ConcurrentHashMap<>();
    private final ContextMutexFactory splitBrainProtectionConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> splitBrainProtectionConfigConstructor =
            new ConstructorFunction<String, Object>() {
                @Override
                public Object createNew(String name) {
                    DurableExecutorConfig executorConfig = nodeEngine.getConfig().findDurableExecutorConfig(name);
                    String splitBrainProtectionName = executorConfig.getSplitBrainProtectionName();
                    return splitBrainProtectionName == null ? NULL_OBJECT : splitBrainProtectionName;
                }
            };

    public DistributedDurableExecutorService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        partitionContainers = new DurableExecutorPartitionContainer[partitionCount];
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            partitionContainers[partitionId] = new DurableExecutorPartitionContainer(nodeEngine, partitionId);
        }
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        boolean dsMetricsEnabled = nodeEngine.getProperties().getBoolean(ClusterProperty.METRICS_DATASTRUCTURES);
        if (dsMetricsEnabled) {
            ((NodeEngineImpl) nodeEngine).getMetricsRegistry().registerDynamicMetricsProvider(this);
        }
    }

    public DurableExecutorPartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public void reset() {
        executorStats.clear();
        shutdownExecutors.clear();
        for (int partitionId = 0; partitionId < partitionContainers.length; partitionId++) {
            partitionContainers[partitionId] = new DurableExecutorPartitionContainer(nodeEngine, partitionId);
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public DistributedObject createDistributedObject(String name, UUID source, boolean local) {
        return new DurableExecutorServiceProxy(nodeEngine, this, name);
    }

    @Override
    public void destroyDistributedObject(String name, boolean local) {
        shutdownExecutors.remove(name);
        nodeEngine.getExecutionService().shutdownDurableExecutor(name);
        removeAllContainers(name);
        splitBrainProtectionConfigCache.remove(name);
    }

    public void shutdownExecutor(String name) {
        nodeEngine.getExecutionService().shutdownDurableExecutor(name);
        shutdownExecutors.add(name);
    }

    public boolean isShutdown(String name) {
        return shutdownExecutors.contains(name);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        DurableExecutorPartitionContainer partitionContainer = partitionContainers[partitionId];
        return partitionContainer.prepareReplicationOperation(event.getReplicaIndex());
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearRingBuffersHavingLesserBackupCountThan(partitionId, event.getNewReplicaIndex());
        } else if (event.getNewReplicaIndex() == 0) {
            DurableExecutorPartitionContainer partitionContainer = partitionContainers[partitionId];
            partitionContainer.executeAll();
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearRingBuffersHavingLesserBackupCountThan(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
    }

    private void clearRingBuffersHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        DurableExecutorPartitionContainer partitionContainer = partitionContainers[partitionId];
        partitionContainer.clearRingBuffersHavingLesserBackupCountThan(thresholdReplicaIndex);
    }

    @Override
    public String getSplitBrainProtectionName(final String name) {
        Object splitBrainProtectionName = getOrPutSynchronized(splitBrainProtectionConfigCache, name,
                splitBrainProtectionConfigCacheMutexFactory, splitBrainProtectionConfigConstructor);
        return splitBrainProtectionName == NULL_OBJECT ? null : (String) splitBrainProtectionName;
    }

    private void removeAllContainers(String name) {
        for (int i = 0; i < partitionContainers.length; i++) {
            getPartitionContainer(i).removeContainer(name);
        }
    }

    public ExecutorStats getExecutorStats() {
        return executorStats;
    }

    @Override
    public Map<String, LocalExecutorStatsImpl> getStats() {
        return executorStats.getStatsMap();
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        provide(descriptor, context, DURABLE_EXECUTOR_PREFIX, getStats());
    }
}
