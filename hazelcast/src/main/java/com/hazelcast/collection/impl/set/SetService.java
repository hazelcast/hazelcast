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

package com.hazelcast.collection.impl.set;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.hazelcast.collection.LocalSetStats;
import com.hazelcast.collection.impl.collection.CollectionContainer;
import com.hazelcast.collection.impl.collection.CollectionService;
import com.hazelcast.collection.impl.set.operations.SetReplicationOperation;
import com.hazelcast.collection.impl.txnset.TransactionalSetProxy;
import com.hazelcast.config.SetConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricDescriptorConstants;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.impl.ProviderHelper;
import com.hazelcast.internal.monitor.impl.LocalSetStatsImpl;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.internal.services.TenantContextAwareService;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ContextMutexFactory;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.transaction.impl.Transaction;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutSynchronized;

public class SetService extends CollectionService implements DynamicMetricsProvider,
        StatisticsAwareService<LocalSetStats>, TenantContextAwareService {

    public static final String SERVICE_NAME = "hz:impl:setService";

    private static final Object NULL_OBJECT = new Object();

    private final ConcurrentMap<String, SetContainer> containerMap = new ConcurrentHashMap<String, SetContainer>();

    private final ConcurrentMap<String, Object> splitBrainProtectionConfigCache = new ConcurrentHashMap<String, Object>();
    private final ContextMutexFactory splitBrainProtectionConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> splitBrainProtectionConfigConstructor
            = new ConstructorFunction<String, Object>() {
        @Override
        public Object createNew(String name) {
                SetConfig lockConfig = nodeEngine.getConfig().findSetConfig(name);
                String splitBrainProtectionName = lockConfig.getSplitBrainProtectionName();
                return splitBrainProtectionName == null ? NULL_OBJECT : splitBrainProtectionName;
        }
            };
    private final ConcurrentMap<String, LocalSetStatsImpl> statsMap = new ConcurrentHashMap<>();
    private final ConstructorFunction<String, LocalSetStatsImpl> localCollectionStatsConstructorFunction =
            key -> new LocalSetStatsImpl();

    public SetService(NodeEngine nodeEngine) {
        super(nodeEngine);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        boolean dsMetricsEnabled = nodeEngine.getProperties().getBoolean(ClusterProperty.METRICS_DATASTRUCTURES);
        if (dsMetricsEnabled) {
            ((NodeEngineImpl) nodeEngine).getMetricsRegistry().registerDynamicMetricsProvider(this);
        }
        super.init(nodeEngine, properties);
    }

    @Override
    public SetContainer getOrCreateContainer(String name, boolean backup) {
        SetContainer container = containerMap.get(name);
        if (container == null) {
            container = new SetContainer(name, nodeEngine);
            final SetContainer current = containerMap.putIfAbsent(name, container);
            if (current != null) {
                container = current;
            }
        }
        return container;
    }

    @Override
    public ConcurrentMap<String, ? extends CollectionContainer> getContainerMap() {
        return containerMap;
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public DistributedObject createDistributedObject(String objectId, UUID source, boolean local) {
        return new SetProxyImpl(objectId, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name, boolean local) {
        super.destroyDistributedObject(name, local);
        splitBrainProtectionConfigCache.remove(name);
    }

    @Override
    public TransactionalSetProxy createTransactionalObject(String name, Transaction transaction) {
        return new TransactionalSetProxy(name, transaction, nodeEngine, this);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        final Map<String, CollectionContainer> migrationData = getMigrationData(event);
        return migrationData.isEmpty()
                ? null
                : new SetReplicationOperation(migrationData, event.getPartitionId(), event.getReplicaIndex());
    }

    @Override
    public String getSplitBrainProtectionName(final String name) {
        Object splitBrainProtectionName = getOrPutSynchronized(splitBrainProtectionConfigCache, name,
                splitBrainProtectionConfigCacheMutexFactory, splitBrainProtectionConfigConstructor);
        return splitBrainProtectionName == NULL_OBJECT ? null : (String) splitBrainProtectionName;
    }

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        ProviderHelper.provide(descriptor, context, MetricDescriptorConstants.SET_PREFIX, getStats());
    }

    @Override
    public Map<String, LocalSetStats> getStats() {
        Map<String, LocalSetStats> setStats = MapUtil.createHashMap(containerMap.size());
        for (Map.Entry<String, SetContainer> entry : containerMap.entrySet()) {
            String name = entry.getKey();
            SetContainer setContainer = entry.getValue();
            if (setContainer.getConfig().isStatisticsEnabled()) {
                LocalSetStats setStat = getLocalCollectionStats(name);
                setStats.put(name, setStat);
            }
        }
        return setStats;
    }

    @Override
    public LocalSetStatsImpl getLocalCollectionStats(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, localCollectionStatsConstructorFunction);
    }
}
