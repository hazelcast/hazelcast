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

package com.hazelcast.cardinality.impl;

import com.hazelcast.cardinality.impl.hyperloglog.HyperLogLog;
import com.hazelcast.cardinality.impl.operations.MergeOperation;
import com.hazelcast.cardinality.impl.operations.ReplicationOperation;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.internal.services.SplitBrainProtectionAwareService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.merge.AbstractContainerMerger;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CardinalityEstimatorMergeTypes;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ContextMutexFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getPartitionKey;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

public class CardinalityEstimatorService
        implements ManagedService, RemoteService, MigrationAwareService, SplitBrainProtectionAwareService,
        SplitBrainHandlerService {

    public static final String SERVICE_NAME = "hz:impl:cardinalityEstimatorService";

    /**
     * Speculative factor to be used when initialising collections
     * of an approximate final size.
     */
    private static final double SIZING_FUDGE_FACTOR = 1.3;

    private static final Object NULL_OBJECT = new Object();

    private NodeEngine nodeEngine;
    private final ConcurrentMap<String, CardinalityEstimatorContainer> containers =
            new ConcurrentHashMap<String, CardinalityEstimatorContainer>();
    private final ConstructorFunction<String, CardinalityEstimatorContainer> cardinalityEstimatorContainerConstructorFunction =
            new ConstructorFunction<String, CardinalityEstimatorContainer>() {
                @Override
                public CardinalityEstimatorContainer createNew(String name) {
                    CardinalityEstimatorConfig config = nodeEngine.getConfig().findCardinalityEstimatorConfig(name);
                    return new CardinalityEstimatorContainer(config.getBackupCount(), config.getAsyncBackupCount());
                }
            };

    private final ConcurrentMap<String, Object> splitBrainProtectionConfigCache = new ConcurrentHashMap<String, Object>();
    private final ContextMutexFactory splitBrainProtectionConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> splitBrainProtectionConfigConstructor =
            new ConstructorFunction<String, Object>() {
        @Override
        public Object createNew(String name) {
            CardinalityEstimatorConfig config = nodeEngine.getConfig().findCardinalityEstimatorConfig(name);
            String splitBrainProtectionName = config.getSplitBrainProtectionName();
            return splitBrainProtectionName == null ? NULL_OBJECT : splitBrainProtectionName;
        }
    };

    public void addCardinalityEstimator(String name, CardinalityEstimatorContainer container) {
        checkNotNull(name, "Name can't be null");
        checkNotNull(container, "Container can't be null");

        containers.put(name, container);
    }

    public CardinalityEstimatorContainer getCardinalityEstimatorContainer(String name) {
        return getOrPutIfAbsent(containers, name, cardinalityEstimatorContainerConstructorFunction);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void reset() {
        containers.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public CardinalityEstimatorProxy createDistributedObject(String objectName, UUID source, boolean local) {
        return new CardinalityEstimatorProxy(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName, boolean local) {
        containers.remove(objectName);
        splitBrainProtectionConfigCache.remove(objectName);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        final IPartitionService partitionService = nodeEngine.getPartitionService();
        final int roughSize = (int) ((containers.size() * SIZING_FUDGE_FACTOR) / partitionService.getPartitionCount());
        Map<String, CardinalityEstimatorContainer> data = createHashMap(roughSize);
        int partitionId = event.getPartitionId();
        for (Map.Entry<String, CardinalityEstimatorContainer> containerEntry : containers.entrySet()) {
            String name = containerEntry.getKey();
            CardinalityEstimatorContainer container = containerEntry.getValue();

            if (partitionId == getPartitionId(name) && event.getReplicaIndex() <= container.getTotalBackupCount()) {
                data.put(name, containerEntry.getValue());
            }
        }

        return data.isEmpty() ? null : new ReplicationOperation(data);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearPartitionReplica(event.getPartitionId(), event.getNewReplicaIndex());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearPartitionReplica(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
    }

    private void clearPartitionReplica(int partitionId, int durabilityThreshold) {
        final Iterator<Map.Entry<String, CardinalityEstimatorContainer>> iterator = containers.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, CardinalityEstimatorContainer> entry = iterator.next();

            if (getPartitionId(entry.getKey()) == partitionId
                    && (durabilityThreshold == -1 || durabilityThreshold > entry.getValue().getTotalBackupCount())) {
                iterator.remove();
            }
        }
    }

    private int getPartitionId(String name) {
        IPartitionService partitionService = nodeEngine.getPartitionService();
        String partitionKey = getPartitionKey(name);
        return partitionService.getPartitionId(partitionKey);
    }

    @Override
    public String getSplitBrainProtectionName(String name) {
        Object splitBrainProtectionName = getOrPutSynchronized(splitBrainProtectionConfigCache, name,
                splitBrainProtectionConfigCacheMutexFactory, splitBrainProtectionConfigConstructor);
        return splitBrainProtectionName == NULL_OBJECT ? null : (String) splitBrainProtectionName;
    }

    @Override
    public Runnable prepareMergeRunnable() {
        CardinalityEstimatorContainerCollector collector = new CardinalityEstimatorContainerCollector(nodeEngine, containers);
        collector.run();
        return new Merger(collector);
    }

    private class Merger
            extends AbstractContainerMerger<CardinalityEstimatorContainer, HyperLogLog, CardinalityEstimatorMergeTypes> {

        Merger(CardinalityEstimatorContainerCollector collector) {
            super(collector, nodeEngine);
        }

        @Override
        protected String getLabel() {
            return "cardinality estimator";
        }

        @Override
        public void runInternal() {
            CardinalityEstimatorContainerCollector collector = (CardinalityEstimatorContainerCollector) this.collector;
            Map<Integer, Collection<CardinalityEstimatorContainer>> containerMap = collector.getCollectedContainers();
            for (Map.Entry<Integer, Collection<CardinalityEstimatorContainer>> entry : containerMap.entrySet()) {
                // TODO: batching support (tkountis)
                int partitionId = entry.getKey();
                Collection<CardinalityEstimatorContainer> containerList = entry.getValue();

                for (CardinalityEstimatorContainer container : containerList) {
                    String containerName = collector.getContainerName(container);
                    SplitBrainMergePolicy<HyperLogLog, CardinalityEstimatorMergeTypes, HyperLogLog> mergePolicy
                            = getMergePolicy(collector.getMergePolicyConfig(container));

                    MergeOperation operation = new MergeOperation(containerName, mergePolicy, container.hll);
                    invoke(SERVICE_NAME, operation, partitionId);
                }
            }
        }
    }
}
