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

package com.hazelcast.cardinality.impl;

import com.hazelcast.cardinality.impl.hyperloglog.HyperLogLog;
import com.hazelcast.cardinality.impl.operations.MergeOperation;
import com.hazelcast.cardinality.impl.operations.ReplicationOperation;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.impl.merge.AbstractContainerMerger;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CardinalityEstimatorMergeTypes;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ContextMutexFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getPartitionKey;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class CardinalityEstimatorService
        implements ManagedService, RemoteService, MigrationAwareService, QuorumAwareService, SplitBrainHandlerService {

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

    private final ConcurrentMap<String, Object> quorumConfigCache = new ConcurrentHashMap<String, Object>();
    private final ContextMutexFactory quorumConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> quorumConfigConstructor = new ConstructorFunction<String, Object>() {
        @Override
        public Object createNew(String name) {
            CardinalityEstimatorConfig config = nodeEngine.getConfig().findCardinalityEstimatorConfig(name);
            String quorumName = config.getQuorumName();
            return quorumName == null ? NULL_OBJECT : quorumName;
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
    public CardinalityEstimatorProxy createDistributedObject(String objectName) {
        return new CardinalityEstimatorProxy(objectName, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        containers.remove(objectName);
        quorumConfigCache.remove(objectName);
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
    public String getQuorumName(String name) {
        // RU_COMPAT_3_9
        if (nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_10)) {
            return null;
        }
        Object quorumName = getOrPutSynchronized(quorumConfigCache, name, quorumConfigCacheMutexFactory, quorumConfigConstructor);
        return quorumName == NULL_OBJECT ? null : (String) quorumName;
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
                    SplitBrainMergePolicy<HyperLogLog, CardinalityEstimatorMergeTypes> mergePolicy
                            = getMergePolicy(collector.getMergePolicyConfig(container));

                    MergeOperation operation = new MergeOperation(containerName, mergePolicy, container.hll);
                    invoke(SERVICE_NAME, operation, partitionId);
                }
            }
        }
    }
}
