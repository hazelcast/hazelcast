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

import com.hazelcast.cardinality.impl.operations.MergeOperation;
import com.hazelcast.cardinality.impl.operations.ReplicationOperation;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.DiscardMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ContextMutexFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getPartitionKey;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.MapUtil.createHashMap;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class CardinalityEstimatorService
        implements ManagedService, RemoteService, MigrationAwareService,
                   SplitBrainHandlerService, QuorumAwareService {

    public static final String SERVICE_NAME = "hz:impl:cardinalityEstimatorService";

    /**
     * Speculative factor to be used when initialising collections
     * of an approximate final size.
     */
    private static final double SIZING_FUDGE_FACTOR = 1.3;

    private static final Object NULL_OBJECT = new Object();

    private NodeEngine nodeEngine;
    private SplitBrainMergePolicyProvider mergePolicyProvider;
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
        this.mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
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

    @Override
    public Runnable prepareMergeRunnable() {
        Map<String, CardinalityEstimatorContainer> state =
                new HashMap<String, CardinalityEstimatorContainer>();

        for (Map.Entry<String, CardinalityEstimatorContainer> entry : containers.entrySet()) {
            SplitBrainMergePolicy mergePolicy = getMergePolicy(entry.getKey());

            int partition = getPartitionId(entry.getKey());
            if (nodeEngine.getPartitionService().isPartitionOwner(partition)
                    && !(mergePolicy instanceof DiscardMergePolicy)) {
                state.put(entry.getKey(), entry.getValue());
            }
        }

        return new Merger(state);
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
        Object quorumName = getOrPutSynchronized(quorumConfigCache, name, quorumConfigCacheMutexFactory,
                quorumConfigConstructor);
        return quorumName == NULL_OBJECT ? null : (String) quorumName;
    }

    private SplitBrainMergePolicy getMergePolicy(String name) {
        String mergePolicyName = nodeEngine.getConfig().findCardinalityEstimatorConfig(name).getMergePolicyConfig().getPolicy();
        return mergePolicyProvider.getMergePolicy(mergePolicyName);
    }

    private class Merger implements Runnable {

        private static final int TIMEOUT_FACTOR = 500;

        private Map<String, CardinalityEstimatorContainer> snapshot;

        Merger(Map<String, CardinalityEstimatorContainer> snapshot) {
            this.snapshot = snapshot;
        }

        @SuppressWarnings({"checkstyle:methodlength"})
        @Override
        public void run() {
            // we cannot merge into a 3.9 cluster, since not all members may understand the CollectionMergeOperation
            if (nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_10)) {
                return;
            }

            final ILogger logger = nodeEngine.getLogger(CardinalityEstimatorService.class);
            final Semaphore semaphore = new Semaphore(0);

            ExecutionCallback<Object> mergeCallback = new ExecutionCallback<Object>() {
                @Override
                public void onResponse(Object response) {
                    semaphore.release(1);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.warning("Error while running merge operation: " + t.getMessage());
                    semaphore.release(1);
                }
            };

            int size = 0;
            int operationCount = 0;

            try {
                //TODO tkountis - Batching support
                for (Map.Entry<String, CardinalityEstimatorContainer> entry : snapshot.entrySet()) {
                    String containerName = entry.getKey();
                    CardinalityEstimatorContainer container = entry.getValue();
                    int partitionId = getPartitionId(containerName);

                    operationCount++;

                    SplitBrainMergePolicy mergePolicy = getMergePolicy(containerName);
                    MergeOperation operation = new MergeOperation(containerName, mergePolicy, container.hll);
                    try {
                        nodeEngine.getOperationService()
                                  .invokeOnPartition(SERVICE_NAME, operation, partitionId)
                                  .andThen(mergeCallback);
                    } catch (Throwable t) {
                        throw rethrow(t);
                    }
                    size++;

                }

                snapshot.clear();

            } catch (Exception ex) {
                logger.warning("CardinalityEstimatorService merging didn't complete successfully.", ex);
                throw rethrow(ex);
            }

            try {
                semaphore.tryAcquire(operationCount, size * TIMEOUT_FACTOR, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                logger.finest("Interrupted while waiting for merge operation...");
            }
        }
    }
}

