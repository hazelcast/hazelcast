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

package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.concurrent.atomicreference.operations.AtomicReferenceReplicationOperation;
import com.hazelcast.concurrent.atomicreference.operations.MergeOperation;
import com.hazelcast.config.AtomicReferenceConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.util.ExceptionUtil.rethrow;

public class AtomicReferenceService
        implements ManagedService, RemoteService, MigrationAwareService, QuorumAwareService, SplitBrainHandlerService {

    public static final String SERVICE_NAME = "hz:impl:atomicReferenceService";

    private static final Object NULL_OBJECT = new Object();

    private final ConcurrentMap<String, AtomicReferenceContainer> containers
            = new ConcurrentHashMap<String, AtomicReferenceContainer>();
    private final ConstructorFunction<String, AtomicReferenceContainer> atomicReferenceConstructorFunction =
            new ConstructorFunction<String, AtomicReferenceContainer>() {
                public AtomicReferenceContainer createNew(String key) {
                    return new AtomicReferenceContainer(nodeEngine, key);
                }
            };

    private final ConcurrentMap<String, Object> quorumConfigCache = new ConcurrentHashMap<String, Object>();
    private final ContextMutexFactory quorumConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> quorumConfigConstructor = new ConstructorFunction<String, Object>() {
        @Override
        public Object createNew(String name) {
            AtomicReferenceConfig config = nodeEngine.getConfig().findAtomicReferenceConfig(name);
            String quorumName = config.getQuorumName();
            // the quorumName will be null if there is no quorum defined for this data structure,
            // but the QuorumService is active, due to another data structure with a quorum configuration
            return quorumName == null ? NULL_OBJECT : quorumName;
        }
    };

    private NodeEngine nodeEngine;
    private SplitBrainMergePolicyProvider mergePolicyProvider;

    public AtomicReferenceService() {
    }

    public AtomicReferenceContainer getReferenceContainer(String name) {
        return getOrPutIfAbsent(containers, name, atomicReferenceConstructorFunction);
    }

    public boolean containsReferenceContainer(String name) {
        return containers.containsKey(name);
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
    public AtomicReferenceProxy createDistributedObject(String name) {
        return new AtomicReferenceProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name) {
        containers.remove(name);
        quorumConfigCache.remove(name);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > 1) {
            return null;
        }

        Map<String, Data> data = new HashMap<String, Data>();
        int partitionId = event.getPartitionId();
        for (Map.Entry<String, AtomicReferenceContainer> containerEntry : containers.entrySet()) {
            String name = containerEntry.getKey();
            if (partitionId == getPartitionId(name)) {
                AtomicReferenceContainer atomicReferenceContainer = containerEntry.getValue();
                Data value = atomicReferenceContainer.get();
                data.put(name, value);
            }
        }
        return data.isEmpty() ? null : new AtomicReferenceReplicationOperation(data);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            int thresholdReplicaIndex = event.getNewReplicaIndex();
            if (thresholdReplicaIndex == -1 || thresholdReplicaIndex > 1) {
                clearPartitionReplica(event.getPartitionId());
            }
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            int thresholdReplicaIndex = event.getCurrentReplicaIndex();
            if (thresholdReplicaIndex == -1 || thresholdReplicaIndex > 1) {
                clearPartitionReplica(event.getPartitionId());
            }
        }
    }

    private void clearPartitionReplica(int partitionId) {
        final Iterator<String> iterator = containers.keySet().iterator();
        while (iterator.hasNext()) {
            String name = iterator.next();
            if (getPartitionId(name) == partitionId) {
                iterator.remove();
            }
        }
    }

    private int getPartitionId(String name) {
        IPartitionService partitionService = nodeEngine.getPartitionService();
        String partitionKey = StringPartitioningStrategy.getPartitionKey(name);
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

    @Override
    public Runnable prepareMergeRunnable() {
        IPartitionService partitionService = nodeEngine.getPartitionService();
        Map<Integer, List<AtomicReferenceContainer>> containerMap = new HashMap<Integer, List<AtomicReferenceContainer>>();

        for (Map.Entry<String, AtomicReferenceContainer> entry : containers.entrySet()) {
            AtomicReferenceContainer container = entry.getValue();
            if (!(getMergePolicy(container) instanceof DiscardMergePolicy)) {
                String name = entry.getKey();
                int partitionId = partitionService.getPartitionId(StringPartitioningStrategy.getPartitionKey(name));
                if (partitionService.isPartitionOwner(partitionId)) {
                    // add your owned values to the map so they will be merged
                    List<AtomicReferenceContainer> containerList = containerMap.get(partitionId);
                    if (containerList == null) {
                        containerList = new ArrayList<AtomicReferenceContainer>(containers.size());
                        containerMap.put(partitionId, containerList);
                    }
                    containerList.add(container);
                }
            }
        }
        containers.clear();

        return new Merger(containerMap);
    }

    private SplitBrainMergePolicy getMergePolicy(AtomicReferenceContainer container) {
        String mergePolicyName = container.getConfig().getMergePolicyConfig().getPolicy();
        return mergePolicyProvider.getMergePolicy(mergePolicyName);
    }

    private class Merger implements Runnable {

        private static final long TIMEOUT_FACTOR = 500;

        private final ILogger logger = nodeEngine.getLogger(AtomicReferenceService.class);
        private final Semaphore semaphore = new Semaphore(0);
        private final ExecutionCallback<Object> mergeCallback = new ExecutionCallback<Object>() {
            @Override
            public void onResponse(Object response) {
                semaphore.release(1);
            }

            @Override
            public void onFailure(Throwable t) {
                logger.warning("Error while running AtomicReference merge operation: " + t.getMessage());
                semaphore.release(1);
            }
        };

        private final Map<Integer, List<AtomicReferenceContainer>> containerMap;

        Merger(Map<Integer, List<AtomicReferenceContainer>> containerMap) {
            this.containerMap = containerMap;
        }

        @Override
        public void run() {
            // we cannot merge into a 3.9 cluster, since not all members may understand the MergeOperation
            // RU_COMPAT_3_9
            if (nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_10)) {
                logger.info("Cluster needs to run version " + Versions.V3_10 + " to merge AtomicReference instances");
                return;
            }

            int valueCount = 0;
            for (Map.Entry<Integer, List<AtomicReferenceContainer>> entry : containerMap.entrySet()) {
                // TODO: add batching (which is a bit complex, since AtomicReference is a single-value data structure,
                // so we need an operation for multiple AtomicReference instances, which doesn't exist so far)
                int partitionId = entry.getKey();
                List<AtomicReferenceContainer> containerList = entry.getValue();

                for (AtomicReferenceContainer container : containerList) {
                    String name = container.getName();
                    valueCount++;

                    MergeOperation operation = new MergeOperation(name, getMergePolicy(container), container.get());
                    try {
                        nodeEngine.getOperationService()
                                .invokeOnPartition(SERVICE_NAME, operation, partitionId)
                                .andThen(mergeCallback);
                    } catch (Throwable t) {
                        throw rethrow(t);
                    }
                }
            }
            containerMap.clear();

            try {
                if (!semaphore.tryAcquire(valueCount, valueCount * TIMEOUT_FACTOR, TimeUnit.MILLISECONDS)) {
                    logger.warning("Split-brain healing for AtomicReference instances didn't finish within the timeout...");
                }
            } catch (InterruptedException e) {
                logger.finest("Interrupted while waiting for split-brain healing of AtomicReference instances...");
                Thread.currentThread().interrupt();
            }
        }
    }
}
