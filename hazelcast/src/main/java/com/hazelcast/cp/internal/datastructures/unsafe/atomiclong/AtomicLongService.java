/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.unsafe.atomiclong;

import com.hazelcast.config.AtomicLongConfig;
import com.hazelcast.cp.internal.datastructures.unsafe.atomiclong.operations.AtomicLongReplicationOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.atomiclong.operations.MergeOperation;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.internal.services.SplitBrainProtectionAwareService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.merge.AbstractContainerMerger;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.AtomicLongMergeTypes;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.partition.MigrationAwareService;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.spi.partition.PartitionMigrationEvent;
import com.hazelcast.spi.partition.PartitionReplicationEvent;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ContextMutexFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.config.ConfigValidator.checkBasicConfig;
import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getPartitionKey;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;

public class AtomicLongService
        implements ManagedService, RemoteService, MigrationAwareService, SplitBrainProtectionAwareService,
        SplitBrainHandlerService {

    public static final String SERVICE_NAME = "hz:impl:atomicLongService";

    private static final Object NULL_OBJECT = new Object();

    private final ConcurrentMap<String, AtomicLongContainer> containers = new ConcurrentHashMap<String, AtomicLongContainer>();
    private final ConstructorFunction<String, AtomicLongContainer> atomicLongConstructorFunction =
            new ConstructorFunction<String, AtomicLongContainer>() {
                public AtomicLongContainer createNew(String key) {
                    return new AtomicLongContainer();
                }
            };

    private final ConcurrentMap<String, Object> splitBrainProtectionConfigCache = new ConcurrentHashMap<String, Object>();
    private final ContextMutexFactory splitBrainProtectionConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> splitBrainProtectionConfigConstructor =
            new ConstructorFunction<String, Object>() {
        @Override
        public Object createNew(String name) {
            AtomicLongConfig config = nodeEngine.getConfig().findAtomicLongConfig(name);
            String splitBrainProtectionName = config.getSplitBrainProtectionName();
            // the splitBrainProtectionName will be null if there is no split brain protection defined for this data structure,
            // but the SplitBrainProtectionService is active, due to another data structure with
            // a split brain protection configuration
            return splitBrainProtectionName == null ? NULL_OBJECT : splitBrainProtectionName;
        }
    };

    private NodeEngine nodeEngine;

    public AtomicLongService() {
    }

    public AtomicLongContainer getLongContainer(String name) {
        return getOrPutIfAbsent(containers, name, atomicLongConstructorFunction);
    }

    public boolean containsAtomicLong(String name) {
        return containers.containsKey(name);
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
    public AtomicLongProxy createDistributedObject(String name) {
        AtomicLongConfig atomicLongConfig = nodeEngine.getConfig().findAtomicLongConfig(name);
        checkBasicConfig(atomicLongConfig, nodeEngine.getSplitBrainMergePolicyProvider());

        return new AtomicLongProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name) {
        containers.remove(name);
        splitBrainProtectionConfigCache.remove(name);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > 1) {
            return null;
        }

        Map<String, Long> data = new HashMap<String, Long>();
        int partitionId = event.getPartitionId();
        for (Map.Entry<String, AtomicLongContainer> containerEntry : containers.entrySet()) {
            String name = containerEntry.getKey();
            if (partitionId == getPartitionId(name)) {
                AtomicLongContainer container = containerEntry.getValue();
                data.put(name, container.get());
            }
        }
        return data.isEmpty() ? null : new AtomicLongReplicationOperation(data);
    }

    private int getPartitionId(String name) {
        IPartitionService partitionService = nodeEngine.getPartitionService();
        String partitionKey = getPartitionKey(name);
        return partitionService.getPartitionId(partitionKey);
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

    @Override
    public String getSplitBrainProtectionName(String name) {
        Object splitBrainProtectionName = getOrPutSynchronized(splitBrainProtectionConfigCache, name,
                splitBrainProtectionConfigCacheMutexFactory, splitBrainProtectionConfigConstructor);
        return splitBrainProtectionName == NULL_OBJECT ? null : (String) splitBrainProtectionName;
    }

    @Override
    public Runnable prepareMergeRunnable() {
        AtomicLongContainerCollector collector = new AtomicLongContainerCollector(nodeEngine, containers);
        collector.run();
        return new Merger(collector);
    }

    private class Merger extends AbstractContainerMerger<AtomicLongContainer, Long, AtomicLongMergeTypes> {

        Merger(AtomicLongContainerCollector collector) {
            super(collector, nodeEngine);
        }

        @Override
        protected String getLabel() {
            return "AtomicLong";
        }

        @Override
        public void runInternal() {
            AtomicLongContainerCollector collector = (AtomicLongContainerCollector) this.collector;
            for (Map.Entry<Integer, Collection<AtomicLongContainer>> entry : collector.getCollectedContainers().entrySet()) {
                // TODO: add batching (which is a bit complex, since AtomicLong is a single-value data structure,
                // so we need an operation for multiple AtomicLong instances, which doesn't exist so far)
                int partitionId = entry.getKey();
                Collection<AtomicLongContainer> containerList = entry.getValue();

                for (AtomicLongContainer container : containerList) {
                    String name = collector.getContainerName(container);
                    SplitBrainMergePolicy<Long, AtomicLongMergeTypes> mergePolicy
                            = getMergePolicy(collector.getMergePolicyConfig(container));

                    MergeOperation operation = new MergeOperation(name, mergePolicy, container.get());
                    invoke(SERVICE_NAME, operation, partitionId);
                }
            }
        }
    }
}
