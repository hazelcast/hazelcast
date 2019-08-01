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

package com.hazelcast.cp.internal.datastructures.unsafe.atomicreference;

import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.operations.AtomicReferenceReplicationOperation;
import com.hazelcast.cp.internal.datastructures.unsafe.atomicreference.operations.MergeOperation;
import com.hazelcast.config.AtomicReferenceConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.spi.partition.MigrationAwareService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.partition.PartitionMigrationEvent;
import com.hazelcast.spi.partition.PartitionReplicationEvent;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.services.QuorumAwareService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.spi.impl.merge.AbstractContainerMerger;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.AtomicReferenceMergeTypes;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.partition.MigrationEndpoint;
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
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;

public class AtomicReferenceService
        implements ManagedService, RemoteService, MigrationAwareService, QuorumAwareService, SplitBrainHandlerService {

    public static final String SERVICE_NAME = "hz:impl:atomicReferenceService";

    private static final Object NULL_OBJECT = new Object();

    private final ConcurrentMap<String, AtomicReferenceContainer> containers
            = new ConcurrentHashMap<String, AtomicReferenceContainer>();
    private final ConstructorFunction<String, AtomicReferenceContainer> atomicReferenceConstructorFunction =
            new ConstructorFunction<String, AtomicReferenceContainer>() {
                public AtomicReferenceContainer createNew(String key) {
                    return new AtomicReferenceContainer();
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
        AtomicReferenceConfig atomicReferenceConfig = nodeEngine.getConfig().findAtomicReferenceConfig(name);
        checkBasicConfig(atomicReferenceConfig, nodeEngine.getSplitBrainMergePolicyProvider());

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
        Object quorumName = getOrPutSynchronized(quorumConfigCache, name, quorumConfigCacheMutexFactory,
                quorumConfigConstructor);
        return quorumName == NULL_OBJECT ? null : (String) quorumName;
    }

    @Override
    public Runnable prepareMergeRunnable() {
        AtomicReferenceContainerCollector collector = new AtomicReferenceContainerCollector(nodeEngine, containers);
        collector.run();
        return new Merger(collector);
    }

    private class Merger extends AbstractContainerMerger<AtomicReferenceContainer, Object, AtomicReferenceMergeTypes> {

        Merger(AtomicReferenceContainerCollector collector) {
            super(collector, nodeEngine);
        }

        @Override
        protected String getLabel() {
            return "AtomicReference";
        }

        @Override
        public void runInternal() {
            AtomicReferenceContainerCollector collector = (AtomicReferenceContainerCollector) this.collector;
            for (Map.Entry<Integer, Collection<AtomicReferenceContainer>> entry : collector.getCollectedContainers().entrySet()) {
                // TODO: add batching (which is a bit complex, since AtomicReference is a single-value data structure,
                // so we need an operation for multiple AtomicReference instances, which doesn't exist so far)
                int partitionId = entry.getKey();
                Collection<AtomicReferenceContainer> containerList = entry.getValue();

                for (AtomicReferenceContainer container : containerList) {
                    String name = collector.getContainerName(container);
                    SplitBrainMergePolicy<Object, AtomicReferenceMergeTypes> mergePolicy
                            = getMergePolicy(collector.getMergePolicyConfig(container));

                    MergeOperation operation = new MergeOperation(name, mergePolicy, container.get());
                    invoke(SERVICE_NAME, operation, partitionId);
                }
            }
        }
    }
}
