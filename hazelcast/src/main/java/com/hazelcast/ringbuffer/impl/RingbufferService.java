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

package com.hazelcast.ringbuffer.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.partition.ChunkedMigrationAwareService;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.partition.impl.NameSpaceUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.internal.services.SplitBrainProtectionAwareService;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ContextMutexFactory;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.ringbuffer.impl.operations.MergeOperation;
import com.hazelcast.ringbuffer.impl.operations.ReplicationOperation;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.merge.AbstractContainerMerger;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.merge.RingbufferMergeData;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.RingbufferMergeTypes;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.config.ConfigValidator.checkRingbufferConfig;
import static com.hazelcast.internal.partition.MigrationEndpoint.DESTINATION;
import static com.hazelcast.internal.partition.MigrationEndpoint.SOURCE;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutSynchronized;
import static com.hazelcast.internal.util.MapUtil.isNullOrEmpty;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * The SPI Service that deals with the {@link com.hazelcast.ringbuffer.Ringbuffer}.
 */
public class RingbufferService implements ManagedService, RemoteService, ChunkedMigrationAwareService,
        SplitBrainProtectionAwareService, SplitBrainHandlerService {

    /**
     * Prefix of ringbuffers that are created for topics. Using a prefix prevents
     * users accidentally retrieving the ringbuffer.
     */
    public static final String TOPIC_RB_PREFIX = "_hz_rb_";

    /**
     * The ringbuffer service name which defines it in the node engine.
     */
    public static final String SERVICE_NAME = "hz:impl:ringbufferService";

    private static final Object NULL_OBJECT = new Object();

    /**
     * Map from namespace to actual ringbuffer containers. The namespace
     * defines the service and object name which is the owner of the ringbuffer
     * container.
     */
    private final ConcurrentMap<Integer, Map<ObjectNamespace, RingbufferContainer>> containers
            = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Object> splitBrainProtectionConfigCache = new ConcurrentHashMap<>();
    private final ContextMutexFactory splitBrainProtectionConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> splitBrainProtectionConfigConstructor =
            new ConstructorFunction<String, Object>() {
                @Override
                public Object createNew(String name) {
                    RingbufferConfig config = nodeEngine.getConfig().findRingbufferConfig(name);
                    String splitBrainProtectionName = config.getSplitBrainProtectionName();
                    // The splitBrainProtectionName will be null
                    // if there is no split brain protection
                    // defined for this data structure, but the
                    // SplitBrainProtectionService is active,
                    // due to another data structure with a
                    // split brain protection configuration
                    return splitBrainProtectionName == null ? NULL_OBJECT : splitBrainProtectionName;
                }
            };

    private NodeEngine nodeEngine;
    private SerializationService serializationService;
    private IPartitionService partitionService;
    private SplitBrainProtectionService splitBrainProtectionService;

    public RingbufferService(NodeEngineImpl nodeEngine) {
        init(nodeEngine, null);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = checkNotNull(nodeEngine, "nodeEngine can't be null");
        this.serializationService = nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.splitBrainProtectionService = nodeEngine.getSplitBrainProtectionService();
    }

    // just for testing
    public ConcurrentMap<Integer, Map<ObjectNamespace, RingbufferContainer>> getContainers() {
        return containers;
    }

    @Override
    public DistributedObject createDistributedObject(String objectName, UUID source, boolean local) {
        RingbufferConfig ringbufferConfig = getRingbufferConfig(objectName);
        checkRingbufferConfig(ringbufferConfig, nodeEngine.getSplitBrainMergePolicyProvider());

        return new RingbufferProxy(nodeEngine, this, objectName, ringbufferConfig);
    }

    @Override
    public void destroyDistributedObject(String name, boolean local) {
        destroyContainer(getRingbufferPartitionId(name), getRingbufferNamespace(name));
        nodeEngine.getEventService().deregisterAllListeners(SERVICE_NAME, name);
        splitBrainProtectionConfigCache.remove(name);
    }

    public void destroyContainer(int partitionId, ObjectNamespace namespace) {
        final Map<ObjectNamespace, RingbufferContainer> partitionContainers = containers.get(partitionId);
        if (partitionContainers == null) {
            return;
        }
        partitionContainers.remove(namespace);
    }

    @Override
    public void reset() {
        containers.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    /**
     * Return the ringbuffer containter for the specified {@code namespace}.
     * If there is no ringbuffer container, create it using the {@code config}.
     *
     * @param namespace the ringbuffer container namespace
     * @param config    the ringbuffer config. Used to create the container when the container doesn't exist
     * @return the ringbuffer container
     * @throws NullPointerException if the {@code config} is {@code null}
     */
    @SuppressWarnings("unchecked")
    public <T, E> RingbufferContainer<T, E> getOrCreateContainer(int partitionId, ObjectNamespace namespace,
                                                                 RingbufferConfig config) {
        if (config == null) {
            throw new NullPointerException("Ringbuffer config must not be null when ringbuffer is being created");
        }
        final Map<ObjectNamespace, RingbufferContainer> partitionContainers = getOrCreateRingbufferContainers(partitionId);

        RingbufferContainer<T, E> ringbuffer = partitionContainers.get(namespace);
        if (ringbuffer != null) {
            return ringbuffer;
        }

        ringbuffer = new RingbufferContainer<>(namespace, config, nodeEngine, partitionId);
        ringbuffer.getStore().instrument(nodeEngine);
        partitionContainers.put(namespace, ringbuffer);
        return ringbuffer;
    }

    /**
     * Returns the ringbuffer container if it already exists for the
     * given {@code partitionId} and {@code namespace}. Returns {@code null}
     * if it doesn't exist.
     *
     * @param partitionId the partition ID of the ringbuffer container
     * @param namespace   the namespace of the ringbuffer container
     * @param <T>         the type of items in the ringbuffer container
     * @param <E>         the type of items in the ringbuffer
     * @return the ringbuffer container or {@code null} if it has not been created
     */
    @SuppressWarnings("unchecked")
    public <T, E> RingbufferContainer<T, E> getContainerOrNull(int partitionId, ObjectNamespace namespace) {
        final Map<ObjectNamespace, RingbufferContainer> partitionContainers = containers.get(partitionId);
        return partitionContainers != null ? partitionContainers.get(namespace) : null;
    }

    private Map<ObjectNamespace, RingbufferContainer> getOrCreateRingbufferContainers(int partitionId) {
        final Map<ObjectNamespace, RingbufferContainer> partitionContainer = containers.get(partitionId);
        if (partitionContainer == null) {
            containers.putIfAbsent(partitionId, new ConcurrentHashMap<>());
        }
        return containers.get(partitionId);
    }

    public RingbufferConfig getRingbufferConfig(String name) {
        Config config = nodeEngine.getConfig();
        return config.findRingbufferConfig(getConfigName(name));
    }

    public static ObjectNamespace getRingbufferNamespace(String name) {
        return new DistributedObjectNamespace(SERVICE_NAME, name);
    }

    public int getRingbufferPartitionId(String ringbufferName) {
        Data partitionAwareData = serializationService.toData(ringbufferName, StringPartitioningStrategy.INSTANCE);
        return partitionService.getPartitionId(partitionAwareData);
    }

    public void addRingbuffer(int partitionId, RingbufferContainer ringbuffer, RingbufferConfig config) {
        checkNotNull(ringbuffer, "ringbuffer can't be null");
        ringbuffer.init(config, nodeEngine);
        ringbuffer.getStore().instrument(nodeEngine);
        getOrCreateRingbufferContainers(partitionId).put(ringbuffer.getNamespace(), ringbuffer);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return prepareReplicationOperation(event, getAllServiceNamespaces(event));
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
                                                 Collection<ServiceNamespace> namespaces) {
        int partitionId = event.getPartitionId();
        Map<ObjectNamespace, RingbufferContainer> partitionContainers = containers.get(partitionId);
        if (isNullOrEmpty(partitionContainers)) {
            return null;
        }

        Map<ObjectNamespace, RingbufferContainer> migrationData = new HashMap<>();
        for (ServiceNamespace namespace : namespaces) {
            ObjectNamespace ns = (ObjectNamespace) namespace;
            RingbufferContainer container = partitionContainers.get(ns);
            if (container != null && container.getConfig().getTotalBackupCount() >= event.getReplicaIndex()) {
                migrationData.put(ns, container);
            }
        }
        if (migrationData.isEmpty()) {
            return null;
        }
        return new ReplicationOperation(migrationData, event.getPartitionId(), event.getReplicaIndex());
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == SOURCE) {
            clearRingbuffersHavingLesserBackupCountThan(event.getPartitionId(), event.getNewReplicaIndex());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == DESTINATION) {
            clearRingbuffersHavingLesserBackupCountThan(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
    }

    private void clearRingbuffersHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        final Map<ObjectNamespace, RingbufferContainer> partitionContainers = containers.get(partitionId);
        if (partitionContainers == null || partitionContainers.isEmpty()) {
            return;
        }

        final Iterator<Entry<ObjectNamespace, RingbufferContainer>> iterator = partitionContainers.entrySet().iterator();
        while (iterator.hasNext()) {
            final Entry<ObjectNamespace, RingbufferContainer> entry = iterator.next();
            final RingbufferContainer container = entry.getValue();
            if (thresholdReplicaIndex < 0 || container.getConfig().getTotalBackupCount() < thresholdReplicaIndex) {
                iterator.remove();
            }
        }
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        Map<ObjectNamespace, RingbufferContainer> partitionContainers = containers.get(partitionId);

        return NameSpaceUtil.getAllNamespaces(partitionContainers,
                container -> container.getConfig().getTotalBackupCount() >= event.getReplicaIndex(),
                RingbufferContainer::getNamespace);
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return namespace instanceof ObjectNamespace;
    }

    @Override
    public String getSplitBrainProtectionName(final String name) {
        Object splitBrainProtectionName = getOrPutSynchronized(splitBrainProtectionConfigCache, name,
                splitBrainProtectionConfigCacheMutexFactory, splitBrainProtectionConfigConstructor);
        return splitBrainProtectionName == NULL_OBJECT ? null : (String) splitBrainProtectionName;
    }

    public void ensureNoSplitBrain(String distributedObjectName,
                                   SplitBrainProtectionOn requiredSplitBrainProtectionPermissionType) {
        splitBrainProtectionService.ensureNoSplitBrain(getSplitBrainProtectionName(distributedObjectName),
                requiredSplitBrainProtectionPermissionType);
    }

    @Override
    public Runnable prepareMergeRunnable() {
        RingbufferContainerCollector collector = new RingbufferContainerCollector(nodeEngine, containers);
        collector.run();
        return new Merger(collector);
    }

    private class Merger
            extends AbstractContainerMerger<RingbufferContainer, RingbufferMergeData, RingbufferMergeTypes> {

        Merger(RingbufferContainerCollector collector) {
            super(collector, nodeEngine);
        }

        @Override
        protected String getLabel() {
            return "ringbuffer";
        }

        @Override
        public void runInternal() {
            for (Entry<Integer, Collection<RingbufferContainer>> entry : collector.getCollectedContainers().entrySet()) {
                int partitionId = entry.getKey();
                Collection<RingbufferContainer> containerList = entry.getValue();
                for (RingbufferContainer container : containerList) {
                    // TODO: add batching (which is a bit complex, since collections don't have a multi-name operation yet
                    SplitBrainMergePolicy<RingbufferMergeData, RingbufferMergeTypes, RingbufferMergeData> mergePolicy
                            = getMergePolicy(container.getConfig().getMergePolicyConfig());

                    sendBatch(partitionId, mergePolicy, container);
                }
            }
        }

        private void sendBatch(int partitionId,
                               SplitBrainMergePolicy<RingbufferMergeData, RingbufferMergeTypes, RingbufferMergeData> mergePolicy,
                               RingbufferContainer mergingContainer) {
            final MergeOperation operation = new MergeOperation(mergingContainer.getNamespace(), mergePolicy,
                    mergingContainer.getRingbuffer());
            invoke(SERVICE_NAME, operation, partitionId);
        }
    }

    private static String getConfigName(String name) {
        if (name.startsWith(TOPIC_RB_PREFIX)) {
            name = name.substring(TOPIC_RB_PREFIX.length());
        }
        return name;
    }
}
