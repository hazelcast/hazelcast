/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.ringbuffer.impl.operations.ReplicationOperation;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.FragmentedMigrationAwareService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.version.Version;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.spi.partition.MigrationEndpoint.DESTINATION;
import static com.hazelcast.spi.partition.MigrationEndpoint.SOURCE;
import static com.hazelcast.util.MapUtil.isNullOrEmpty;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * The SPI Service that deals with the {@link com.hazelcast.ringbuffer.Ringbuffer}.
 */
public class RingbufferService implements ManagedService, RemoteService, FragmentedMigrationAwareService {
    /**
     * Prefix of ringbuffers that are created for topics. Using a prefix prevents users accidentally retrieving the ringbuffer.
     */
    public static final String TOPIC_RB_PREFIX = "_hz_rb_";

    /**
     * The ringbuffer service name which defines it in the node engine.
     */
    public static final String SERVICE_NAME = "hz:impl:ringbufferService";
    /**
     * Map from namespace to actual ringbuffer containers. The namespace defines the service and object name which
     * is the owner of the ringbuffer container.
     */
    private final ConcurrentMap<Integer, Map<ObjectNamespace, RingbufferContainer>> containers
            = new ConcurrentHashMap<Integer, Map<ObjectNamespace, RingbufferContainer>>();
    /**
     * The node engine for this node.
     */
    private NodeEngine nodeEngine;

    public RingbufferService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = checkNotNull(nodeEngine, "nodeEngine can't be null");
    }

    private static String getConfigName(String name) {
        if (name.startsWith(TOPIC_RB_PREFIX)) {
            name = name.substring(TOPIC_RB_PREFIX.length());
        }
        return name;
    }

    // just for testing
    public ConcurrentMap<Integer, Map<ObjectNamespace, RingbufferContainer>> getContainers() {
        return containers;
    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        final RingbufferConfig ringbufferConfig = getRingbufferConfig(objectName);
        return new RingbufferProxy(nodeEngine, this, objectName, ringbufferConfig);
    }

    @Override
    public void destroyDistributedObject(String name) {
        destroyContainer(getRingbufferPartitionId(name), getRingbufferNamespace(name));
        nodeEngine.getEventService().deregisterAllListeners(SERVICE_NAME, name);
    }

    public void destroyContainer(int partitionId, ObjectNamespace namespace) {
        final Map<ObjectNamespace, RingbufferContainer> partitionContainers = containers.get(partitionId);
        if (partitionContainers == null) {
            return;
        }
        partitionContainers.remove(namespace);
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

    /**
     * Return the ringbuffer containter for the specified {@code namespace}.
     * If there is no ringbuffer container, create it using the {@code config}.
     * When the cluster version is less than {@link Versions#V3_9} then the only
     * supported namespace is for the ringbuffer service, other namespaces will
     * throw an {@link UnsupportedOperationException}.
     *
     * @param namespace the ringbuffer container namespace
     * @param config    the ringbuffer config. Used to create the container when the container doesn't exist
     * @return the ringbuffer container
     * @throws UnsupportedOperationException if the cluster version is less than {@link Versions#V3_9} and the service name
     *                                       in the object namespace is not {@link RingbufferService#SERVICE_NAME}
     * @throws NullPointerException          if the {@code config} is {@code null}
     */
    @SuppressWarnings("unchecked")
    public <T> RingbufferContainer<T> getOrCreateContainer(int partitionId, ObjectNamespace namespace, RingbufferConfig config) {
        if (config == null) {
            throw new NullPointerException("Ringbuffer config should not be null when ringbuffer is being created");
        }
        final Version clusterVersion = nodeEngine.getClusterService().getClusterVersion();
        if (clusterVersion.isLessThan(Versions.V3_9)
                && !SERVICE_NAME.equals(namespace.getServiceName())) {
            throw new UnsupportedOperationException("Ringbuffer containers for service "
                    + namespace.getServiceName() + " are not supported when cluster version is " + clusterVersion);
        }

        final Map<ObjectNamespace, RingbufferContainer> partitionContainers = getOrCreateRingbufferContainers(partitionId);

        RingbufferContainer<T> ringbuffer = partitionContainers.get(namespace);
        if (ringbuffer != null) {
            return ringbuffer;
        }

        ringbuffer = new RingbufferContainer<T>(
                namespace,
                config,
                nodeEngine.getSerializationService(),
                nodeEngine.getConfigClassLoader(), partitionId);
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
     * @return the ringbuffer container or {@code null} if it has not been created
     */
    @SuppressWarnings("unchecked")
    public <T> RingbufferContainer<T> getContainerOrNull(int partitionId, ObjectNamespace namespace) {
        final Map<ObjectNamespace, RingbufferContainer> partitionContainers = containers.get(partitionId);
        return partitionContainers != null ? partitionContainers.get(namespace) : null;
    }

    private Map<ObjectNamespace, RingbufferContainer> getOrCreateRingbufferContainers(int partitionId) {
        final Map<ObjectNamespace, RingbufferContainer> partitionContainer = containers.get(partitionId);
        if (partitionContainer == null) {
            containers.putIfAbsent(partitionId, new HashMap<ObjectNamespace, RingbufferContainer>());
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
        final Data partitionAwareData =
                nodeEngine.getSerializationService().toData(ringbufferName, StringPartitioningStrategy.INSTANCE);
        return nodeEngine.getPartitionService().getPartitionId(partitionAwareData);
    }

    public void addRingbuffer(int partitionId, RingbufferContainer ringbuffer, RingbufferConfig config) {
        checkNotNull(ringbuffer, "ringbuffer can't be null");
        final SerializationService serializationService = nodeEngine.getSerializationService();
        ringbuffer.init(config, serializationService, nodeEngine.getConfigClassLoader());
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
        final int partitionId = event.getPartitionId();
        final Map<ObjectNamespace, RingbufferContainer> partitionContainers = containers.get(partitionId);
        if (isNullOrEmpty(partitionContainers)) {
            return null;
        }
        final Map<ObjectNamespace, RingbufferContainer> migrationData = new HashMap<ObjectNamespace, RingbufferContainer>();
        for (ServiceNamespace namespace : namespaces) {
            final ObjectNamespace ns = (ObjectNamespace) namespace;
            final RingbufferContainer container = partitionContainers.get(ns);
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
        final int partitionId = event.getPartitionId();
        final Map<ObjectNamespace, RingbufferContainer> partitionContainers = containers.get(partitionId);
        if (partitionContainers == null || partitionContainers.isEmpty()) {
            return Collections.emptyList();
        }
        final Set<ServiceNamespace> namespaces = new HashSet<ServiceNamespace>();
        for (RingbufferContainer container : partitionContainers.values()) {
            if (container.getConfig().getTotalBackupCount() < event.getReplicaIndex()) {
                continue;
            }
            namespaces.add(container.getNamespace());
        }
        return namespaces;
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return namespace instanceof ObjectNamespace;
    }
}
