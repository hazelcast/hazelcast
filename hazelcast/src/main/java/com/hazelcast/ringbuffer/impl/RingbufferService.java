/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.ringbuffer.impl.operations.ReplicationOperation;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.partition.MigrationEndpoint.DESTINATION;
import static com.hazelcast.partition.MigrationEndpoint.SOURCE;
import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getPartitionKey;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * The SPI Service that deals with the {@link com.hazelcast.ringbuffer.Ringbuffer}.
 */
public class RingbufferService implements ManagedService, RemoteService, MigrationAwareService {

    /**
     * Prefix of ringbuffers that are created for topics. Using a prefix prevents users accidentally retrieving the ringbuffer.
     */
    public static final String TOPIC_RB_PREFIX = "_hz_rb_";

    public static final String SERVICE_NAME = "hz:impl:ringbufferService";

    private NodeEngine nodeEngine;
    private final ConcurrentMap<String, RingbufferContainer> containers
            = new ConcurrentHashMap<String, RingbufferContainer>();

    public RingbufferService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = checkNotNull(nodeEngine, "nodeEngine can't be null");
    }

    // just for testing.
    public ConcurrentMap<String, RingbufferContainer> getContainers() {
        return containers;
    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        RingbufferConfig ringbufferConfig = getRingbufferConfig(objectName);
        return new RingbufferProxy(nodeEngine, this, objectName, ringbufferConfig);
    }

    @Override
    public void destroyDistributedObject(String name) {
        containers.remove(name);
        nodeEngine.getEventService().deregisterAllListeners(SERVICE_NAME, name);
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
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        Map<String, RingbufferContainer> migrationData = new HashMap<String, RingbufferContainer>();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        for (Map.Entry<String, RingbufferContainer> entry : containers.entrySet()) {
            String name = entry.getKey();
            int partitionId = partitionService.getPartitionId(getPartitionKey(name));
            RingbufferContainer container = entry.getValue();
            int backupCount = container.getConfig().getTotalBackupCount();
            if (partitionId == event.getPartitionId() && backupCount >= event.getReplicaIndex()) {
                migrationData.put(name, container);
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
            clearMigrationData(event.getPartitionId());
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == DESTINATION) {
            clearMigrationData(event.getPartitionId());
        }
    }

    private void clearMigrationData(int partitionId) {
        Iterator<Map.Entry<String, RingbufferContainer>> iterator = containers.entrySet().iterator();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        while (iterator.hasNext()) {
            Map.Entry<String, RingbufferContainer> entry = iterator.next();
            String name = entry.getKey();
            int containerPartitionId = partitionService.getPartitionId(getPartitionKey(name));
            if (containerPartitionId == partitionId) {
                iterator.remove();
            }
        }
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        clearMigrationData(partitionId);
    }

    public RingbufferContainer getContainer(String name) {
        RingbufferContainer ringbuffer = containers.get(name);
        if (ringbuffer != null) {
            return ringbuffer;
        }

        RingbufferConfig ringbufferConfig = getRingbufferConfig(name);
        ringbuffer = new RingbufferContainer(name, ringbufferConfig, nodeEngine.getSerializationService());
        containers.put(name, ringbuffer);
        return ringbuffer;
    }

    private RingbufferConfig getRingbufferConfig(String name) {
        Config config = nodeEngine.getConfig();
        return config.getRingbufferConfig(getConfigName(name));
    }

    public void addRingbuffer(String name, RingbufferContainer ringbuffer) {
        checkNotNull(name, "name can't be null");
        checkNotNull(ringbuffer, "ringbuffer can't be null");

        ringbuffer.init(nodeEngine);
        containers.put(name, ringbuffer);
    }

    private static String getConfigName(String name) {
        if (name.startsWith(TOPIC_RB_PREFIX)) {
            name = name.substring(TOPIC_RB_PREFIX.length());
        }
        return name;
    }

}
