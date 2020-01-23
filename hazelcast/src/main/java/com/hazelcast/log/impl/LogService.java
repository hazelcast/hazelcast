/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.log.impl;

import com.hazelcast.config.LogConfig;
import com.hazelcast.internal.logstore.LogStore;
import com.hazelcast.internal.logstore.LogStoreConfig;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class LogService implements ManagedService, MigrationAwareService, RemoteService {

    public static final String SERVICE_NAME = "hz:impl:logService";
    private NodeEngine nodeEngine;
    private Properties properties;
    private LogServicePartition[] partitions;

    public LogService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.partitions = new LogServicePartition[nodeEngine.getPartitionService().getPartitionCount()];
        for (int k = 0; k < partitions.length; k++) {
            partitions[k] = new LogServicePartition(k);
        }
    }

    public LogContainer getContainer(int partition, String name) {
        return partitions[partition].getOrCreate(name);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return null;
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {

    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {

    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {

    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        this.properties = properties;
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {

    }

    @Override
    public LogProxy createDistributedObject(String objectName, UUID source, boolean local) {
        LogConfig logConfig = nodeEngine.getConfig().findLogConfig(objectName);
        return new LogProxy(objectName, this, nodeEngine, logConfig);
    }

    @Override
    public void destroyDistributedObject(String objectName, boolean local) {

    }

    public class LogServicePartition {

        private final int partition;
        private Map<String, LogContainer> containers = new HashMap<>();

        public LogServicePartition(int partition) {
            this.partition = partition;
        }

        public LogContainer getOrCreate(String name) {
            LogContainer container = containers.get(name);
            if (container == null) {
                LogConfig config = nodeEngine.getConfig().findLogConfig(name);
                Class<?> type = null;
                try {
                    String typeName = config.getType();
                    if (typeName != null) {
                        if ("long".equals(typeName)) {
                            type = Long.TYPE;
                        } else if ("int".equals(typeName)) {
                            type = Integer.TYPE;
                        } else if ("byte".equals(typeName)) {
                            type = Byte.TYPE;
                        } else if ("double".equals(typeName)) {
                            type = Double.TYPE;
                        } else {
                            type = LogServicePartition.class.getClassLoader().loadClass(typeName);
                        }
                    }
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }

                // System.out.println("type:"+type);

                LogStoreConfig storageEngineConfig = new LogStoreConfig()
                        .setSegmentSize(config.getSegmentSize())
                        .setMaxSegmentCount(config.getMaxSegmentCount())
                        .setType(type)
                        .setTenuringAgeMillis(config.getTenuringAgeMillis())
                        .setRetentionMillis(config.getRetentionMillis())
                        .setEncoder(config.getEncoder());

                LogStore engine = LogStore.create(storageEngineConfig);
                container = new LogContainer(name, partition, engine, nodeEngine.getSerializationService());
                containers.put(name, container);
            }
            return container;
        }
    }
}
