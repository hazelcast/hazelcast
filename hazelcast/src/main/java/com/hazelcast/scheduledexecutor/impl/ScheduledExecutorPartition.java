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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.scheduledexecutor.impl.operations.ReplicationOperation;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.util.ConstructorFunction;

import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.util.MapUtil.createHashMap;

public class ScheduledExecutorPartition
        extends AbstractScheduledExecutorContainerHolder {

    private final ILogger logger;
    private final int partitionId;

    private final ConstructorFunction<String, ScheduledExecutorContainer> containerConstructorFunction =
            new ConstructorFunction<String, ScheduledExecutorContainer>() {
                @Override
                public ScheduledExecutorContainer createNew(String name) {
                    if (logger.isFinestEnabled()) {
                        logger.finest("[Partition:" + partitionId + "]Create new scheduled executor container with name:" + name);
                    }

                    ScheduledExecutorConfig config = nodeEngine.getConfig().findScheduledExecutorConfig(name);
                    return new ScheduledExecutorContainer(name, partitionId, nodeEngine, config.getDurability(),
                            config.getCapacity());
                }
            };

    ScheduledExecutorPartition(NodeEngine nodeEngine, int partitionId) {
        super(nodeEngine);
        this.logger = nodeEngine.getLogger(getClass());
        this.partitionId = partitionId;
    }

    public Operation prepareReplicationOperation(int replicaIndex, boolean migrationMode) {
        Map<String, Map<String, ScheduledTaskDescriptor>> map = createHashMap(containers.size());

        if (logger.isFinestEnabled()) {
            logger.finest("[Partition: " + partitionId + "] Prepare replication(migration: " + migrationMode + ") "
                    + "for index: " + replicaIndex);
        }

        for (ScheduledExecutorContainer container : containers.values()) {
            if (replicaIndex > container.getDurability()) {
                continue;
            }
            map.put(container.getName(), container.prepareForReplication(migrationMode));
        }

        return new ReplicationOperation(map);
    }

    @Override
    public ConstructorFunction<String, ScheduledExecutorContainer> getContainerConstructorFunction() {
        return containerConstructorFunction;
    }

    void disposeObsoleteReplicas(int thresholdReplicaIndex) {
        if (logger.isFinestEnabled()) {
            logger.finest("[Partition: " + partitionId + "] Dispose obsolete replicas with thresholdReplicaIndex: "
                    + thresholdReplicaIndex);
        }

        if (thresholdReplicaIndex < 0) {
            for (ScheduledExecutorContainer container : containers.values()) {
                container.destroy();
            }

            containers.clear();
        } else {
            Iterator<ScheduledExecutorContainer> iterator = containers.values().iterator();
            while (iterator.hasNext()) {
                ScheduledExecutorContainer container = iterator.next();
                if (thresholdReplicaIndex > container.getDurability()) {
                    container.destroy();
                    iterator.remove();
                }
            }
        }
    }

    void promoteSuspended() {
        if (logger.isFinestEnabled()) {
            logger.finest("[Partition: " + partitionId + "] " + "Promote suspended");
        }

        for (ScheduledExecutorContainer container : containers.values()) {
            container.promoteSuspended();
        }
    }
}
