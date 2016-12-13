/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.scheduledexecutor.impl.operations.ReplicationOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.MapUtil;

public class ScheduledExecutorPartition implements ScheduledExecutorContainerHolder {

    private final ILogger logger;

    private final int partitionId;

    private final NodeEngine nodeEngine;

    private final ConcurrentMap<String, ScheduledExecutorContainer> containers =
            new ConcurrentHashMap<String, ScheduledExecutorContainer>();

    private final ConstructorFunction<String, ScheduledExecutorContainer> containerConstructorFunction =
            new ConstructorFunction<String, ScheduledExecutorContainer>() {
                @Override
                public ScheduledExecutorContainer createNew(String name) {
                    ScheduledExecutorConfig config = nodeEngine.getConfig().findScheduledExecutorConfig(name);
                    return new ScheduledExecutorContainer(name, partitionId, nodeEngine, config.getDurability());
                }
            };

    public ScheduledExecutorPartition(NodeEngine nodeEngine, int partitionId) {
        this.logger = nodeEngine.getLogger(getClass());
        this.partitionId = partitionId;
        this.nodeEngine = nodeEngine;
    }

    public void createContainer(String name, ConcurrentMap<String, ScheduledTaskDescriptor> tasks) {
        checkNotNull(name, "Name can't be null");
        checkNotNull(tasks, "Tasks info can't be null");

        if (logger.isFineEnabled()) {
            logger.fine("Creating Scheduled Executor partition with name: " + name);
        }

        ScheduledExecutorConfig config = nodeEngine.getConfig().findScheduledExecutorConfig(name);
        ScheduledExecutorContainer container =
                new ScheduledExecutorContainer(name, partitionId, nodeEngine, config.getDurability(), tasks);
        containers.put(name, container);
    }

    public Collection<ScheduledExecutorContainer> getContainers() {
        return Collections.unmodifiableCollection(containers.values());
    }

    public ScheduledExecutorContainer getOrCreateContainer(String name) {
        checkNotNull(name, "Name can't be null");

        return getOrPutIfAbsent(containers, name, containerConstructorFunction);
    }

    public void destroy() {
        for (ScheduledExecutorContainer container : containers.values()) {
            ((InternalExecutionService) nodeEngine.getExecutionService())
                    .shutdownScheduledDurableExecutor(container.getName());
        }
    }

    public Operation prepareReplicationOperation(int replicaIndex, boolean migrationMode) {
        final Map<String, Map<String, ScheduledTaskDescriptor>> map = MapUtil.createHashMap(containers.size());

        for (ScheduledExecutorContainer container : containers.values()) {
            if (replicaIndex > container.getDurability()) {
                continue;
            }
            map.put(container.getName(), container.prepareForReplication(migrationMode));
        }

        return new ReplicationOperation(map);
    }

    void disposeObsoleteReplicas(int thresholdReplicaIndex) {
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

    void promoteStash() {
        for (ScheduledExecutorContainer container : containers.values()) {
            container.promoteStash();
        }
    }
}
