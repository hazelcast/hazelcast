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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.logging.ILogger;
import com.hazelcast.scheduledexecutor.impl.operations.ReplicationOperation;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.Iterator;
import java.util.Map;

import static com.hazelcast.config.ScheduledExecutorConfig.CapacityPolicy.PER_PARTITION;
import static com.hazelcast.internal.util.MapUtil.createHashMap;
import static com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService.NOOP_PERMIT;

public class ScheduledExecutorPartition extends AbstractScheduledExecutorContainerHolder {

    private final int partitionId;
    private final ILogger logger;
    private final ConstructorFunction<String, ScheduledExecutorContainer> containerConstructorFunction;

    ScheduledExecutorPartition(NodeEngine nodeEngine, DistributedScheduledExecutorService service, int partitionId) {
        super(nodeEngine);
        this.logger = nodeEngine.getLogger(getClass());
        this.partitionId = partitionId;
        this.containerConstructorFunction = name -> {
            if (logger.isFinestEnabled()) {
                logger.finest("[Partition:" + partitionId + "]Create new scheduled executor container with name:" + name);
            }
            ScheduledExecutorConfig config = nodeEngine.getConfig().findScheduledExecutorConfig(name);
            return new ScheduledExecutorContainer(name, partitionId, nodeEngine,
                    newPermitFor(name, service, config), config.getDurability(), config.isStatisticsEnabled());
        };
    }

    /**
     * Collects all tasks for replication for all schedulers on this partition.
     *
     * @param replicaIndex the replica index of the member which will receive the returned operation
     * @return the
     */
    public Operation prepareReplicationOperation(int replicaIndex) {
        Map<String, Map<String, ScheduledTaskDescriptor>> map = createHashMap(containers.size());

        if (logger.isFinestEnabled()) {
            logger.finest("[Partition: " + partitionId + "] Preparing replication for index: " + replicaIndex);
        }

        for (ScheduledExecutorContainer container : containers.values()) {
            if (replicaIndex > container.getDurability()) {
                continue;
            }
            map.put(container.getName(), container.prepareForReplication());
        }

        return map.isEmpty() ? null : new ReplicationOperation(map);
    }

    @Override
    public ConstructorFunction<String, ScheduledExecutorContainer> getContainerConstructorFunction() {
        return containerConstructorFunction;
    }

    CapacityPermit newPermitFor(String name, DistributedScheduledExecutorService service,
                                ScheduledExecutorConfig config) {
        if (config.getCapacity() == 0) {
            return NOOP_PERMIT;
        }

        return config.getCapacityPolicy() == PER_PARTITION
                ? new PartitionCapacityPermit(name, config.getCapacity(), partitionId)
                : service.permitFor(name, config);
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

    /**
     * Attempts to promote and schedule all suspended tasks on this partition.
     * Exceptions throw during rescheduling will be rethrown and will prevent
     * further suspended tasks from being scheduled.
     */
    void promoteSuspended() {
        if (logger.isFinestEnabled()) {
            logger.finest("[Partition: " + partitionId + "] " + "Promote suspended");
        }

        for (ScheduledExecutorContainer container : containers.values()) {
            container.promoteSuspended();
        }
    }

    /**
     * Attempts to cancel and interrupt all tasks on all containers. Exceptions
     * thrown during task cancellation will be rethrown and prevent further
     * tasks from being cancelled.
     */
    void suspendTasks() {
        if (logger.isFinestEnabled()) {
            logger.finest("[Partition: " + partitionId + "] Suspending tasks");
        }

        for (ScheduledExecutorContainer container : containers.values()) {
            container.suspendTasks();
        }
    }
}
