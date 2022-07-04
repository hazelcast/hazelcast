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

package com.hazelcast.durableexecutor.impl;

import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.durableexecutor.impl.operations.ReplicationOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DurableExecutorPartitionContainer {

    private final int partitionId;
    private final NodeEngineImpl nodeEngine;

    private final ConcurrentMap<String, DurableExecutorContainer> executorContainerMap
            = new ConcurrentHashMap<>();

    public DurableExecutorPartitionContainer(NodeEngineImpl nodeEngine, int partitionId) {
        this.nodeEngine = nodeEngine;
        this.partitionId = partitionId;
    }

    public DurableExecutorContainer getOrCreateContainer(String name) {
        DurableExecutorContainer executorContainer = executorContainerMap.get(name);
        if (executorContainer == null) {
            executorContainer = createExecutorContainer(name);
            executorContainerMap.put(name, executorContainer);
        }
        return executorContainer;
    }

    public void createExecutorContainer(String name, TaskRingBuffer ringBuffer) {
        DurableExecutorConfig durableExecutorConfig = nodeEngine.getConfig().findDurableExecutorConfig(name);
        int durability = durableExecutorConfig.getDurability();
        boolean statisticsEnabled = durableExecutorConfig.isStatisticsEnabled();
        executorContainerMap.put(name, new DurableExecutorContainer(nodeEngine, name,
                partitionId, durability, statisticsEnabled, ringBuffer));
    }

    public Operation prepareReplicationOperation(int replicaIndex) {
        HashMap<String, DurableExecutorContainer> map = new HashMap<>();
        for (DurableExecutorContainer executorContainer : executorContainerMap.values()) {
            if (replicaIndex > executorContainer.getDurability()) {
                continue;
            }
            map.put(executorContainer.getName(), executorContainer);
        }
        return map.isEmpty() ? null : new ReplicationOperation(map);
    }

    public void clearRingBuffersHavingLesserBackupCountThan(int thresholdReplicaIndex) {
        if (thresholdReplicaIndex < 0) {
            executorContainerMap.clear();
        }
        Iterator<DurableExecutorContainer> iterator = executorContainerMap.values().iterator();
        while (iterator.hasNext()) {
            DurableExecutorContainer executorContainer = iterator.next();
            if (thresholdReplicaIndex > executorContainer.getDurability()) {
                iterator.remove();
            }
        }
    }

    public void executeAll() {
        for (DurableExecutorContainer container : executorContainerMap.values()) {
            container.executeAll();
        }
    }

    public void removeContainer(String name) {
        executorContainerMap.remove(name);
    }

    Collection<DurableExecutorContainer> getAllExecutorContainers() {
        return executorContainerMap.values();
    }

    boolean hasNoExecutorContainer() {
        return executorContainerMap.isEmpty();
    }

    // for testing
    DurableExecutorContainer getExistingExecutorContainer(String name) {
        return executorContainerMap.get(name);
    }

    private DurableExecutorContainer createExecutorContainer(String name) {
        DurableExecutorConfig durableExecutorConfig = nodeEngine.getConfig().findDurableExecutorConfig(name);
        int durability = durableExecutorConfig.getDurability();
        int ringBufferCapacity = durableExecutorConfig.getCapacity();
        boolean statisticsEnabled = durableExecutorConfig.isStatisticsEnabled();
        TaskRingBuffer ringBuffer = new TaskRingBuffer(ringBufferCapacity);
        return new DurableExecutorContainer(nodeEngine, name, partitionId, durability, statisticsEnabled, ringBuffer);
    }
}
