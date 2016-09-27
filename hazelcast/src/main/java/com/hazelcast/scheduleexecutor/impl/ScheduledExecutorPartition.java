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

package com.hazelcast.scheduleexecutor.impl;

import com.hazelcast.scheduleexecutor.impl.operations.ReplicationOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.ConstructorFunction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class ScheduledExecutorPartition {

    //TODO tkountis - Configurable ?
    private final static int DURABILITY = 1;

    private final int partitionId;

    private final NodeEngine nodeEngine;

    private final ConcurrentMap<String, ScheduledExecutorContainer> containers =
            new ConcurrentHashMap<String, ScheduledExecutorContainer>();

    private final ConstructorFunction<String, ScheduledExecutorContainer> containerConstructorFunction =
            new ConstructorFunction<String, ScheduledExecutorContainer>() {
                @Override
                public ScheduledExecutorContainer createNew(String name) {
                    return new ScheduledExecutorContainer(name, partitionId, nodeEngine, DURABILITY);
                }
            };

    public ScheduledExecutorPartition(NodeEngine nodeEngine, int partitionId) {
        this.partitionId = partitionId;
        this.nodeEngine = nodeEngine;
    }

    public void createContainer(String name, Map<String, BackupTaskDescriptor> backup) {
        checkNotNull(name, "Name can't be null");
        checkNotNull(backup, "Backup info can't be null");

        //TODO tkountis - Configuration based

        ScheduledExecutorContainer container =
                new ScheduledExecutorContainer(name, partitionId, nodeEngine, DURABILITY, backup);
        containers.put(name, container);
    }

    public ScheduledExecutorContainer getOrCreateContainer(String name) {
        checkNotNull(name, "Name can't be null");

        return getOrPutIfAbsent(containers, name, containerConstructorFunction);
    }

    public Operation prepareReplicationOperation(int replicaIndex) {
        Map<String, List<BackupTaskDescriptor>> map = new HashMap<String, List<BackupTaskDescriptor>>();
        for (ScheduledExecutorContainer container : containers.values()) {
            if (replicaIndex > container.getDurability()) {
                continue;
            }
            map.put(container.getName(), container.prepareReplicationOperation());
        }

        return new ReplicationOperation(map);
    }

    void disposeObsoleteReplicas(int thresholdReplicaIndex) {
        if (thresholdReplicaIndex < 0) {
            containers.clear();
        }

        Iterator<ScheduledExecutorContainer> iterator = containers.values().iterator();
        while (iterator.hasNext()) {
            ScheduledExecutorContainer container = iterator.next();
            if (thresholdReplicaIndex > container.getDurability()) {
                iterator.remove();
            }
        }
    }

    void scheduleAllPendingAndBackups() {
        for (ScheduledExecutorContainer container : containers.values()) {
            container.scheduleAllPerndingAndBackup();
        }
    }
}
