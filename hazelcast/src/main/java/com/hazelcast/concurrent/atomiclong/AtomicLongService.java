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

package com.hazelcast.concurrent.atomiclong;

import com.hazelcast.concurrent.atomiclong.operations.AtomicLongReplicationOperation;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.util.ConstructorFunction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getPartitionKey;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

public class AtomicLongService implements ManagedService, RemoteService, MigrationAwareService {

    public static final String SERVICE_NAME = "hz:impl:atomicLongService";

    private NodeEngine nodeEngine;
    private final ConcurrentMap<String, AtomicLongContainer> containers = new ConcurrentHashMap<String, AtomicLongContainer>();
    private final ConstructorFunction<String, AtomicLongContainer> atomicLongConstructorFunction =
            new ConstructorFunction<String, AtomicLongContainer>() {
                public AtomicLongContainer createNew(String key) {
                    return new AtomicLongContainer();
                }
            };

    public AtomicLongService() {
    }

    public AtomicLongContainer getLongContainer(String name) {
        return getOrPutIfAbsent(containers, name, atomicLongConstructorFunction);
    }

    // need for testing..
    public boolean containsAtomicLong(String name) {
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
    public AtomicLongProxy createDistributedObject(String name) {
        return new AtomicLongProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name) {
        containers.remove(name);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > 1) {
            return null;
        }

        Map<String, Long> data = new HashMap<String, Long>();
        int partitionId = event.getPartitionId();
        for (Map.Entry<String, AtomicLongContainer> containerEntry : containers.entrySet()) {
            String name = containerEntry.getKey();
            if (partitionId == getPartitionId(name)) {
                AtomicLongContainer container = containerEntry.getValue();
                data.put(name, container.get());
            }
        }
        return data.isEmpty() ? null : new AtomicLongReplicationOperation(data);
    }

    private int getPartitionId(String name) {
        IPartitionService partitionService = nodeEngine.getPartitionService();
        String partitionKey = getPartitionKey(name);
        return partitionService.getPartitionId(partitionKey);
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
}
