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

package com.hazelcast.internal.longregister;

import com.hazelcast.internal.longregister.operations.LongRegisterReplicationOperation;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getPartitionKey;

public class LongRegisterService implements ManagedService, RemoteService, MigrationAwareService {

    /**
     * Keeping the old service name for backward compatibility during performance testing.
     * <pre>
     * IAtomicLong atomicLong = HazelcastInstance.getDistributedObject("hz:impl:atomicLongService", name);
     * </pre>
     */
    public static final String SERVICE_NAME = "hz:impl:atomicLongService";

    private final ConcurrentMap<String, LongRegister> registers = new ConcurrentHashMap<>();
    private final ConstructorFunction<String, LongRegister> longRegisterConstructorFunction = key -> new LongRegister();

    private final NodeEngine nodeEngine;

    public LongRegisterService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    public LongRegister getLongRegister(String name) {
        return getOrPutIfAbsent(registers, name, longRegisterConstructorFunction);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
        registers.clear();
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public LongRegisterProxy createDistributedObject(String name, UUID source, boolean local) {
        return new LongRegisterProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name, boolean local) {
        registers.remove(name);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > 1) {
            return null;
        }

        Map<String, Long> data = new HashMap<>();
        int partitionId = event.getPartitionId();
        for (Map.Entry<String, LongRegister> containerEntry : registers.entrySet()) {
            String name = containerEntry.getKey();
            if (partitionId == getPartitionId(name)) {
                LongRegister register = containerEntry.getValue();
                data.put(name, register.get());
            }
        }
        return data.isEmpty() ? null : new LongRegisterReplicationOperation(data);
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
        registers.keySet().removeIf(name -> getPartitionId(name) == partitionId);
    }
}
