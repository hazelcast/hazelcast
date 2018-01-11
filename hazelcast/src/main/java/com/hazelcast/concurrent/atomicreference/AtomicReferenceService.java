/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.atomicreference;

import com.hazelcast.concurrent.atomicreference.operations.AtomicReferenceReplicationOperation;
import com.hazelcast.config.AtomicReferenceConfig;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ContextMutexFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;

public class AtomicReferenceService implements ManagedService, RemoteService, MigrationAwareService, QuorumAwareService {

    public static final String SERVICE_NAME = "hz:impl:atomicReferenceService";

    private static final Object NULL_OBJECT = new Object();

    private NodeEngine nodeEngine;
    private final ConcurrentMap<String, AtomicReferenceContainer> containers
            = new ConcurrentHashMap<String, AtomicReferenceContainer>();
    private final ConstructorFunction<String, AtomicReferenceContainer> atomicReferenceConstructorFunction =
            new ConstructorFunction<String, AtomicReferenceContainer>() {
                public AtomicReferenceContainer createNew(String key) {
                    AtomicReferenceConfig config = nodeEngine.getConfig().findAtomicReferenceConfig(key);
                    return new AtomicReferenceContainer(config);
                }
            };

    private final ConcurrentMap<String, Object> quorumConfigCache = new ConcurrentHashMap<String, Object>();
    private final ContextMutexFactory quorumConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> quorumConfigConstructor = new ConstructorFunction<String, Object>() {
        @Override
        public Object createNew(String name) {
            AtomicReferenceConfig config = nodeEngine.getConfig().findAtomicReferenceConfig(name);
            String quorumName = config.getQuorumName();
            return quorumName == null ? NULL_OBJECT : quorumName;
        }
    };

    public AtomicReferenceService() {
    }

    public AtomicReferenceContainer getReferenceContainer(String name) {
        return getOrPutIfAbsent(containers, name, atomicReferenceConstructorFunction);
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
    public AtomicReferenceProxy createDistributedObject(String name) {
        return new AtomicReferenceProxy(name, nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name) {
        containers.remove(name);
        quorumConfigCache.remove(name);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent partitionMigrationEvent) {
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > 1) {
            return null;
        }

        Map<String, Data> data = new HashMap<String, Data>();
        int partitionId = event.getPartitionId();
        for (Map.Entry<String, AtomicReferenceContainer> containerEntry : containers.entrySet()) {
            String name = containerEntry.getKey();
            if (partitionId == getPartitionId(name)) {
                AtomicReferenceContainer atomicReferenceContainer = containerEntry.getValue();
                Data value = atomicReferenceContainer.get();
                data.put(name, value);
            }
        }
        return data.isEmpty() ? null : new AtomicReferenceReplicationOperation(data);
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

    private int getPartitionId(String name) {
        IPartitionService partitionService = nodeEngine.getPartitionService();
        String partitionKey = StringPartitioningStrategy.getPartitionKey(name);
        return partitionService.getPartitionId(partitionKey);
    }

    @Override
    public String getQuorumName(String name) {
        // RU_COMPAT_3_9
        if (nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_10)) {
            return null;
        }
        Object quorumName = getOrPutSynchronized(quorumConfigCache, name, quorumConfigCacheMutexFactory,
                quorumConfigConstructor);
        return quorumName == NULL_OBJECT ? null : (String) quorumName;
    }
}
