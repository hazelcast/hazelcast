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

package com.hazelcast.durableexecutor.impl;

import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.partition.MigrationEndpoint;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ContextMutexFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutSynchronized;

public class DistributedDurableExecutorService implements ManagedService, RemoteService, MigrationAwareService,
        QuorumAwareService {

    public static final String SERVICE_NAME = "hz:impl:durableExecutorService";

    private static final Object NULL_OBJECT = new Object();

    private final NodeEngineImpl nodeEngine;
    private final DurableExecutorPartitionContainer[] partitionContainers;

    private final Set<String> shutdownExecutors
            = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    private final ConcurrentMap<String, Object> quorumConfigCache = new ConcurrentHashMap<String, Object>();
    private final ContextMutexFactory quorumConfigCacheMutexFactory = new ContextMutexFactory();
    private final ConstructorFunction<String, Object> quorumConfigConstructor = new ConstructorFunction<String, Object>() {
        @Override
        public Object createNew(String name) {
            DurableExecutorConfig executorConfig = nodeEngine.getConfig().findDurableExecutorConfig(name);
            String quorumName = executorConfig.getQuorumName();
            return quorumName == null ? NULL_OBJECT : quorumName;
        }
    };

    public DistributedDurableExecutorService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        partitionContainers = new DurableExecutorPartitionContainer[partitionCount];
        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            partitionContainers[partitionId] = new DurableExecutorPartitionContainer(nodeEngine, partitionId);
        }
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    public DurableExecutorPartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public void reset() {
        shutdownExecutors.clear();
        for (int partitionId = 0; partitionId < partitionContainers.length; partitionId++) {
            partitionContainers[partitionId] = new DurableExecutorPartitionContainer(nodeEngine, partitionId);
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        reset();
    }

    @Override
    public DistributedObject createDistributedObject(String name) {
        return new DurableExecutorServiceProxy(nodeEngine, this, name);
    }

    @Override
    public void destroyDistributedObject(String name) {
        shutdownExecutors.remove(name);
        nodeEngine.getExecutionService().shutdownDurableExecutor(name);
        quorumConfigCache.remove(name);
    }

    public void shutdownExecutor(String name) {
        nodeEngine.getExecutionService().shutdownDurableExecutor(name);
        shutdownExecutors.add(name);
    }

    public boolean isShutdown(String name) {
        return shutdownExecutors.contains(name);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        int partitionId = event.getPartitionId();
        DurableExecutorPartitionContainer partitionContainer = partitionContainers[partitionId];
        return partitionContainer.prepareReplicationOperation(event.getReplicaIndex());
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        int partitionId = event.getPartitionId();
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearRingBuffersHavingLesserBackupCountThan(partitionId, event.getNewReplicaIndex());
        } else if (event.getNewReplicaIndex() == 0) {
            DurableExecutorPartitionContainer partitionContainer = partitionContainers[partitionId];
            partitionContainer.executeAll();
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearRingBuffersHavingLesserBackupCountThan(event.getPartitionId(), event.getCurrentReplicaIndex());
        }
    }

    private void clearRingBuffersHavingLesserBackupCountThan(int partitionId, int thresholdReplicaIndex) {
        DurableExecutorPartitionContainer partitionContainer = partitionContainers[partitionId];
        partitionContainer.clearRingBuffersHavingLesserBackupCountThan(thresholdReplicaIndex);
    }

    @Override
    public String getQuorumName(final String name) {
        // RU_COMPAT_3_9
        if (nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_10)) {
            return null;
        }
        Object quorumName = getOrPutSynchronized(quorumConfigCache, name, quorumConfigCacheMutexFactory,
                quorumConfigConstructor);
        return quorumName == NULL_OBJECT ? null : (String) quorumName;
    }

}
