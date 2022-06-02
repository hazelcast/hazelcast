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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.FragmentedMigrationAwareService;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.partition.operation.PartitionBackupReplicaAntiEntropyOperation;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.partition.IPartitionService.SERVICE_NAME;

public abstract class AbstractPartitionPrimaryReplicaAntiEntropyTask
        implements PartitionSpecificRunnable, UrgentSystemOperation {

    private static final int OPERATION_TRY_COUNT = 10;

    private static final int OPERATION_TRY_PAUSE_MILLIS = 250;

    protected final NodeEngineImpl nodeEngine;

    protected final InternalPartitionServiceImpl partitionService;

    protected final int partitionId;

    public AbstractPartitionPrimaryReplicaAntiEntropyTask(NodeEngineImpl nodeEngine, int partitionId) {
        this.nodeEngine = nodeEngine;
        this.partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        this.partitionId = partitionId;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    // works only on primary. backups are retained
    // in PartitionBackupReplicaAntiEntropyTask
    final Collection<ServiceNamespace> retainAndGetNamespaces() {
        PartitionReplicationEvent event = new PartitionReplicationEvent(null, partitionId, 0);
        Collection<FragmentedMigrationAwareService> services
                = nodeEngine.getServices(FragmentedMigrationAwareService.class);

        Collection<ServiceNamespace> namespaces = new HashSet<>();
        for (FragmentedMigrationAwareService service : services) {
            Collection<ServiceNamespace> serviceNamespaces = service.getAllServiceNamespaces(event);
            if (serviceNamespaces != null) {
                namespaces.addAll(serviceNamespaces);
            }
        }
        namespaces.add(NonFragmentedServiceNamespace.INSTANCE);

        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        replicaManager.retainNamespaces(partitionId, namespaces);

        ILogger logger = nodeEngine.getLogger(getClass());
        if (logger.isFinestEnabled()) {
            logger.finest("Retained namespaces for partitionId=" + partitionId + ". Service namespaces="
                    + namespaces + ", retained namespaces=" + replicaManager.getNamespaces(partitionId));
        }
        return replicaManager.getNamespaces(partitionId);
    }

    final void invokePartitionBackupReplicaAntiEntropyOp(int replicaIndex, PartitionReplica target,
                                                         Collection<ServiceNamespace> namespaces,
                                                         BiConsumer<Object, Throwable> callback) {
        if (skipSendingToTarget(target)) {
            return;
        }

        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        Map<ServiceNamespace, Long> versionMap = new HashMap<>();
        for (ServiceNamespace ns : namespaces) {
            long[] versions = replicaManager.getPartitionReplicaVersions(partitionId, ns);
            long currentReplicaVersion = versions[replicaIndex - 1];
            versionMap.put(ns, currentReplicaVersion);
        }

        boolean hasCallback = (callback != null);

        Operation op = new PartitionBackupReplicaAntiEntropyOperation(versionMap, hasCallback);
        op.setPartitionId(partitionId)
                .setReplicaIndex(replicaIndex)
                .setServiceName(SERVICE_NAME);

        ILogger logger = nodeEngine.getLogger(getClass());
        if (logger.isFinestEnabled()) {
            logger.finest("Sending anti-entropy operation to " + target + " for partitionId=" + partitionId
                    + ", replicaIndex=" + replicaIndex + ", namespaces=" + versionMap);
        }

        OperationService operationService = nodeEngine.getOperationService();
        if (hasCallback) {
            ExecutorService asyncExecutor =
                    nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);
            operationService.createInvocationBuilder(SERVICE_NAME, op, target.address())
                    .setTryCount(OPERATION_TRY_COUNT)
                    .setTryPauseMillis(OPERATION_TRY_PAUSE_MILLIS)
                    .invoke()
                    .whenCompleteAsync(callback, asyncExecutor);
        } else {
            operationService.send(op, target.address());
        }
    }

    private boolean skipSendingToTarget(PartitionReplica target) {
        ClusterServiceImpl clusterService = nodeEngine.getNode().getClusterService();

        assert !target.isIdentical(nodeEngine.getLocalMember()) : "Could not send anti-entropy operation, because "
                + target + " is local member itself! Local-member: " + clusterService.getLocalMember()
                + ", " + partitionService.getPartition(partitionId);

        if (clusterService.getMember(target.address(), target.uuid()) == null) {
            ILogger logger = nodeEngine.getLogger(getClass());
            if (logger.isFinestEnabled()) {
                if (clusterService.isMissingMember(target.address(), target.uuid())) {
                    logger.finest("Could not send anti-entropy operation, because " + target + " is a missing member. "
                            + partitionService.getPartition(partitionId));
                } else {
                    logger.finest("Could not send anti-entropy operation, because " + target + " is not a known member. "
                            + partitionService.getPartition(partitionId));
                }
            }
            return true;
        }
        return false;
    }
}
