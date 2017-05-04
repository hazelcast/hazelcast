/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.operation.CheckReplicaVersion;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.FragmentedMigrationAwareService;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.spi.partition.IPartitionService.SERVICE_NAME;

final class CheckReplicaVersionTask implements PartitionSpecificRunnable, UrgentSystemOperation {

    private static final int OPERATION_TRY_COUNT = 10;
    private static final int OPERATION_TRY_PAUSE_MILLIS = 250;

    private final NodeEngineImpl nodeEngine;
    private final InternalPartitionServiceImpl partitionService;
    private final int partitionId;
    private final int replicaIndex;
    private final ExecutionCallback callback;

    CheckReplicaVersionTask(NodeEngineImpl nodeEngine, InternalPartitionServiceImpl partitionService,
            int partitionId, int replicaIndex, ExecutionCallback callback) {
        this.nodeEngine = nodeEngine;
        this.partitionService = partitionService;
        this.partitionId = partitionId;

        if (replicaIndex < 1 || replicaIndex > InternalPartition.MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Replica index should be in range [1-"
                    + InternalPartition.MAX_BACKUP_COUNT + "]");
        }
        this.replicaIndex = replicaIndex;
        this.callback = callback;
    }

    @Override
    public void run() {
        InternalPartition partition = partitionService.getPartition(partitionId);
        if (partition.isMigrating()) {
            notifyCallback(false);
            return;
        }

        Address target = partition.getReplicaAddress(replicaIndex);
        if (target == null) {
            notifyCallback(false);
            return;
        }

        invokeCheckReplicaVersion(target);
    }

    private void invokeCheckReplicaVersion(Address target) {
        Collection<ServiceNamespace> namespaces = retainAndGetNamespaces();

        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        Map<ServiceNamespace, Long> versionMap = new HashMap<ServiceNamespace, Long>();
        for (ServiceNamespace ns : namespaces) {
            long[] versions = replicaManager.getPartitionReplicaVersions(partitionId, ns);
            long currentReplicaVersion = versions[replicaIndex - 1];

            if (currentReplicaVersion > 0) {
                versionMap.put(ns, currentReplicaVersion);
            }
        }

        if (versionMap.isEmpty()) {
            notifyCallback(true);
            return;
        }

        // ASSERTION
        if (nodeEngine.getClusterService().getClusterVersion().isLessThan(Versions.V3_9)) {
            assert versionMap.size() == 1 : "Only single namespace is allowed before V3.9: " + versionMap;
        }

        CheckReplicaVersion op = new CheckReplicaVersion(versionMap, shouldInvoke());
        op.setPartitionId(partitionId).setReplicaIndex(replicaIndex).setServiceName(SERVICE_NAME);
        OperationService operationService = nodeEngine.getOperationService();

        if (shouldInvoke()) {
            operationService.createInvocationBuilder(SERVICE_NAME, op, target)
                    .setExecutionCallback(callback)
                    .setTryCount(OPERATION_TRY_COUNT)
                    .setTryPauseMillis(OPERATION_TRY_PAUSE_MILLIS)
                    .invoke();
        } else {
            operationService.send(op, target);
        }
    }

    // works only on primary. backups are retained when CheckReplicaVersion is executed.
    private Collection<ServiceNamespace> retainAndGetNamespaces() {
        PartitionReplicationEvent event = new PartitionReplicationEvent(partitionId, 0);
        Collection<FragmentedMigrationAwareService> services = nodeEngine.getServices(FragmentedMigrationAwareService.class);

        Set<ServiceNamespace> namespaces = new HashSet<ServiceNamespace>();
        for (FragmentedMigrationAwareService service : services) {
            Collection<ServiceNamespace> serviceNamespaces = service.getAllServiceNamespaces(event);
            namespaces.addAll(serviceNamespaces);
        }
        namespaces.add(NonFragmentedServiceNamespace.INSTANCE);

        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        replicaManager.retainNamespaces(partitionId, namespaces);
        return replicaManager.getNamespaces(partitionId);
    }

    private void notifyCallback(boolean result) {
        if (shouldInvoke()) {
            callback.onResponse(result);
        }
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    private boolean shouldInvoke() {
        return callback != null;
    }
}
