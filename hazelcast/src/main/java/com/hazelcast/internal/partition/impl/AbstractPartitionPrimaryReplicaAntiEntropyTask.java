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
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.operation.PartitionBackupReplicaAntiEntropyOperation;
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

    // works only on primary. backups are retained in PartitionBackupReplicaAntiEntropyTask
    final Collection<ServiceNamespace> retainAndGetNamespaces() {
        PartitionReplicationEvent event = new PartitionReplicationEvent(partitionId, 0);
        Collection<FragmentedMigrationAwareService> services = nodeEngine.getServices(FragmentedMigrationAwareService.class);

        Set<ServiceNamespace> namespaces = new HashSet<ServiceNamespace>();
        for (FragmentedMigrationAwareService service : services) {
            Collection<ServiceNamespace> serviceNamespaces = service.getAllServiceNamespaces(event);
            if (serviceNamespaces != null) {
                namespaces.addAll(serviceNamespaces);
            }
        }
        namespaces.add(NonFragmentedServiceNamespace.INSTANCE);

        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        replicaManager.retainNamespaces(partitionId, namespaces);
        return replicaManager.getNamespaces(partitionId);
    }

    final void invokePartitionBackupReplicaAntiEntropyOp(int replicaIndex, Address target,
                                                         Collection<ServiceNamespace> namespaces, ExecutionCallback callback) {
        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        Map<ServiceNamespace, Long> versionMap = new HashMap<ServiceNamespace, Long>();
        for (ServiceNamespace ns : namespaces) {
            long[] versions = replicaManager.getPartitionReplicaVersions(partitionId, ns);
            long currentReplicaVersion = versions[replicaIndex - 1];

            if (currentReplicaVersion > 0) {
                versionMap.put(ns, currentReplicaVersion);
            }
        }

        boolean hasCallback = (callback != null);

        PartitionBackupReplicaAntiEntropyOperation op = new PartitionBackupReplicaAntiEntropyOperation(versionMap, hasCallback);
        op.setPartitionId(partitionId).setReplicaIndex(replicaIndex).setServiceName(SERVICE_NAME);
        OperationService operationService = nodeEngine.getOperationService();

        if (hasCallback) {
            operationService.createInvocationBuilder(SERVICE_NAME, op, target)
                            .setExecutionCallback(callback)
                            .setTryCount(OPERATION_TRY_COUNT)
                            .setTryPauseMillis(OPERATION_TRY_PAUSE_MILLIS)
                            .invoke();
        } else {
            operationService.send(op, target);
        }
    }

}
