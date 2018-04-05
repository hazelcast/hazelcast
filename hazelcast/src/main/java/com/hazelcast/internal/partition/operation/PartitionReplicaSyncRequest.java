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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.internal.partition.ReplicaErrorLogger;
import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * The request sent from a replica to the partition owner to synchronize the replica data. The partition owner can send a
 * response to the replica to retry the sync operation when:
 * <ul>
 * <li>the replica sync is not allowed (because migrations are not allowed)</li>
 * <li>the operation was received by a node which is not the partition owner</li>
 * <li>the maximum number of parallel synchronizations has already been reached</li>
 * </ul>
 * An empty response can be sent if the current replica version is 0.
 */
// RU_COMPAT_39: Do not remove Versioned interface!
// Version info is needed on 3.9 members while deserializing the operation.
public final class PartitionReplicaSyncRequest extends AbstractPartitionOperation
        implements PartitionAwareOperation, MigrationCycleOperation, Versioned {

    private Collection<ServiceNamespace> allNamespaces;

    public PartitionReplicaSyncRequest() {
        allNamespaces = Collections.emptySet();
    }

    public PartitionReplicaSyncRequest(int partitionId, Collection<ServiceNamespace> namespaces, int replicaIndex) {
        this.allNamespaces = namespaces;
        setPartitionId(partitionId);
        setReplicaIndex(replicaIndex);
    }

    @Override
    public void beforeRun() throws Exception {
        int syncReplicaIndex = getReplicaIndex();
        if (syncReplicaIndex < 1 || syncReplicaIndex > InternalPartition.MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Replica index " + syncReplicaIndex
                    + " should be in the range [1-" + InternalPartition.MAX_BACKUP_COUNT + "]");
        }
    }

    @Override
    public void run() throws Exception {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();

        if (!partitionService.isMigrationAllowed()) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Migration is paused! Cannot run replica sync -> " + toString());
            }
            sendRetryResponse();
            return;
        }

        if (!preCheckReplicaSync(nodeEngine, partitionId, replicaIndex)) {
            return;
        }

        try {
            PartitionReplicationEvent event = new PartitionReplicationEvent(partitionId, replicaIndex);
            if (allNamespaces.remove(NonFragmentedServiceNamespace.INSTANCE)) {
                Collection<Operation> operations = createNonFragmentedReplicationOperations(event);
                sendOperations(operations, NonFragmentedServiceNamespace.INSTANCE);
            }

            for (ServiceNamespace namespace : allNamespaces) {
                Collection<Operation> operations = createFragmentReplicationOperations(event, namespace);
                sendOperations(operations, namespace);
            }
        } finally {
            partitionService.getReplicaManager().releaseReplicaSyncPermit();
        }
    }

    private void sendOperations(Collection<Operation> operations, ServiceNamespace ns) throws Exception {
        if (operations.isEmpty()) {
            logNoReplicaDataFound(getPartitionId(), ns, getReplicaIndex());
            sendResponse(null, ns);
        } else {
            sendResponse(operations, ns);
        }
    }

    /** Checks if we can continue with the replication or not. Can send a retry or empty response to the replica in some cases */
    private boolean preCheckReplicaSync(NodeEngineImpl nodeEngine, int partitionId, int replicaIndex) throws IOException {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
        InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);
        Address owner = partition.getOwnerOrNull();

        ILogger logger = getLogger();
        if (!nodeEngine.getThisAddress().equals(owner)) {
            if (logger.isFinestEnabled()) {
                logger.finest("Wrong target! " + toString() + " cannot be processed! Target should be: " + owner);
            }
            sendRetryResponse();
            return false;
        }

        if (!partitionService.getReplicaManager().tryToAcquireReplicaSyncPermit()) {
            if (logger.isFinestEnabled()) {
                logger.finest(
                        "Max parallel replication process limit exceeded! Could not run replica sync -> " + toString());
            }
            sendRetryResponse();
            return false;
        }
        return true;
    }

    /** Send a response to the replica to retry the replica sync */
    private void sendRetryResponse() {
        NodeEngine nodeEngine = getNodeEngine();
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();

        PartitionReplicaSyncRetryResponse response = new PartitionReplicaSyncRetryResponse(allNamespaces);
        response.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        Address target = getCallerAddress();
        OperationService operationService = nodeEngine.getOperationService();
        operationService.send(response, target);
    }

    /** Send a synchronization response to the caller replica containing the replication operations to be executed */
    private void sendResponse(Collection<Operation> operations, ServiceNamespace ns) throws IOException {
        NodeEngine nodeEngine = getNodeEngine();

        PartitionReplicaSyncResponse syncResponse = createResponse(operations, ns);
        Address target = getCallerAddress();
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Sending sync response to -> " + target + " for partitionId="
                    + getPartitionId() + ", replicaIndex=" + getReplicaIndex() + ", namespaces=" + ns);
        }

        // PartitionReplicaSyncResponse is TargetAware and sent directly without invocation system.
        syncResponse.setTarget(target);

        OperationService operationService = nodeEngine.getOperationService();
        operationService.send(syncResponse, target);
    }

    private PartitionReplicaSyncResponse createResponse(Collection<Operation> operations, ServiceNamespace ns) {
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();
        InternalPartitionService partitionService = getService();
        PartitionReplicaVersionManager versionManager = partitionService.getPartitionReplicaVersionManager();

        long[] versions = versionManager.getPartitionReplicaVersions(partitionId, ns);
        PartitionReplicaSyncResponse syncResponse = new PartitionReplicaSyncResponse(operations, ns, versions);
        syncResponse.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        return syncResponse;
    }

    private void logNoReplicaDataFound(int partitionId, ServiceNamespace namespace, int replicaIndex) {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("No replica data is found for partitionId=" + partitionId + ", replicaIndex=" + replicaIndex
                + ", namespace= " + namespace);
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeInt(allNamespaces.size());
        for (ServiceNamespace namespace : allNamespaces) {
            out.writeObject(namespace);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        allNamespaces = new ArrayList<ServiceNamespace>(len);
        for (int i = 0; i < len; i++) {
            ServiceNamespace ns = in.readObject();
            allNamespaces.add(ns);
        }
    }

    @Override
    public int getId() {
        return PartitionDataSerializerHook.REPLICA_SYNC_REQUEST;
    }
}
