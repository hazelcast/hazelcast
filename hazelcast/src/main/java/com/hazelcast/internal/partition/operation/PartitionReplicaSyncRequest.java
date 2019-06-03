/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.partition.PartitionReplica;
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
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.partition.PartitionReplicationEvent;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.ServiceNamespace;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeList;

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
public final class PartitionReplicaSyncRequest extends AbstractPartitionOperation
        implements PartitionAwareOperation, MigrationCycleOperation {

    private List<ServiceNamespace> namespaces;

    public PartitionReplicaSyncRequest() {
        namespaces = Collections.emptyList();
    }

    public PartitionReplicaSyncRequest(int partitionId, List<ServiceNamespace> namespaces, int replicaIndex) {
        this.namespaces = namespaces;
        setPartitionId(partitionId);
        setReplicaIndex(replicaIndex);
    }

    @Override
    public void beforeRun() {
        int syncReplicaIndex = getReplicaIndex();
        if (syncReplicaIndex < 1 || syncReplicaIndex > InternalPartition.MAX_BACKUP_COUNT) {
            throw new IllegalArgumentException("Replica index " + syncReplicaIndex
                    + " should be in the range [1-" + InternalPartition.MAX_BACKUP_COUNT + "]");
        }
    }

    @Override
    public void run() {
        InternalPartitionServiceImpl partitionService = getService();
        if (!partitionService.areMigrationTasksAllowed()) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Migration is paused! Cannot process request. partitionId="
                        + getPartitionId() + ", replicaIndex=" + getReplicaIndex() + ", namespaces=" + namespaces);
            }
            sendRetryResponse();
            return;
        }

        if (!checkPartitionOwner()) {
            sendRetryResponse();
            return;
        }

        int permits = partitionService.getReplicaManager().tryAcquireReplicaSyncPermits(namespaces.size());
        if (permits == 0) {
            logNotEnoughPermits();
            sendRetryResponse();
            return;
        }

        sendOperationsForNamespaces(permits);

        // send retry response for remaining namespaces
        if (!namespaces.isEmpty()) {
            logNotEnoughPermits();
            sendRetryResponse();
        }
    }

    private void logNotEnoughPermits() {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Not enough permits available! Cannot process request. partitionId="
                    + getPartitionId() + ", replicaIndex=" + getReplicaIndex() + ", namespaces=" + namespaces);
        }
    }

    /**
     * Send responses for first number of {@code permits} namespaces and remove them from the list.
     */
    private void sendOperationsForNamespaces(int permits) {
        InternalPartitionServiceImpl partitionService = getService();
        try {
            PartitionReplicationEvent event = new PartitionReplicationEvent(getPartitionId(), getReplicaIndex());
            Iterator<ServiceNamespace> iterator = namespaces.iterator();
            for (int i = 0; i < permits; i++) {
                ServiceNamespace namespace = iterator.next();
                Collection<Operation> operations;
                if (NonFragmentedServiceNamespace.INSTANCE.equals(namespace)) {
                    operations = createNonFragmentedReplicationOperations(event);
                } else {
                    operations = createFragmentReplicationOperations(event, namespace);
                }
                sendOperations(operations, namespace);
                iterator.remove();
            }
        } finally {
            partitionService.getReplicaManager().releaseReplicaSyncPermits(permits);
        }
    }

    private void sendOperations(Collection<Operation> operations, ServiceNamespace ns) {
        if (operations.isEmpty()) {
            logNoReplicaDataFound(getPartitionId(), ns, getReplicaIndex());
            sendResponse(null, ns);
        } else {
            sendResponse(operations, ns);
        }
    }

    /** Checks if we are the primary owner of the partition. */
    private boolean checkPartitionOwner() {
        InternalPartitionServiceImpl partitionService = getService();
        PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
        InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(getPartitionId());
        PartitionReplica owner = partition.getOwnerReplicaOrNull();

        NodeEngine nodeEngine = getNodeEngine();
        if (owner == null || !owner.isIdentical(nodeEngine.getLocalMember())) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("This node is not owner partition. Cannot process request. partitionId="
                        + getPartitionId() + ", replicaIndex=" + getReplicaIndex() + ", namespaces=" + namespaces);
            }
            return false;
        }
        return true;
    }

    /** Send a response to the replica to retry the replica sync */
    private void sendRetryResponse() {
        NodeEngine nodeEngine = getNodeEngine();
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();

        PartitionReplicaSyncRetryResponse response = new PartitionReplicaSyncRetryResponse(namespaces);
        response.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        Address target = getCallerAddress();
        OperationService operationService = nodeEngine.getOperationService();
        operationService.send(response, target);
    }

    /** Send a synchronization response to the caller replica containing the replication operations to be executed */
    private void sendResponse(Collection<Operation> operations, ServiceNamespace ns) {
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
        writeList(namespaces, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        namespaces = readList(in);
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.REPLICA_SYNC_REQUEST;
    }
}
