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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.partition.ChunkSupplier;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.MigrationCycleOperation;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.partition.ReplicaErrorLogger;
import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.MigrationManager;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readCollection;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;
import static com.hazelcast.internal.util.CollectionUtil.isEmpty;

/**
 * The request sent from a replica to the partition owner to
 * synchronize the replica data. The partition owner can send
 * a response to the replica to retry the sync operation when:
 * <ul>
 * <li>the replica sync is not allowed (because migrations are not allowed)</li>
 * <li>the operation was received by a node which is not the partition owner</li>
 * <li>the maximum number of parallel synchronizations has already been reached</li>
 * </ul>
 * An empty response can be sent if the current replica version is 0.
 */
public class PartitionReplicaSyncRequest extends AbstractPartitionOperation
        implements PartitionAwareOperation, MigrationCycleOperation {

    protected volatile Collection<ServiceNamespace> namespaces;

    public PartitionReplicaSyncRequest() {
        namespaces = Collections.emptyList();
    }

    public PartitionReplicaSyncRequest(Collection<ServiceNamespace> namespaces,
                                       int partitionId, int replicaIndex) {
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
        Integer permits = getPermits();
        if (permits == null) {
            return;
        }

        sendOperationsForNamespaces(permits);

        // send retry response for remaining namespaces
        if (!namespaces.isEmpty()) {
            logNotEnoughPermits();
            sendRetryResponse();
        }
    }

    // partition ID for which replica sync is requested. Overridden in offloaded version.
    protected int partitionId() {
        return getPartitionId();
    }

    @Nullable
    protected Integer getPermits() {
        InternalPartitionServiceImpl partitionService = getService();
        if (!partitionService.areMigrationTasksAllowed()) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Migration is paused! Cannot process request. partitionId="
                        + partitionId() + ", replicaIndex=" + getReplicaIndex() + ", namespaces=" + namespaces);
            }
            sendRetryResponse();
            return null;
        }

        if (!checkPartitionOwner()) {
            sendRetryResponse();
            return null;
        }

        int permits = partitionService.getReplicaManager().tryAcquireReplicaSyncPermits(namespaces.size());
        if (permits == 0) {
            logNotEnoughPermits();
            sendRetryResponse();
            return null;
        }
        return permits;
    }

    protected void logNotEnoughPermits() {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Not enough permits available! Cannot process request. partitionId="
                    + partitionId() + ", replicaIndex=" + getReplicaIndex() + ", namespaces=" + namespaces);
        }
    }

    /**
     * Send responses for first number of {@code permits} namespaces and remove them from the list.
     */
    protected void sendOperationsForNamespaces(int permits) {
        InternalPartitionServiceImpl partitionService = getService();
        try {
            PartitionReplicationEvent event = new PartitionReplicationEvent(getCallerAddress(),
                    partitionId(), getReplicaIndex());
            Iterator<ServiceNamespace> iterator = namespaces.iterator();
            for (int i = 0; i < permits; i++) {
                ServiceNamespace namespace = iterator.next();

                Collection<Operation> operations = Collections.emptyList();
                Collection<ChunkSupplier> chunkSuppliers = Collections.emptyList();

                if (NonFragmentedServiceNamespace.INSTANCE.equals(namespace)) {
                    operations = createNonFragmentedReplicationOperations(event);
                } else {
                    chunkSuppliers = isChunkedMigrationEnabled()
                            ? collectChunkSuppliers(event, namespace) : chunkSuppliers;
                    if (isEmpty(chunkSuppliers)) {
                        operations = createFragmentReplicationOperations(event, namespace);
                    }
                }

                sendOperations(operations, chunkSuppliers, namespace);

                while (hasRemainingChunksToSend(chunkSuppliers)) {
                    sendOperations(operations, chunkSuppliers, namespace);
                }

                iterator.remove();

            }
        } finally {
            partitionService.getReplicaManager().releaseReplicaSyncPermits(permits);
        }
    }

    protected boolean hasRemainingChunksToSend(Collection<ChunkSupplier> chunkSuppliers) {
        if (!isChunkedMigrationEnabled()) {
            return false;
        }

        Iterator<ChunkSupplier> iterator = chunkSuppliers.iterator();
        while (iterator.hasNext()) {
            ChunkSupplier chunkSupplier = iterator.next();
            if (chunkSupplier.hasNext()) {
                return true;
            } else {
                iterator.remove();
            }
        }
        return false;
    }

    protected void sendOperations(Collection<Operation> operations,
                                  Collection<ChunkSupplier> chunkSuppliers,
                                  ServiceNamespace ns) {
        if (isEmpty(operations) && isEmpty(chunkSuppliers)) {
            logNoReplicaDataFound(partitionId(), ns, getReplicaIndex());
            sendResponse(null, null, ns);
        } else {
            sendResponse(operations, chunkSuppliers, ns);
        }
    }

    /**
     * Checks if we are the primary owner of the partition.
     */
    protected boolean checkPartitionOwner() {
        InternalPartitionServiceImpl partitionService = getService();
        PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
        InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId());
        PartitionReplica owner = partition.getOwnerReplicaOrNull();

        NodeEngine nodeEngine = getNodeEngine();
        if (owner == null || !owner.isIdentical(nodeEngine.getLocalMember())) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("This node is not owner partition. Cannot process request. partitionId="
                        + partitionId() + ", replicaIndex=" + getReplicaIndex() + ", namespaces=" + namespaces);
            }
            return false;
        }
        return true;
    }

    /**
     * Send a response to the replica to retry the replica sync
     */
    protected void sendRetryResponse() {
        NodeEngine nodeEngine = getNodeEngine();
        int partitionId = partitionId();
        int replicaIndex = getReplicaIndex();

        PartitionReplicaSyncRetryResponse response = new PartitionReplicaSyncRetryResponse(namespaces);
        response.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        Address target = getCallerAddress();
        OperationService operationService = nodeEngine.getOperationService();
        operationService.send(response, target);
    }

    /**
     * Send a synchronization response to the caller replica containing the replication operations to be executed
     */
    private void sendResponse(Collection<Operation> operations,
                              Collection<ChunkSupplier> chunkSuppliers,
                              ServiceNamespace ns) {
        NodeEngine nodeEngine = getNodeEngine();

        PartitionReplicaSyncResponse syncResponse = createResponse(operations, chunkSuppliers, ns);
        Address target = getCallerAddress();
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Sending sync response to -> " + target + " for partitionId="
                    + partitionId() + ", replicaIndex=" + getReplicaIndex() + ", namespaces=" + ns);
        }

        // PartitionReplicaSyncResponse is TargetAware and sent directly without invocation system.
        syncResponse.setTarget(target);

        OperationService operationService = nodeEngine.getOperationService();
        operationService.send(syncResponse, target);
    }

    protected PartitionReplicaSyncResponse createResponse(Collection<Operation> operations,
                                                          Collection<ChunkSupplier> chunkSuppliers,
                                                          ServiceNamespace ns) {
        int partitionId = partitionId();
        int replicaIndex = getReplicaIndex();
        InternalPartitionServiceImpl partitionService = getService();
        PartitionReplicaVersionManager versionManager = partitionService.getPartitionReplicaVersionManager();

        long[] versions = versionManager.getPartitionReplicaVersions(partitionId, ns);
        PartitionReplicaSyncResponse syncResponse = new PartitionReplicaSyncResponse(operations,
                chunkSuppliers, ns, versions,
                isChunkedMigrationEnabled(),
                getMaxTotalChunkedDataInBytes(),
                getLogger(), partitionId);
        syncResponse.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
        return syncResponse;
    }

    protected final boolean isChunkedMigrationEnabled() {
        InternalPartitionServiceImpl partitionService = getService();
        return partitionService.getMigrationManager().isChunkedMigrationEnabled();
    }

    protected final int getMaxTotalChunkedDataInBytes() {
        InternalPartitionServiceImpl partitionService = getService();
        MigrationManager migrationManager = partitionService.getMigrationManager();
        return migrationManager.getMaxTotalChunkedDataInBytes();
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
        writeCollection(namespaces, out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        namespaces = readCollection(in);
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.REPLICA_SYNC_REQUEST;
    }
}
