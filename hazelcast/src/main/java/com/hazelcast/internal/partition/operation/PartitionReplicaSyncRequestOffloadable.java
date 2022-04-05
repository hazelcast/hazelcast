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

import com.hazelcast.internal.partition.ChunkSupplier;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readCollection;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;
import static com.hazelcast.internal.util.CollectionUtil.isEmpty;
import static com.hazelcast.internal.util.CollectionUtil.isNotEmpty;
import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;

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
 *
 * @since 5.0
 */
public final class PartitionReplicaSyncRequestOffloadable
        extends PartitionReplicaSyncRequest {

    private final transient ConcurrentMap<BiTuple, long[]> replicaVersions = new ConcurrentHashMap<>();
    private volatile int partitionId;

    public PartitionReplicaSyncRequestOffloadable() {
        namespaces = Collections.emptyList();
    }

    public PartitionReplicaSyncRequestOffloadable(Collection<ServiceNamespace> namespaces,
                                                  int partitionId, int replicaIndex) {
        this.namespaces = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.namespaces.addAll(namespaces);
        this.partitionId = partitionId;
        setPartitionId(-1);
        setReplicaIndex(replicaIndex);
    }

    @Override
    public CallStatus call() throws Exception {
        return new ReplicaSyncRequestOffload();
    }

    /**
     * Send responses for first number of {@code permits} namespaces and remove them from the list.
     */
    protected void sendOperationsForNamespaces(int permits) {
        InternalPartitionServiceImpl partitionService = getService();
        try {
            PartitionReplicationEvent event = new PartitionReplicationEvent(getCallerAddress(), partitionId,
                    getReplicaIndex());
            // It is only safe to read replica versions before
            // preparing replication operations. Reasoning: even
            // though partition is already marked as migrating,
            // operations may be already queued in partition thread.
            // If we read replica versions after replication operation
            // is prepared, we may read updated replica versions
            // but replication op may have stale data -> future
            // backup sync checks will not detect the stale data.
            readReplicaVersions();

            final Iterator<ServiceNamespace> iterator = namespaces.iterator();
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
                        operations = createFragmentReplicationOperationsOffload(event, namespace);
                    }
                }
                // operations can be null if await-ing
                // for non-fragmented services' repl
                // operations failed due to interruption
                if (isNotEmpty(operations) || isNotEmpty(chunkSuppliers)) {
                    // TODO why using new CopyOnWriteArrayList<>(chunkSuppliers)?
                    sendOperationsOnPartitionThread(new CopyOnWriteArrayList<>(operations),
                            new CopyOnWriteArrayList<>(chunkSuppliers), namespace);
                    while (hasRemainingChunksToSend(chunkSuppliers)) {
                        sendOperationsOnPartitionThread(new CopyOnWriteArrayList<>(operations),
                                new CopyOnWriteArrayList<>(chunkSuppliers), namespace);
                    }
                    iterator.remove();
                }
            }
        } finally {
            partitionService.getReplicaManager().releaseReplicaSyncPermits(permits);
        }
    }

    private void readReplicaVersions() {
        InternalPartitionServiceImpl partitionService = getService();
        OperationService operationService = getNodeEngine().getOperationService();
        PartitionReplicaVersionManager versionManager = partitionService.getPartitionReplicaVersionManager();
        UrgentPartitionRunnable<Void> gatherReplicaVersionsRunnable = new UrgentPartitionRunnable<>(partitionId(),
                () -> {
                    for (ServiceNamespace ns : namespaces) {
                        // make a copy because
                        // getPartitionReplicaVersions
                        // returns references to the internal
                        // replica versions data structures
                        // that may change under our feet
                        long[] versions = Arrays.copyOf(versionManager.getPartitionReplicaVersions(partitionId(), ns),
                                IPartition.MAX_BACKUP_COUNT);
                        replicaVersions.put(BiTuple.of(partitionId(), ns), versions);
                    }
                });
        operationService.execute(gatherReplicaVersionsRunnable);
        gatherReplicaVersionsRunnable.future.joinInternal();
    }

    @Override
    protected int partitionId() {
        return this.partitionId;
    }

    private void sendOperationsOnPartitionThread(Collection<Operation> operations,
                                                 Collection<ChunkSupplier> chunkSuppliers,
                                                 ServiceNamespace ns) {
        if (isRunningOnPartitionThread()) {
            sendOperations(operations, chunkSuppliers, ns);
        } else {
            UrgentPartitionRunnable partitionRunnable = new UrgentPartitionRunnable(partitionId(),
                    () -> sendOperations(operations, chunkSuppliers, ns));
            getNodeEngine().getOperationService().execute(partitionRunnable);
            partitionRunnable.future.joinInternal();
        }
    }

    @Override
    protected PartitionReplicaSyncResponse createResponse(Collection<Operation> operations,
                                                          Collection<ChunkSupplier> chunkSuppliers,
                                                          ServiceNamespace ns) {
        int partitionId = partitionId();
        int replicaIndex = getReplicaIndex();
        long[] versions = replicaVersions.get(BiTuple.of(partitionId, ns));

        PartitionReplicaSyncResponse syncResponse
                = new PartitionReplicaSyncResponse(operations, chunkSuppliers, ns,
                versions, isChunkedMigrationEnabled(), getMaxTotalChunkedDataInBytes(),
                getLogger(), partitionId);

        syncResponse.setPartitionId(partitionId)
                .setReplicaIndex(replicaIndex);

        return syncResponse;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.REPLICA_SYNC_REQUEST_OFFLOADABLE;
    }

    final class ReplicaSyncRequestOffload extends Offload {

        ReplicaSyncRequestOffload() {
            super(PartitionReplicaSyncRequestOffloadable.this);
        }

        @Override
        public void start() throws Exception {
            try {
                nodeEngine.getExecutionService().execute(ExecutionService.ASYNC_EXECUTOR,
                        () -> {
                            // set partition as migrating to disable mutating
                            // operations while preparing replication operations
                            if (!trySetMigratingFlag()) {
                                sendRetryResponse();
                            }

                            try {
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
                            } finally {
                                clearMigratingFlag();
                            }
                        });
            } catch (RejectedExecutionException e) {
                // if execution on async executor was rejected, then send retry response
                sendRetryResponse();
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        writeCollection(namespaces, out);
        out.writeInt(partitionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        namespaces = Collections.newSetFromMap(new ConcurrentHashMap<>());
        namespaces.addAll(readCollection(in));
        partitionId = in.readInt();
    }

    private boolean trySetMigratingFlag() {
        InternalPartitionServiceImpl partitionService = getService();
        PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
        UrgentPartitionRunnable<Boolean> trySetMigrating = new UrgentPartitionRunnable<>(partitionId(),
                () -> partitionStateManager.trySetMigratingFlag(partitionId()));
        getNodeEngine().getOperationService().execute(trySetMigrating);
        return trySetMigrating.future.joinInternal();
    }

    private void clearMigratingFlag() {
        InternalPartitionServiceImpl partitionService = getService();
        PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
        UrgentPartitionRunnable<Void> trySetMigrating = new UrgentPartitionRunnable<>(partitionId(),
                () -> partitionStateManager.clearMigratingFlag(partitionId()));
        getNodeEngine().getOperationService().execute(trySetMigrating);
        trySetMigrating.future.joinInternal();
    }
}
