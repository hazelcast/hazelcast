/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.internal.partition.operation.PartitionReplicaSyncRequestOffloadable.PartitionNamespaceTuple.tupleOf;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.readCollection;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;
import static com.hazelcast.internal.util.ThreadUtil.isRunningOnPartitionThread;

/**
 * The request sent from a replica to the partition owner to synchronize the replica data. The partition owner can send a
 * response to the replica to retry the sync operation when:
 * <ul>
 * <li>the replica sync is not allowed (because migrations are not allowed)</li>
 * <li>the operation was received by a node which is not the partition owner</li>
 * <li>the maximum number of parallel synchronizations has already been reached</li>
 * </ul>
 * An empty response can be sent if the current replica version is 0.
 *
 * @since   5.0
 */
public final class PartitionReplicaSyncRequestOffloadable
        extends PartitionReplicaSyncRequest {

    private final transient ConcurrentMap<PartitionNamespaceTuple, long[]> replicaVersions = new ConcurrentHashMap<>();
    private volatile int partitionId;

    public PartitionReplicaSyncRequestOffloadable() {
        namespaces = Collections.emptyList();
    }

    public PartitionReplicaSyncRequestOffloadable(int partitionId, Collection<ServiceNamespace> namespaces, int replicaIndex) {
        this.namespaces = Collections.newSetFromMap(new ConcurrentHashMap<>());
        this.namespaces.addAll(namespaces);
        this.partitionId = partitionId;
        setPartitionId(-1);
        setReplicaIndex(replicaIndex);
    }

    @Override
    public CallStatus call()
            throws Exception {
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
            // It is only safe to read replica versions before preparing replication operations.
            // Reasoning: even though partition is already marked as migrating,
            // operations may be already queued in partition thread.
            // If we read replica versions after replication operation
            // is prepared, we may read updated replica versions but replication op
            // may have stale data -> future backup sync checks will not detect the
            // stale data.
            readReplicaVersions();

            final Iterator<ServiceNamespace> iterator = namespaces.iterator();
            for (int i = 0; i < permits; i++) {
                ServiceNamespace namespace = iterator.next();
                Collection<Operation> operations;
                if (NonFragmentedServiceNamespace.INSTANCE.equals(namespace)) {
                    operations = createNonFragmentedReplicationOperations(event);
                } else {
                    operations = createFragmentReplicationOperationsOffload(event, namespace);
                }
                // operations can be null if await-ing for non-fragmented services' repl operations failed due to interruption
                if (operations != null) {
                    operations = new CopyOnWriteArrayList<>(operations);
                    sendOperationsOnPartitionThread(operations, namespace);
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
                        // make a copy because getPartitionReplicaVersions returns references
                        // to the internal replica versions data structures that may change under our feet
                        long[] versions = Arrays.copyOf(versionManager.getPartitionReplicaVersions(partitionId(), ns),
                                IPartition.MAX_BACKUP_COUNT);
                        replicaVersions.put(tupleOf(partitionId(), ns), versions);
                    }
                });
        operationService.execute(gatherReplicaVersionsRunnable);
        gatherReplicaVersionsRunnable.future.joinInternal();
    }

    @Override
    protected int partitionId() {
        return this.partitionId;
    }

    private void sendOperationsOnPartitionThread(Collection<Operation> operations, ServiceNamespace ns) {
        if (isRunningOnPartitionThread()) {
            sendOperations(operations, ns);
        } else {
            UrgentPartitionRunnable partitionRunnable = new UrgentPartitionRunnable(partitionId(),
                    () -> sendOperations(operations, ns));
            getNodeEngine().getOperationService().execute(partitionRunnable);
            partitionRunnable.future.joinInternal();
        }
    }

    @Override
    protected PartitionReplicaSyncResponse createResponse(Collection<Operation> operations, ServiceNamespace ns) {
        int partitionId = partitionId();
        int replicaIndex = getReplicaIndex();
        long[] versions = replicaVersions.get(tupleOf(partitionId, ns));
        PartitionReplicaSyncResponse syncResponse = new PartitionReplicaSyncResponse(operations, ns, versions);
        syncResponse.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
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
            InternalPartitionServiceImpl partitionService = getService();
            PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
            // set partition as migrating to disable mutating operations
            // while preparing replication operations
            if (!partitionStateManager.trySetMigratingFlag(partitionId)
                    && !partitionStateManager.isMigrating(partitionId)) {
                throw new RetryableHazelcastException("Cannot set migrating flag, "
                        + "probably previous migration's finalization is not completed yet.");
            }

            try {
                // executed on generic operation thread
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
                partitionStateManager.clearMigratingFlag(partitionId);
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

    static final class PartitionNamespaceTuple {
        final int partitionId;
        final ServiceNamespace ns;

        private PartitionNamespaceTuple(int partitionId, ServiceNamespace ns) {
            this.partitionId = partitionId;
            this.ns = ns;
        }

        static PartitionNamespaceTuple tupleOf(int partitionId, ServiceNamespace ns) {
            return new PartitionNamespaceTuple(partitionId, ns);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartitionNamespaceTuple that = (PartitionNamespaceTuple) o;
            return partitionId == that.partitionId && Objects.equals(ns, that.ns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionId, ns);
        }
    }
}
