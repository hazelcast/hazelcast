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
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.partition.ChunkSerDeHelper;
import com.hazelcast.internal.partition.ChunkSupplier;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.ReplicaErrorLogger;
import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.internal.partition.impl.PartitionReplicaManager;
import com.hazelcast.internal.partition.impl.PartitionStateManager;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.impl.Versioned;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.TargetAware;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;

import static com.hazelcast.internal.partition.ChunkSerDeHelper.readChunkedOperations;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.readNullableCollection;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeNullableCollection;
import static com.hazelcast.spi.impl.operationexecutor.OperationRunner.runDirect;
import static com.hazelcast.spi.impl.operationservice.OperationResponseHandlerFactory.createErrorLoggingResponseHandler;

/**
 * The replica synchronization response sent from the partition
 * owner to a replica. It will execute the received operation list
 * if the replica index hasn't changed. If the current replica
 * index is not the one sent by the partition owner, it will :
 * <ul>
 * <li>fail all received operations</li>
 * <li>cancel the current replica sync request</li>
 * <li>if the node is still a replica it will reschedule the replica synchronization request</li>
 * <li>if the node is not a replica anymore it will clear the replica versions for the partition</li>
 * </ul>
 */
@SuppressFBWarnings("EI_EXPOSE_REP")
public class PartitionReplicaSyncResponse extends AbstractPartitionOperation
        implements PartitionAwareOperation, BackupOperation, UrgentSystemOperation,
        AllowedDuringPassiveState, TargetAware, Versioned {

    private Collection<Operation> operations;
    private ServiceNamespace namespace;
    private long[] versions;

    private transient ChunkSerDeHelper chunkSerDeHelper;

    public PartitionReplicaSyncResponse() {
    }

    public PartitionReplicaSyncResponse(Collection<Operation> operations,
                                        Collection<ChunkSupplier> chunkSuppliers,
                                        ServiceNamespace namespace,
                                        long[] versions,
                                        boolean chunkedMigrationEnabled,
                                        int maxTotalChunkedDataInBytes,
                                        ILogger logger,
                                        int partitionId) {
        this.operations = operations;
        this.namespace = namespace;
        this.versions = versions;
        this.chunkSerDeHelper = new ChunkSerDeHelper(logger, partitionId,
                chunkSuppliers, chunkedMigrationEnabled, maxTotalChunkedDataInBytes);
    }

    @Override
    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionServiceImpl partitionService = getService();
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();

        PartitionStateManager partitionStateManager = partitionService.getPartitionStateManager();
        InternalPartitionImpl partition = partitionStateManager.getPartitionImpl(partitionId);
        int currentReplicaIndex = partition.getReplicaIndex(PartitionReplica.from(nodeEngine.getLocalMember()));
        try {
            if (replicaIndex == currentReplicaIndex) {
                executeOperations();
            } else {
                nodeNotOwnsBackup(partition);
            }
            if (operations != null) {
                operations.clear();
            }
        } finally {
            postProcessReplicaSync(partitionService, currentReplicaIndex);
        }
    }

    private void postProcessReplicaSync(InternalPartitionServiceImpl partitionService, int currentReplicaIndex) {
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();

        PartitionReplicaManager replicaManager = partitionService.getReplicaManager();
        if (replicaIndex == currentReplicaIndex) {
            replicaManager.finalizeReplicaSync(partitionId, replicaIndex, namespace, versions);
        } else {
            replicaManager.clearReplicaSyncRequest(partitionId, namespace, replicaIndex);
            if (currentReplicaIndex < 0) {
                replicaManager.clearPartitionReplicaVersions(partitionId, namespace);
            }
        }
    }

    /**
     * Fail all replication operations with the exception that this node is no longer the replica with the sent index
     */
    private void nodeNotOwnsBackup(InternalPartitionImpl partition) {
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();
        NodeEngine nodeEngine = getNodeEngine();

        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            int currentReplicaIndex = partition.getReplicaIndex(PartitionReplica.from(nodeEngine.getLocalMember()));
            logger.finest(
                    "This node is not backup replica of partitionId=" + partitionId + ", replicaIndex=" + replicaIndex
                            + " anymore. current replicaIndex=" + currentReplicaIndex);
        }

        if (operations != null) {
            PartitionReplica replica = partition.getReplica(replicaIndex);
            Member targetMember = null;
            if (replica != null) {
                ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
                targetMember = clusterService.getMember(replica.address(), replica.uuid());
            }
            Throwable throwable = new WrongTargetException(nodeEngine.getLocalMember(), targetMember, partitionId,
                    replicaIndex, getClass().getName());
            for (Operation op : operations) {
                prepareOperation(op);
                onOperationFailure(op, throwable);
            }
        }
    }

    private void executeOperations() {
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();
        if (operations != null && !operations.isEmpty()) {
            logApplyReplicaSync(partitionId, replicaIndex);
            for (Operation op : operations) {
                prepareOperation(op);
                try {
                    runDirect(op);
                } catch (Throwable e) {
                    onOperationFailure(op, e);
                    logException(op, e);
                }
            }
        } else {
            logEmptyTaskList(partitionId, replicaIndex);
        }
    }

    private void prepareOperation(Operation op) {
        int partitionId = getPartitionId();
        int replicaIndex = getReplicaIndex();
        NodeEngine nodeEngine = getNodeEngine();

        ILogger opLogger = nodeEngine.getLogger(op.getClass());
        OperationResponseHandler responseHandler = createErrorLoggingResponseHandler(opLogger);

        op.setNodeEngine(nodeEngine).setPartitionId(partitionId)
                .setReplicaIndex(replicaIndex).setOperationResponseHandler(responseHandler);
    }

    private void logEmptyTaskList(int partitionId, int replicaIndex) {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("No data available for replica sync, partitionId=" + partitionId
                    + ", replicaIndex=" + replicaIndex + ", namespace=" + namespace
                    + ", versions=" + Arrays.toString(versions));
        }
    }

    private void logException(Operation op, Throwable e) {
        ILogger logger = getLogger();
        NodeEngine nodeEngine = getNodeEngine();
        Level level = nodeEngine.isRunning() ? Level.WARNING : Level.FINEST;
        if (logger.isLoggable(level)) {
            logger.log(level, "While executing " + op, e);
        }
    }

    private void logApplyReplicaSync(int partitionId, int replicaIndex) {
        ILogger logger = getLogger();
        if (logger.isFinestEnabled()) {
            logger.finest("Applying replica sync for partitionId=" + partitionId
                    + ", replicaIndex=" + replicaIndex + ", namespace=" + namespace
                    + ", versions=" + Arrays.toString(versions));
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public String getServiceName() {
        return InternalPartitionService.SERVICE_NAME;
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        if (operations != null) {
            for (Operation op : operations) {
                prepareOperation(op);
                onOperationFailure(op, e);
            }
        }
    }

    private void onOperationFailure(Operation op, Throwable e) {
        try {
            op.onExecutionFailure(e);
        } catch (Throwable t) {
            getLogger().warning("While calling operation.onFailure(). op: " + op, t);
        }
    }

    @Override
    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
    }

    @Override
    public void setTarget(Address address) {
        if (operations != null) {
            for (Operation op : operations) {
                if (op instanceof TargetAware) {
                    ((TargetAware) op).setTarget(address);
                }
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(namespace);
        out.writeLongArray(versions);
        writeNullableCollection(operations, out);
        chunkSerDeHelper.writeChunkedOperations(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        namespace = in.readObject();
        versions = in.readLongArray();
        operations = readNullableCollection(in);
        operations = readChunkedOperations(in, operations);
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", namespace=").append(namespace);
        sb.append(", versions=").append(Arrays.toString(versions));
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.REPLICA_SYNC_RESPONSE;
    }
}
