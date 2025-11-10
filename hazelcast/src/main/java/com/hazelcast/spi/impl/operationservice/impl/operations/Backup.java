/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl.operations;

import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.internal.partition.ReplicaErrorLogger;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.AsynchronouslyExecutingBackupOperation;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationAccessor;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import static com.hazelcast.internal.nio.IOUtil.readDataAsObject;
import static com.hazelcast.internal.partition.IPartition.MAX_BACKUP_COUNT;
import static com.hazelcast.spi.impl.operationexecutor.OperationRunner.runDirect;
import static com.hazelcast.spi.impl.operationservice.OperationResponseHandlerFactory.createEmptyResponseHandler;

public final class Backup extends Operation implements BackupOperation, AllowedDuringPassiveState,
        IdentifiedDataSerializable, Consumer<Operation> {

    private Address originalCaller;
    private ServiceNamespace namespace;
    private long[] replicaVersions;
    private boolean sync;

    private Operation backupOp;
    private Data backupOpData;

    private transient Throwable validationFailure;
    private transient boolean backupOperationInitialized;
    private transient boolean offloaded;

    private long clientCorrelationId;

    public Backup() {
    }

    public Backup(Operation backupOp, Address originalCaller, long[] replicaVersions, boolean sync) {
        this(backupOp, originalCaller, replicaVersions, sync, -1);
    }

    public Backup(Operation backupOp, Address originalCaller, long[] replicaVersions, boolean sync, long clientCorrelationId) {
        this.backupOp = backupOp;
        this.originalCaller = originalCaller;
        this.sync = sync;
        this.replicaVersions = replicaVersions;
        if (sync && originalCaller == null) {
            throw new IllegalArgumentException("Sync backup requires original caller address, Backup operation: "
                    + backupOp);
        }
        this.clientCorrelationId = clientCorrelationId;
    }

    public Backup(Data backupOpData, Address originalCaller, long[] replicaVersions, boolean sync) {
        this(backupOpData, originalCaller, replicaVersions, sync, -1);
    }

    public Backup(Data backupOpData, Address originalCaller, long[] replicaVersions, boolean sync, long clientCorrelationId) {
        this.backupOpData = backupOpData;
        this.originalCaller = originalCaller;
        this.sync = sync;
        this.replicaVersions = replicaVersions;
        if (sync && originalCaller == null) {
            throw new IllegalArgumentException("Sync backup requires original caller address, Backup operation data: "
                    + backupOpData);
        }
        this.clientCorrelationId = clientCorrelationId;
    }

    public Operation getBackupOp() {
        return backupOp;
    }

    @Override
    public void beforeRun() {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        int partitionId = getPartitionId();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        ILogger logger = getLogger();

        ensureBackupOperationInitialized();
        PartitionReplicaVersionManager versionManager = partitionService.getPartitionReplicaVersionManager();
        namespace = versionManager.getServiceNamespace(backupOp);

        if (!nodeEngine.getNode().getNodeExtension().isStartCompleted()) {
            validationFailure = new IllegalStateException("Ignoring backup! "
                    + "Backup operation is received before startup is completed.");
            if (logger.isFinestEnabled()) {
                logger.finest(validationFailure.getMessage());
            }
            return;
        }

        InternalPartition partition = partitionService.getPartition(partitionId);
        PartitionReplica owner = partition.getReplica(getReplicaIndex());
        if (owner == null || !owner.isIdentical(nodeEngine.getLocalMember())) {
            validationFailure = new IllegalStateException("Wrong target! " + toString()
                    + " cannot be processed! Target should be: " + owner);
            if (logger.isFinestEnabled()) {
                logger.finest(validationFailure.getMessage());
            }
            return;
        }
        if (versionManager.isPartitionReplicaVersionStale(getPartitionId(), namespace,
                replicaVersions, getReplicaIndex())) {
            validationFailure = new IllegalStateException("Ignoring stale backup with namespace: " + namespace
                    + ", versions: " + Arrays.toString(replicaVersions));
            if (logger.isFineEnabled()) {
                long[] currentVersions = versionManager.getPartitionReplicaVersions(partitionId, namespace);
                logger.fine("Ignoring stale backup! namespace: " + namespace
                        + ", Current-versions: " + Arrays.toString(currentVersions)
                        + ", Backup-versions: " + Arrays.toString(replicaVersions));
            }
            return;
        }
    }

    private void ensureBackupOperationInitialized() {
        if (!backupOperationInitialized) {
            backupOperationInitialized = true;
            backupOp.setNodeEngine(getNodeEngine());
            backupOp.setPartitionId(getPartitionId());
            backupOp.setReplicaIndex(getReplicaIndex());
            backupOp.setCallerUuid(getCallerUuid());
            OperationAccessor.setCallerAddress(backupOp, getCallerAddress());
            OperationAccessor.setInvocationTime(backupOp, Clock.currentTimeMillis());
            backupOp.setOperationResponseHandler(createEmptyResponseHandler());
        }
    }

    @Override
    public void run() throws Exception {
        if (validationFailure != null) {
            onExecutionFailure(validationFailure);
            return;
        }

        ensureBackupOperationInitialized();
        NodeEngineImpl nodeEngineImpl = (NodeEngineImpl) getNodeEngine();
        Set<Operation> asyncOperations = nodeEngineImpl.getOperationService().getAsyncOperations();
        CallStatus callStatus = runDirect(backupOp, nodeEngineImpl, asyncOperations, this);

        // When implementing offloaded or blocking backup operations it is very important to:
        // 1) preserve order of backups operations (for primary operations it does not matter
        // as they do not have a defined order until they enter execution on partition thread).
        // This is easy to mix with blocking backup operations because returning WAIT from Operation.call()
        // puts the operation at the end of the queue. Order will be preserved if
        // after `BlockingOperation.shouldWait()` returned true, subsequent `Operation.call()` does not return WAIT.
        // For offloaded operations care must be taken not to execute multiple backup operations
        // concurrently and possibly out of order or the operations must be commutative.
        // 2) send backup ack using callback provided via OffloadedBackupOperation to avoid waiting
        // for backup ack timeouts.
        //
        // Offloaded and blocking backups should be used with care with sync backups as they can increase
        // latency if the wait on backup is long and cause backup-timeouts InvocationMonitor log messages.
        offloaded = callStatus.ordinal() == CallStatus.OFFLOAD_ORDINAL || callStatus == CallStatus.WAIT;

        assert !offloaded || backupOp instanceof AsynchronouslyExecutingBackupOperation
                : "Offloaded or blocking backup operation must implement AsynchronouslyExecutingBackupOperation";

        // Error handling for backup ops:
        //
        // Backup (entire operation) can be rejected due to partition migrations - this is handled by
        // Backup.onExecutionFailure and normal invocation retries and Backup.run is not even invoked in this case.
        //
        // If blocking backup operation throws on first attempt (without WAIT) this will propagate to OperationRunner.run
        // and ultimately entire Backup will fail, Backup.onExecutionFailure will be invoked, and not sent
        // and, crucially, replica version not updated which will later trigger anti-entropy sync.
        //
        // If a blocking/offloaded backup operation waits and subsequently fails either during its own execution or because
        // it is rejected due to partitions being migrated, it MUST mark replica as requiring sync so the discrepancy
        // can be fixed. This also should be done in onExecutionFailure. It can be done when the operation was actually
        // offloaded/waiting to avoid redundant marking in previous cases, but because this should be rare case
        // it can also be done for all errors.
        // See UtilSteps.HANDLE_ERROR.
        //
        // isClusterSafe/isMemberSafe is not fully reliable with blocking backups.

        PartitionReplicaVersionManager versionManager = nodeEngineImpl.getPartitionService().getPartitionReplicaVersionManager();
        versionManager.updatePartitionReplicaVersions(getPartitionId(), namespace, replicaVersions, getReplicaIndex());
    }

    @Override
    public void accept(Operation operation) {
        assert operation instanceof AsynchronouslyExecutingBackupOperation
                : "Only expected to run when offloading is enabled";

        this.afterRunInternal();
    }

    @Override
    public void afterRun() throws Exception {
        if (offloaded) {
            return;
        }

        this.afterRunInternal();
    }

    private void afterRunInternal() {
        if (validationFailure != null || !sync || getCallId() == 0 || originalCaller == null) {
            return;
        }

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        long callId = getCallId();
        OperationServiceImpl operationService = nodeEngine.getOperationService();

        if (isCallerClient()) {
            ClientEngine clientEngine = getNodeEngine().getNode().getClientEngine();
            UUID clientUUID = getCallerUuid();
            clientEngine.dispatchBackupEvent(clientUUID, clientCorrelationId);
        } else if (nodeEngine.getThisAddress().equals(originalCaller)) {
            operationService.getBackupHandler().notifyBackupComplete(callId);
        } else {
            operationService.getOutboundResponseHandler()
                    .sendBackupAck(getConnection().getConnectionManager(),
                            originalCaller, callId, backupOp.isUrgent());
        }
    }

    private boolean isCallerClient() {
        return clientCorrelationId != -1;
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
    public void onExecutionFailure(Throwable e) {
        if (backupOp != null) {
            try {
                // Ensure that backup operation is initialized.
                // If there is an exception before `run` (for example caller is not valid anymore),
                // backup operation will not be initialized.
                ensureBackupOperationInitialized();
                backupOp.onExecutionFailure(e);
            } catch (Throwable t) {
                getLogger().warning("While calling operation.onFailure(). op: " + backupOp, t);
            }
        }
    }

    @Override
    public void logError(Throwable e) {
        if (backupOp != null) {
            // Ensure that backup operation is initialized.
            // If there is an exception before `run` (for example caller is not valid anymore),
            // backup operation will not be initialized.
            ensureBackupOperationInitialized();
            backupOp.logError(e);
        } else {
            ReplicaErrorLogger.log(e, getLogger());
        }
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        if (backupOpData == null) {
            out.writeBoolean(false);
            out.writeObject(backupOp);
        } else {
            out.writeBoolean(true);
            IOUtil.writeData(out, backupOpData);
        }

        if (originalCaller == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            originalCaller.writeData(out);
        }

        byte replicaVersionCount = 0;
        for (int k = 0; k < replicaVersions.length; k++) {
            if (replicaVersions[k] != 0) {
                replicaVersionCount = (byte) (k + 1);
            }
        }

        out.writeByte(replicaVersionCount);
        for (int k = 0; k < replicaVersionCount; k++) {
            out.writeLong(replicaVersions[k]);
        }

        out.writeBoolean(sync);
        out.writeLong(clientCorrelationId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        if (in.readBoolean()) {
            backupOp = readDataAsObject(in);
        } else {
            backupOp = in.readObject();
        }

        if (in.readBoolean()) {
            originalCaller = new Address();
            originalCaller.readData(in);
        }

        replicaVersions = new long[MAX_BACKUP_COUNT];
        byte replicaVersionCount = in.readByte();
        for (int k = 0; k < replicaVersionCount; k++) {
            replicaVersions[k] = in.readLong();
        }

        sync = in.readBoolean();
        clientCorrelationId = in.readLong();
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        sb.append(", backupOp=").append(backupOp);
        sb.append(", backupOpData=").append(backupOpData);
        sb.append(", originalCaller=").append(originalCaller);
        sb.append(", version=").append(Arrays.toString(replicaVersions));
        sb.append(", sync=").append(sync);
    }
}
