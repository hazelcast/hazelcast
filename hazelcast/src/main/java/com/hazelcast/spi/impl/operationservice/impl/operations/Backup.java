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

package com.hazelcast.spi.impl.operationservice.impl.operations;

import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.PartitionReplicaVersionManager;
import com.hazelcast.internal.partition.ReplicaErrorLogger;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.util.Clock;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;

import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createEmptyResponseHandler;
import static com.hazelcast.spi.partition.IPartition.MAX_BACKUP_COUNT;

public final class Backup extends Operation implements BackupOperation, AllowedDuringPassiveState,
        IdentifiedDataSerializable {

    private Address originalCaller;
    private ServiceNamespace namespace;
    private long[] replicaVersions;
    private boolean sync;

    private Operation backupOp;
    private Data backupOpData;

    private transient Throwable validationFailure;

    public Backup() {
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public Backup(Operation backupOp, Address originalCaller, long[] replicaVersions, boolean sync) {
        this.backupOp = backupOp;
        this.originalCaller = originalCaller;
        this.sync = sync;
        this.replicaVersions = replicaVersions;
        if (sync && originalCaller == null) {
            throw new IllegalArgumentException("Sync backup requires original caller address, Backup operation: "
                    + backupOp);
        }
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public Backup(Data backupOpData, Address originalCaller, long[] replicaVersions, boolean sync) {
        this.backupOpData = backupOpData;
        this.originalCaller = originalCaller;
        this.sync = sync;
        this.replicaVersions = replicaVersions;
        if (sync && originalCaller == null) {
            throw new IllegalArgumentException("Sync backup requires original caller address, Backup operation data: "
                    + backupOpData);
        }
    }

    public Operation getBackupOp() {
        return backupOp;
    }

    @Override
    public void beforeRun() throws Exception {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        int partitionId = getPartitionId();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        ILogger logger = getLogger();

        IPartition partition = partitionService.getPartition(partitionId);
        Address owner = partition.getReplicaAddress(getReplicaIndex());

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
        if (!nodeEngine.getThisAddress().equals(owner)) {
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
        if (backupOp.getNodeEngine() == null) {
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

        backupOp.beforeRun();
        backupOp.run();
        backupOp.afterRun();

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        PartitionReplicaVersionManager versionManager = nodeEngine.getPartitionService().getPartitionReplicaVersionManager();
        versionManager.updatePartitionReplicaVersions(getPartitionId(), namespace, replicaVersions, getReplicaIndex());
    }

    @Override
    public void afterRun() throws Exception {
        if (validationFailure != null || !sync || getCallId() == 0 || originalCaller == null) {
            return;
        }

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        long callId = getCallId();
        OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();

        if (nodeEngine.getThisAddress().equals(originalCaller)) {
            operationService.getInboundResponseHandler().notifyBackupComplete(callId);
        } else {
            operationService.getOutboundResponseHandler().sendBackupAck(originalCaller, callId, backupOp.isUrgent());
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
    public int getId() {
        return SpiDataSerializerHook.BACKUP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        if (backupOpData == null) {
            out.writeBoolean(false);
            out.writeObject(backupOp);
        } else {
            out.writeBoolean(true);
            out.writeData(backupOpData);
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
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        if (in.readBoolean()) {
            backupOp = in.readDataAsObject();
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
