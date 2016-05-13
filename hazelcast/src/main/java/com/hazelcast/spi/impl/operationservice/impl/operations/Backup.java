/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.partition.ReplicaErrorLogger;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.responses.BackupAckResponse;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.util.Clock;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;

import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createEmptyResponseHandler;
import static com.hazelcast.spi.partition.IPartition.MAX_BACKUP_COUNT;

public final class Backup extends Operation implements BackupOperation, IdentifiedDataSerializable {

    private Address originalCaller;
    private long[] replicaVersions;
    private boolean sync;

    private Operation backupOp;
    private Data backupOpData;

    private transient boolean valid = true;

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
        NodeEngine nodeEngine = getNodeEngine();
        int partitionId = getPartitionId();
        InternalPartitionService partitionService = (InternalPartitionService) nodeEngine.getPartitionService();
        ILogger logger = getLogger();

        IPartition partition = partitionService.getPartition(partitionId);
        Address owner = partition.getReplicaAddress(getReplicaIndex());
        if (!nodeEngine.getThisAddress().equals(owner)) {
            valid = false;
            if (logger.isFinestEnabled()) {
                logger.finest("Wrong target! " + toString() + " cannot be processed! Target should be: " + owner);
            }
        } else if (partitionService.isPartitionReplicaVersionStale(getPartitionId(), replicaVersions, getReplicaIndex())) {
            valid = false;
            if (logger.isFineEnabled()) {
                long[] currentVersions = partitionService.getPartitionReplicaVersions(partitionId);
                logger.fine("Ignoring stale backup! Current-versions: " + Arrays.toString(currentVersions)
                        + ", Backup-versions: " + Arrays.toString(replicaVersions));
            }
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
        if (!valid) {
            onExecutionFailure(
                    new IllegalStateException("Wrong target! " + toString() + " cannot be processed!"));
            return;
        }

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();

        if (backupOp == null && backupOpData != null) {
            backupOp = nodeEngine.getSerializationService().toObject(backupOpData);
        }

        if (backupOp != null) {
            ensureBackupOperationInitialized();

            backupOp.beforeRun();
            backupOp.run();
            backupOp.afterRun();
        }

        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        partitionService.updatePartitionReplicaVersions(getPartitionId(), replicaVersions, getReplicaIndex());
    }

    @Override
    public void afterRun() throws Exception {
        if (!valid || !sync || getCallId() == 0 || originalCaller == null) {
            return;
        }

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        long callId = getCallId();
        OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();

        if (nodeEngine.getThisAddress().equals(originalCaller)) {
            operationService.getResponseHandler().notifyBackupComplete(callId);
        } else {
            BackupAckResponse backupAckResponse = new BackupAckResponse(callId, backupOp.isUrgent());
            operationService.send(backupAckResponse, originalCaller);
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public Object getResponse() {
        return null;
    }

    @Override
    public boolean validatesTarget() {
        return false;
    }

    @Override
    public void onExecutionFailure(Throwable e) {
        if (backupOp != null) {
            try {
                // Be sure that backup operation is initialized.
                // If there is an exception before `run` (for example caller is not valid anymore),
                // backup operation is initialized. So, we are initializing it here ourselves.
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
            // Be sure that backup operation is initialized.
            // If there is an exception before `run` (for example caller is not valid anymore),
            // backup operation is initialized. So, we are initializing it here ourselves.
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
        if (backupOpData != null) {
            out.writeBoolean(true);
            out.writeData(backupOpData);
        } else {
            out.writeBoolean(false);
            out.writeObject(backupOp);
        }

        if (originalCaller != null) {
            out.writeBoolean(true);
            originalCaller.writeData(out);
        } else {
            out.writeBoolean(false);
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
            backupOpData = in.readData();
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
