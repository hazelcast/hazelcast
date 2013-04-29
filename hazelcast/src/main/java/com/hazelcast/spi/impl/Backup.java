/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.partition.PartitionServiceImpl;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;

/**
 * @mdogan 4/5/13
 */
final class Backup extends Operation implements BackupOperation, IdentifiedDataSerializable {

    private Operation backupOp;
    private Address originalCaller;
    private long[] replicaVersions;
    private boolean sync;

    Backup() {
    }

    public Backup(Operation backupOp, Address originalCaller, long[] replicaVersions, boolean sync) {
        this.backupOp = backupOp;
        this.originalCaller = originalCaller;
        this.sync = sync;
        this.replicaVersions = replicaVersions;
        if (sync && originalCaller == null) {
            throw new IllegalArgumentException("Sync backup requires original caller address!");
        }
    }

    public void beforeRun() throws Exception {
        if (backupOp != null) {
            final NodeEngine nodeEngine = getNodeEngine();
            backupOp.setNodeEngine(nodeEngine);
            backupOp.setResponseHandler(new ResponseHandler() {
                public void sendResponse(Object obj) {
                    if (obj instanceof Throwable && !(obj instanceof RetryableException)) {
                        Throwable t = (Throwable) obj;
                        nodeEngine.getLogger(getClass()).log(Level.SEVERE, t.getMessage(), t);
                    }
                }
            });
            backupOp.setCallerUuid(getCallerUuid());
            backupOp.setValidateTarget(false);
            OperationAccessor.setCallerAddress(backupOp, getCallerAddress());
            OperationAccessor.setInvocationTime(backupOp, Clock.currentTimeMillis());
        }
    }

    public void run() throws Exception {
        final NodeEngine nodeEngine = getNodeEngine();
        final PartitionServiceImpl partitionService = (PartitionServiceImpl) nodeEngine.getPartitionService();
        partitionService.updatePartitionReplicaVersions(getPartitionId(), replicaVersions, getReplicaIndex());

        if (backupOp != null) {
            final OperationService operationService = nodeEngine.getOperationService();
            operationService.runOperation(backupOp);
        }
    }

    public void afterRun() throws Exception {
        if (sync) {
            final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
            final long callId = getCallId();
            final OperationServiceImpl operationService = nodeEngine.operationService;

            if (!nodeEngine.getThisAddress().equals(originalCaller)) {
                BackupResponse backupResponse = new BackupResponse();
                OperationAccessor.setCallId(backupResponse, callId);
                operationService.send(backupResponse, originalCaller);
            } else {
                operationService.notifyBackupCall(callId);
            }
        }
    }

    public final boolean returnsResponse() {
        return false;
    }

    public Object getResponse() {
        return null;
    }

    public boolean validatesTarget() {
        return true;
    }

    public void logError(Throwable e) {
        final ILogger logger = getLogger();
        logger.log(Level.INFO, e.getClass() + ": " + e.getMessage());
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(backupOp);
        if (originalCaller != null) {
            out.writeBoolean(true);
            originalCaller.writeData(out);
        } else {
            out.writeBoolean(false);
        }
        for (int i = 0; i < PartitionInfo.MAX_BACKUP_COUNT; i++) {
            out.writeLong(replicaVersions[i]);
        }
        out.writeBoolean(sync);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        backupOp = in.readObject();
        if (in.readBoolean()) {
            originalCaller = new Address();
            originalCaller.readData(in);
        }
        replicaVersions = new long[PartitionInfo.MAX_BACKUP_COUNT];
        for (int i = 0; i < PartitionInfo.MAX_BACKUP_COUNT; i++) {
            replicaVersions[i] = in.readLong();
        }
        sync = in.readBoolean();
    }

    @Override
    public int getId() {
        return DataSerializerSpiHook.BACKUP;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("Backup");
        sb.append("{backupOp=").append(backupOp);
        sb.append(", originalCaller=").append(originalCaller);
        sb.append(", version=").append(Arrays.toString(replicaVersions));
        sb.append(", sync=").append(sync);
        sb.append('}');
        return sb.toString();
    }
}
