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
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.PartitionServiceImpl;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.*;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author mdogan 4/5/13
 */
final class Backup extends Operation implements BackupOperation, IdentifiedDataSerializable {

    private Operation backupOp;
    private Address originalCaller;
    private long[] replicaVersions;
    private boolean sync;

    private transient boolean valid = true;

    Backup() {
    }

    Backup(Operation backupOp, Address originalCaller, long[] replicaVersions, boolean sync) {
        this.backupOp = backupOp;
        this.originalCaller = originalCaller;
        this.sync = sync;
        this.replicaVersions = replicaVersions;
        if (sync && originalCaller == null) {
            throw new IllegalArgumentException("Sync backup requires original caller address, Op: " + backupOp);
        }
    }

    @Override
    public void beforeRun() throws Exception {
        final NodeEngine nodeEngine = getNodeEngine();
        final int partitionId = getPartitionId();
        final InternalPartition partition = nodeEngine.getPartitionService().getPartition(partitionId);
        final Address owner = partition.getReplicaAddress(getReplicaIndex());
        if (!nodeEngine.getThisAddress().equals(owner)) {
            valid = false;
            final ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest("Wrong target! " + toString() + " cannot be processed! Target should be: " + owner);
            }
        }
    }

    @Override
    public void run() throws Exception {
        if (!valid) {
            return;
        }
        final NodeEngine nodeEngine = getNodeEngine();
        final PartitionServiceImpl partitionService = (PartitionServiceImpl) nodeEngine.getPartitionService();
        partitionService.updatePartitionReplicaVersions(getPartitionId(), replicaVersions, getReplicaIndex());

        if (backupOp != null) {
            backupOp.setNodeEngine(nodeEngine);
            backupOp.setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler());
            backupOp.setCallerUuid(getCallerUuid());
            OperationAccessor.setCallerAddress(backupOp, getCallerAddress());
            OperationAccessor.setInvocationTime(backupOp, Clock.currentTimeMillis());

            final OperationService operationService = nodeEngine.getOperationService();
            operationService.runOperation(backupOp);
        }
    }

    @Override
    public void afterRun() throws Exception {
        if (sync && getCallId() != 0 && originalCaller != null) {
            final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
            final long callId = getCallId();
            final InternalOperationService operationService = nodeEngine.operationService;

            if (!nodeEngine.getThisAddress().equals(originalCaller)) {
                BackupResponse backupResponse = new BackupResponse(callId, backupOp.isUrgent());
                operationService.send(backupResponse, originalCaller);
            } else {
                operationService.notifyBackupCall(callId);
            }
        }
    }

    @Override
    public final boolean returnsResponse() {
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
    public void logError(Throwable e) {
        ReplicaErrorLogger.log(e, getLogger());
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
        out.writeLongArray(replicaVersions);
        out.writeBoolean(sync);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        backupOp = in.readObject();
        if (in.readBoolean()) {
            originalCaller = new Address();
            originalCaller.readData(in);
        }
        replicaVersions = in.readLongArray();
        sync = in.readBoolean();
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
