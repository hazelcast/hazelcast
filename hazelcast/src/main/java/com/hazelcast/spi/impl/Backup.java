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

import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.ReplicaErrorLogger;
import com.hazelcast.spi.BackupOperation;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.Arrays;

final class Backup extends Operation implements BackupOperation, IdentifiedDataSerializable {

    private Data backupOpData;
    private long[] replicaVersions;

    private transient Operation backupOp;

    Backup() {
    }

    Backup(Data backupOp, Address originalCaller, long[] replicaVersions, boolean sync) {
        this.backupOpData = backupOp;
        this.replicaVersions = replicaVersions;
        if (sync && originalCaller == null) {
            throw new IllegalArgumentException("Sync backup requires original caller address, Op: " + backupOp);
        }
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        if (backupOpData != null) {
            backupOp = nodeEngine.getSerializationService().toObject(backupOpData);
            backupOp.setPartitionId(getPartitionId()).setReplicaIndex(getReplicaIndex());
            backupOp.setNodeEngine(nodeEngine);
            backupOp.setCallerUuid(getCallerUuid());
            OperationAccessor.setCallId(backupOp, getCallId());
            OperationAccessor.setCallerAddress(backupOp, getCallerAddress());
            OperationAccessor.setInvocationTime(backupOp, Clock.currentTimeMillis());
            backupOp.setResponseHandler(ResponseHandlerFactory.createErrorLoggingResponseHandler(getLogger()));

            backupOp.beforeRun();
            backupOp.run();
            backupOp.afterRun();

        }

        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        partitionService.updatePartitionReplicaVersions(getPartitionId(), replicaVersions, getReplicaIndex());
    }

    @Override
    public void afterRun() throws Exception {
    }

    @Override
    public ExceptionAction onException(Throwable throwable) {
        if (throwable instanceof WrongTargetException) {
            NodeEngine nodeEngine = getNodeEngine();
            InternalPartitionService partitionService = nodeEngine.getPartitionService();
            if (partitionService.getMaxBackupCount() < getReplicaIndex()) {
                return ExceptionAction.THROW_EXCEPTION;
            }
        }
        return super.onException(throwable);
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return Boolean.TRUE;
    }

    @Override
    public boolean validatesTarget() {
        return true;
    }

    @Override
    public void logError(Throwable e) {
        if (backupOp != null) {
            backupOp.logError(e);
        } else {
            ReplicaErrorLogger.log(e, getLogger());
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeData(backupOpData);
        out.writeLongArray(replicaVersions);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        backupOpData = in.readData();
        replicaVersions = in.readLongArray();
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
        sb.append("{backupOpData=").append(backupOpData);
        sb.append(", backupOp=").append(backupOp);
        sb.append(", version=").append(Arrays.toString(replicaVersions));
        sb.append('}');
        return sb.toString();
    }
}
