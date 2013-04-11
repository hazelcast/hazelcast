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
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.logging.Level;

/**
 * @mdogan 4/5/13
 */
final class Backup extends Operation implements BackupOperation, IdentifiedDataSerializable {

    private Operation backupOp;
    private Address originalCaller;
    private long version;
    private boolean sync;

    Backup() {
    }

    public Backup(Operation backupOp, Address originalCaller, long version, boolean sync) {
        this.backupOp = backupOp;
        this.originalCaller = originalCaller;
        this.sync = sync;
        this.version = version;
    }

    public void beforeRun() throws Exception {
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

    public void run() throws Exception {
        final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        final OperationServiceImpl operationService = nodeEngine.operationService;
        operationService.runBackupOperation(backupOp, getPartitionId(), version);
    }

    public void afterRun() throws Exception {
        if (sync) {
            final OperationService operationService = getNodeEngine().getOperationService();
            BackupResponse backupResponse = new BackupResponse();
            OperationAccessor.setCallId(backupResponse, getCallId());
            operationService.send(backupResponse, originalCaller);
        }
    }

    public final boolean returnsResponse() {
        return false;
    }

    public Object getResponse() {
        return null;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(backupOp);
        originalCaller.writeData(out);
        out.writeLong(version);
        out.writeBoolean(sync);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        backupOp = in.readObject();
        originalCaller = new Address();
        originalCaller.readData(in);
        version = in.readLong();
        sync = in.readBoolean();
    }

    @Override
    public int getId() {
        return DataSerializerSpiHook.BACKUP;
    }
}
