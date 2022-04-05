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

package com.hazelcast.transaction.impl.operations;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;

import java.io.IOException;
import java.util.UUID;

import static com.hazelcast.spi.impl.operationservice.ExceptionAction.THROW_EXCEPTION;
import static com.hazelcast.transaction.impl.TransactionDataSerializerHook.CREATE_TX_BACKUP_LOG;

public class CreateTxBackupLogOperation extends AbstractTxOperation {

    private UUID callerUuid;
    private UUID txnId;

    public CreateTxBackupLogOperation() {
    }

    public CreateTxBackupLogOperation(UUID callerUuid, UUID txnId) {
        this.callerUuid = callerUuid;
        this.txnId = txnId;
    }

    @Override
    public void run() throws Exception {
        TransactionManagerServiceImpl txManagerService = getService();
        txManagerService.createBackupLog(callerUuid, txnId);
    }

    @Override
    public Object getResponse() {
        return true;
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public int getClassId() {
        return CREATE_TX_BACKUP_LOG;
    }

    @Override
    public UUID getCallerUuid() {
        return callerUuid;
    }

    public UUID getTxnId() {
        return txnId;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        UUIDSerializationUtil.writeUUID(out, callerUuid);
        UUIDSerializationUtil.writeUUID(out, txnId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        callerUuid = UUIDSerializationUtil.readUUID(in);
        txnId = UUIDSerializationUtil.readUUID(in);
    }
}
