/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;

import java.io.IOException;

import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;
import static com.hazelcast.transaction.impl.TransactionDataSerializerHook.PURGE_TX_BACKUP_LOG;

public class PurgeTxBackupLogOperation extends AbstractTxOperation {

    private String txnId;

    public PurgeTxBackupLogOperation() {
    }

    public PurgeTxBackupLogOperation(String txnId) {
        this.txnId = txnId;
    }

    @Override
    public void run() throws Exception {
        TransactionManagerServiceImpl txManagerService = getService();
        txManagerService.purgeBackupLog(txnId);
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return THROW_EXCEPTION;
        }
        return super.onInvocationException(throwable);
    }

    @Override
    public int getId() {
        return PURGE_TX_BACKUP_LOG;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(txnId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        txnId = in.readUTF();
    }
}
