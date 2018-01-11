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
import com.hazelcast.transaction.impl.TransactionLogRecord;
import com.hazelcast.transaction.impl.TransactionManagerServiceImpl;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;
import static com.hazelcast.transaction.impl.TransactionDataSerializerHook.REPLICATE_TX_BACKUP_LOG;

/**
 * Replicates the transactionlog to a remote system.
 *
 * This operation is only executed when durability > 0
 */
public class ReplicateTxBackupLogOperation extends AbstractTxOperation {

    // todo: probably we don't want to use linked list.
    private final List<TransactionLogRecord> records = new LinkedList<TransactionLogRecord>();
    private String callerUuid;
    private String txnId;
    private long timeoutMillis;
    private long startTime;

    public ReplicateTxBackupLogOperation() {
    }

    public ReplicateTxBackupLogOperation(List<TransactionLogRecord> logs, String callerUuid, String txnId,
                                         long timeoutMillis, long startTime) {
        records.addAll(logs);
        this.callerUuid = callerUuid;
        this.txnId = txnId;
        this.timeoutMillis = timeoutMillis;
        this.startTime = startTime;
    }

    @Override
    public void run() throws Exception {
        TransactionManagerServiceImpl txManagerService = getService();
        txManagerService.replicaBackupLog(records, callerUuid, txnId, timeoutMillis, startTime);
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
    public int getId() {
        return REPLICATE_TX_BACKUP_LOG;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(callerUuid);
        out.writeUTF(txnId);
        out.writeLong(timeoutMillis);
        out.writeLong(startTime);
        int len = records.size();
        out.writeInt(len);
        if (len > 0) {
            for (TransactionLogRecord record : records) {
                out.writeObject(record);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        callerUuid = in.readUTF();
        txnId = in.readUTF();
        timeoutMillis = in.readLong();
        startTime = in.readLong();
        int len = in.readInt();
        if (len > 0) {
            for (int i = 0; i < len; i++) {
                TransactionLogRecord record = in.readObject();
                records.add(record);
            }
        }
    }
}
