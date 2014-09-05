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

package com.hazelcast.transaction.impl;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.exception.TargetNotMemberException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public final class ReplicateTxOperation extends Operation {

    private final List<TransactionLog> txLogs = new LinkedList<TransactionLog>();
    private String callerUuid;
    private String txnId;
    private long timeoutMillis;
    private long startTime;

    public ReplicateTxOperation() {
    }

    public ReplicateTxOperation(List<TransactionLog> logs, String callerUuid, String txnId,
                                long timeoutMillis, long startTime) {
        txLogs.addAll(logs);
        this.callerUuid = callerUuid;
        this.txnId = txnId;
        this.timeoutMillis = timeoutMillis;
        this.startTime = startTime;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        TransactionManagerServiceImpl txManagerService = getService();
        txManagerService.prepareTxBackupLog(txLogs, callerUuid, txnId, timeoutMillis, startTime);
    }

    @Override
    public void afterRun() throws Exception {
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
    public ExceptionAction onException(Throwable throwable) {
        if (throwable instanceof MemberLeftException || throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onException(throwable);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(callerUuid);
        out.writeUTF(txnId);
        out.writeLong(timeoutMillis);
        out.writeLong(startTime);
        int len = txLogs.size();
        out.writeInt(len);
        if (len > 0) {
            for (TransactionLog txLog : txLogs) {
                out.writeObject(txLog);
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
                TransactionLog txLog = in.readObject();
                txLogs.add(txLog);
            }
        }
    }
}
