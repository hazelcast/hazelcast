/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.transaction.impl.xa;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.impl.TransactionLog;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class PutRemoteTransactionOperation extends Operation implements BackupAwareOperation {

    private final List<TransactionLog> txLogs = new LinkedList<TransactionLog>();

    private SerializableXID xid;
    private String txnId;
    private String txOwnerUuid;
    private long timeoutMillis;
    private long startTime;

    public PutRemoteTransactionOperation() {
    }

    public PutRemoteTransactionOperation(List<TransactionLog> logs, String txnId, SerializableXID xid,
                                         String txOwnerUuid, long timeoutMillis, long startTime) {
        txLogs.addAll(logs);
        this.txnId = txnId;
        this.xid = xid;
        this.txOwnerUuid = txOwnerUuid;
        this.timeoutMillis = timeoutMillis;
        this.startTime = startTime;
    }

    @Override
    public void beforeRun() throws Exception {
    }

    @Override
    public void run() throws Exception {
        XAService xaService = getService();
        NodeEngine nodeEngine = getNodeEngine();
        XATransactionImpl transaction =
                new XATransactionImpl(nodeEngine, txLogs, txnId, xid, txOwnerUuid, timeoutMillis, startTime);
        xaService.putTransaction(transaction);
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
        return null;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getSyncBackupCount() {
        return 0;
    }

    @Override
    public int getAsyncBackupCount() {
        return 1;
    }

    @Override
    public Operation getBackupOperation() {
        return new PutRemoteTransactionBackupOperation(txLogs, txnId, xid, txOwnerUuid, timeoutMillis, startTime);
    }

    @Override
    public String getServiceName() {
        return XAService.SERVICE_NAME;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeUTF(txnId);
        out.writeObject(xid);
        out.writeUTF(txOwnerUuid);
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
        txnId = in.readUTF();
        xid = in.readObject();
        txOwnerUuid = in.readUTF();
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
