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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RecoveredTransaction implements DataSerializable {

    private List<TransactionLog> txLogs;
    private SerializableXID xid;
    private String callerUuid;
    private String txnId;
    private long timeoutMillis;
    private long startTime;

    public RecoveredTransaction() {
    }

    public List<TransactionLog> getTxLogs() {
        return txLogs;
    }

    public void setTxLogs(List<TransactionLog> txLogs) {
        this.txLogs = txLogs;
    }

    public SerializableXID getXid() {
        return xid;
    }

    public void setXid(SerializableXID xid) {
        this.xid = xid;
    }

    public String getCallerUuid() {
        return callerUuid;
    }

    public void setCallerUuid(String callerUuid) {
        this.callerUuid = callerUuid;
    }

    public String getTxnId() {
        return txnId;
    }

    public void setTxnId(String txnId) {
        this.txnId = txnId;
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public void setTimeoutMillis(long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(xid);
        out.writeUTF(callerUuid);
        out.writeUTF(txnId);
        out.writeLong(timeoutMillis);
        out.writeLong(startTime);
        out.writeInt(txLogs.size());
        for (TransactionLog txLog : txLogs) {
            out.writeObject(txLog);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        xid = in.readObject();
        callerUuid = in.readUTF();
        txnId = in.readUTF();
        timeoutMillis = in.readLong();
        startTime = in.readLong();
        final int size = in.readInt();
        txLogs = new ArrayList<TransactionLog>(size);
        for (int i = 0; i < size; i++) {
            final TransactionLog log = in.readObject();
            txLogs.add(log);
        }
    }
}
