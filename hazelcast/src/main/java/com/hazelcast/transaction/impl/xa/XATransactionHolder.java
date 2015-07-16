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
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.transaction.impl.TransactionRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XATransactionHolder implements DataSerializable {
    String txnId;
    SerializableXID xid;
    String ownerUuid;
    long timeoutMilis;
    long startTime;
    List<TransactionRecord> records;

    public XATransactionHolder() {

    }

    public XATransactionHolder(XATransactionImpl xaTransaction) {
        txnId = xaTransaction.getTxnId();
        xid = xaTransaction.getXid();
        ownerUuid = xaTransaction.getOwnerUuid();
        timeoutMilis = xaTransaction.getTimeoutMillis();
        startTime = xaTransaction.getStartTimeMillis();
        records = xaTransaction.getRecords();
    }

    public XATransactionHolder(String txnId, SerializableXID xid, String ownerUuid, long timeoutMilis,
                               long startTime, List<TransactionRecord> records) {
        this.txnId = txnId;
        this.xid = xid;
        this.ownerUuid = ownerUuid;
        this.timeoutMilis = timeoutMilis;
        this.startTime = startTime;
        this.records = records;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(txnId);
        out.writeObject(xid);
        out.writeUTF(ownerUuid);
        out.writeLong(timeoutMilis);
        out.writeLong(startTime);
        int len = records.size();
        out.writeInt(len);
        if (len > 0) {
            for (TransactionRecord record : records) {
                out.writeObject(record);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        txnId = in.readUTF();
        xid = in.readObject();
        ownerUuid = in.readUTF();
        timeoutMilis = in.readLong();
        startTime = in.readLong();
        int size = in.readInt();
        records = new ArrayList<TransactionRecord>(size);
        for (int i = 0; i < size; i++) {
            TransactionRecord record = in.readObject();
            records.add(record);
        }
    }
}
