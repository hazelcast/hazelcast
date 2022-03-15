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

package com.hazelcast.transaction.impl.xa;

import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.transaction.impl.TransactionDataSerializerHook;
import com.hazelcast.transaction.impl.TransactionLogRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class XATransactionDTO implements IdentifiedDataSerializable {
    private UUID txnId;
    private SerializableXID xid;
    private UUID ownerUuid;
    private long timeoutMilis;
    private long startTime;
    private Collection<TransactionLogRecord> records;

    public XATransactionDTO() {
    }

    public XATransactionDTO(XATransaction xaTransaction) {
        txnId = xaTransaction.getTxnId();
        xid = xaTransaction.getXid();
        ownerUuid = xaTransaction.getOwnerUuid();
        timeoutMilis = xaTransaction.getTimeoutMillis();
        startTime = xaTransaction.getStartTime();
        records = xaTransaction.getTransactionRecords();
    }

    public XATransactionDTO(UUID txnId, SerializableXID xid, UUID ownerUuid, long timeoutMilis,
                            long startTime, List<TransactionLogRecord> records) {
        this.txnId = txnId;
        this.xid = xid;
        this.ownerUuid = ownerUuid;
        this.timeoutMilis = timeoutMilis;
        this.startTime = startTime;
        this.records = records;
    }

    public UUID getTxnId() {
        return txnId;
    }

    public SerializableXID getXid() {
        return xid;
    }

    public UUID getOwnerUuid() {
        return ownerUuid;
    }

    public long getTimeoutMilis() {
        return timeoutMilis;
    }

    public long getStartTime() {
        return startTime;
    }

    public Collection<TransactionLogRecord> getRecords() {
        return records;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        UUIDSerializationUtil.writeUUID(out, txnId);
        out.writeObject(xid);
        UUIDSerializationUtil.writeUUID(out, ownerUuid);
        out.writeLong(timeoutMilis);
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
    public void readData(ObjectDataInput in) throws IOException {
        txnId = UUIDSerializationUtil.readUUID(in);
        xid = in.readObject();
        ownerUuid = UUIDSerializationUtil.readUUID(in);
        timeoutMilis = in.readLong();
        startTime = in.readLong();
        int size = in.readInt();
        records = new ArrayList<TransactionLogRecord>(size);
        for (int i = 0; i < size; i++) {
            TransactionLogRecord record = in.readObject();
            records.add(record);
        }
    }

    @Override
    public int getFactoryId() {
        return TransactionDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return TransactionDataSerializerHook.XA_TRANSACTION_DTO;
    }
}
