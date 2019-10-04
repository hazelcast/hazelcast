/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.tx;

import com.hazelcast.internal.util.ThreadUtil;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapRecordKey;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.transaction.impl.TransactionLogRecord;

import java.io.IOException;
import java.util.UUID;

/**
 * Represents an operation on the map in the transaction log.
 */
public class MapTransactionLogRecord implements TransactionLogRecord {

    private int partitionId;
    private long threadId = ThreadUtil.getThreadId();
    private String name;
    private Data key;
    private UUID transactionId;
    private UUID ownerUuid;
    private Operation op;

    public MapTransactionLogRecord() {
    }

    public MapTransactionLogRecord(String name, Data key, int partitionId,
                                   Operation op, UUID ownerUuid, UUID transactionId) {
        this.name = name;
        this.key = key;
        if (!(op instanceof MapTxnOperation)) {
            throw new IllegalArgumentException();
        }
        this.op = op;
        this.ownerUuid = ownerUuid;
        this.partitionId = partitionId;
        this.transactionId = transactionId;
    }

    @Override
    public Operation newPrepareOperation() {
        TxnPrepareOperation operation = new TxnPrepareOperation(partitionId, name, key, ownerUuid, transactionId);
        operation.setThreadId(threadId);
        return operation;
    }

    @Override
    public Operation newCommitOperation() {
        MapTxnOperation operation = (MapTxnOperation) op;
        operation.setThreadId(threadId);
        operation.setOwnerUuid(ownerUuid);
        operation.setTransactionId(transactionId);
        op.setPartitionId(partitionId);
        return op;
    }

    @Override
    public Operation newRollbackOperation() {
        TxnRollbackOperation operation = new TxnRollbackOperation(partitionId, name, key, ownerUuid, transactionId);
        operation.setThreadId(threadId);
        return operation;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(partitionId);
        boolean isNullKey = key == null;
        out.writeBoolean(isNullKey);
        if (!isNullKey) {
            out.writeData(key);
        }
        out.writeLong(threadId);
        UUIDSerializationUtil.writeUUID(out, ownerUuid);
        UUIDSerializationUtil.writeUUID(out, transactionId);
        out.writeObject(op);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        partitionId = in.readInt();
        boolean isNullKey = in.readBoolean();
        if (!isNullKey) {
            key = in.readData();
        }
        threadId = in.readLong();
        ownerUuid = UUIDSerializationUtil.readUUID(in);
        transactionId = UUIDSerializationUtil.readUUID(in);
        op = in.readObject();
    }

    @Override
    public Object getKey() {
        return new MapRecordKey(name, key);
    }

    @Override
    public String toString() {
        return "MapTransactionRecord{"
                + "name='" + name + '\''
                + ", key=" + key
                + ", threadId=" + threadId
                + ", ownerUuid='" + ownerUuid + '\''
                + ", op=" + op
                + ", transactionId=" + transactionId
                + '}';
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MAP_TRANSACTION_LOG_RECORD;
    }
}
