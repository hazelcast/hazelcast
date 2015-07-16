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

package com.hazelcast.map.impl.tx;

import com.hazelcast.map.impl.MapRecordKey;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.impl.AbstractTransactionRecord;
import com.hazelcast.transaction.impl.KeyAwareTransactionRecord;
import com.hazelcast.util.ThreadUtil;

import java.io.IOException;

/**
 * Represents an operation on the map in the transaction log.
 */
public class MapTransactionRecord extends AbstractTransactionRecord implements KeyAwareTransactionRecord {

    private String name;
    private Data key;
    private long threadId = ThreadUtil.getThreadId();
    private String ownerUuid;
    private Operation op;
    private int partitionId;

    public MapTransactionRecord() {
    }

    public MapTransactionRecord(String name, int partitionId, Data key, Operation op, long version, String ownerUuid) {
        this.name = name;
        this.key = key;
        if (!(op instanceof MapTxnOperation)) {
            throw new IllegalArgumentException();
        }
        this.op = op;
        this.ownerUuid = ownerUuid;
        this.partitionId = partitionId;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public Operation createPrepareOperation() {
        return new TxnPrepareOperation(partitionId, name, key, ownerUuid, threadId);
    }

    @Override
    public Operation createRollbackOperation() {
        return new TxnRollbackOperation(partitionId, name, key, ownerUuid, threadId);
    }

    @Override
    public Operation createCommitOperation() {
        MapTxnOperation commitOperation = (MapTxnOperation) op;
        commitOperation.setThreadId(threadId);
        commitOperation.setOwnerUuid(ownerUuid);
        op.setPartitionId(partitionId);
        op.setServiceName(MapService.SERVICE_NAME);
        return op;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.write(partitionId);
        boolean isNullKey = key == null;
        out.writeBoolean(isNullKey);
        if (!isNullKey) {
            out.writeData(key);
        }
        out.writeLong(threadId);
        out.writeUTF(ownerUuid);
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
        ownerUuid = in.readUTF();
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
                + '}';
    }
}
