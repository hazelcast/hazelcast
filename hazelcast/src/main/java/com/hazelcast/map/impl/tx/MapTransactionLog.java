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

package com.hazelcast.map.impl.tx;

import com.hazelcast.map.impl.MapRecordKey;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.impl.KeyAwareTransactionLog;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.ThreadUtil;
import java.io.IOException;
import java.util.concurrent.Future;

/**
 * Represents an operation on the map in the transaction log.
 */
public class MapTransactionLog implements KeyAwareTransactionLog {

    String name;
    Data key;
    long threadId = ThreadUtil.getThreadId();
    String ownerUuid;
    Operation op;

    public MapTransactionLog() {
    }

    public MapTransactionLog(String name, Data key, Operation op, long version, String ownerUuid) {
        this.name = name;
        this.key = key;
        if (!(op instanceof MapTxnOperation)) {
            throw new IllegalArgumentException();
        }
        this.op = op;
        this.ownerUuid = ownerUuid;
    }

    @Override
    public Future prepare(NodeEngine nodeEngine) throws TransactionException {
        TxnPrepareOperation operation = new TxnPrepareOperation(name, key, ownerUuid);
        operation.setThreadId(threadId);
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            return nodeEngine.getOperationService().invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public Future commit(NodeEngine nodeEngine) {
        MapTxnOperation txnOp = (MapTxnOperation) op;
        txnOp.setThreadId(threadId);
        txnOp.setOwnerUuid(ownerUuid);
        try {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
            return nodeEngine.getOperationService().invokeOnPartition(MapService.SERVICE_NAME, op, partitionId);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public Future rollback(NodeEngine nodeEngine) {
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        TxnRollbackOperation operation = new TxnRollbackOperation(name, key, ownerUuid);
        operation.setThreadId(threadId);
        try {
            return nodeEngine.getOperationService().invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        boolean isNullKey = key == null;
        out.writeBoolean(isNullKey);
        if (!isNullKey) {
            key.writeData(out);
        }
        out.writeLong(threadId);
        out.writeUTF(ownerUuid);
        out.writeObject(op);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        boolean isNullKey = in.readBoolean();
        if (!isNullKey) {
            key = new Data();
            key.readData(in);
        }
        threadId = in.readLong();
        ownerUuid = in.readUTF();
        op = in.readObject();
    }

    @Override
    public Object getKey() {
        return new MapRecordKey(name, key);
    }
}
